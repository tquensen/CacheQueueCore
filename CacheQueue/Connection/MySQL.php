<?php
namespace CacheQueue\Connection;

class MySQL implements ConnectionInterface
{
    private $tableName = null;

    /**
     * @var \PDO
     */
    private $db = null;
    /**
     * @var \PDOStatement
     */
    private $stmtGet = null;
    /**
     * @var \PDOStatement
     */
    private $stmtGetJob = null;
    /**
     * @var \PDOStatement
     */
    private $stmtUpdateJob = null;
    /**
     * @var \PDOStatement
     */
    private $stmtUpdateJobStatus = null;
    /**
     * @var \PDOStatement
     */
    private $stmtSetGet = null;
    /**
     * @var \PDOStatement
     */
    private $stmtSetInsert = null;
    /**
     * @var \PDOStatement
     */
    private $stmtSetUpdate = null;
    /**
     * @var \PDOStatement
     */
    private $stmtRefresh = null;
    /**
     * @var \PDOStatement
     */
    private $stmtQueueGet = null;
    /**
     * @var \PDOStatement
     */
    private $stmtQueueInsert = null;
    /**
     * @var \PDOStatement
     */
    private $stmtQueueUpdate = null;
    /**
     * @var \PDOStatement
     */
    private $stmtQueueCount = null;
    /**
     * @var \PDOStatement
     */
    private $stmtQueueCountAll = null;
    /**
     * @var \PDOStatement
     */
    private $stmtReleaseLock = null;

    private $useFulltextTags = null;

    public function __construct($config = array())
    {

        $this->db = new \PDO($config['dns'], $config['user'], $config['pass'], !empty($config['options']) ? $config['options'] : array());
        $this->db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        if ($this->db && $this->db->getAttribute(\PDO::ATTR_DRIVER_NAME) == 'mysql') {
            $this->db->exec('SET CHARACTER SET utf8');
        }

        $this->tableName = !empty($config['table']) ? $config['table'] : 'cache';

        $this->useFulltextTags = !empty($config['useFulltextTags']);
    }

    public function setup()
    {
        $this->db->query('CREATE TABLE '.$this->tableName.' (
            id VARCHAR(250),
            fresh_until BIGINT NOT NULL DEFAULT 0,
            tags VARCHAR(250) NOT NULL DEFAULT "",
            queued TINYINT(1) NOT NULL DEFAULT 0,
            queued_worker INT(11),
            queue_fresh_until BIGINT NOT NULL DEFAULT 0,
            queue_tags VARCHAR(250) NOT NULL DEFAULT "",
            queue_priority INT(11) NOT NULL DEFAULT 0,
            queue_start BIGINT NOT NULL DEFAULT 0,
            date_set BIGINT NOT NULL DEFAULT 0,
            is_temp TINYINT(1) NOT NULL DEFAULT 0,
            task VARCHAR(250),
            params BLOB,
            data LONGBLOB,
            PRIMARY KEY (id),
            INDEX fresh_until (fresh_until),
            INDEX queued (queued, queue_priority),
            '.($this->useFulltextTags ? 'FULLTEXT (tags) ' : 'INDEX tags (tags)').'
            ) ENGINE=INNODB DEFAULT CHARSET=utf8
            '
        );

    }

    public function get($key, $onlyFresh = false)
    {
        $stmt = $this->stmtGet ?: $this->stmtGet = $this->db->prepare('SELECT id, fresh_until, queue_fresh_until, date_set, task, params, data, tags FROM '.$this->tableName.' WHERE id = ? LIMIT 1');
        if (!$stmt->execute(array($key))) {
            return false;
        }
        $result = $stmt->fetch(\PDO::FETCH_ASSOC);
        $stmt->closeCursor();
        if (!$result) {
            return false;
        }

        $return = array();

        $return['key'] = $result['id'];
        //$return['queued'] = !empty($result['queued']);
        $return['fresh_until'] = !empty($result['fresh_until']) ? $result['fresh_until'] : 0;
        $return['is_fresh'] = $return['fresh_until'] > time();

        $return['date_set'] = !empty($result['date_set']) ? $result['date_set'] : 0;

        $return['queue_fresh_until'] = !empty($result['queue_fresh_until']) ? $result['queue_fresh_until'] : 0;
        $return['queue_is_fresh'] = $return['queue_fresh_until'] > time();
        if ($this->useFulltextTags) {
            $return['tags'] = !empty($result['tags']) ? explode(' ', $result['tags']) : array();
        } else {
            $return['tags'] = !empty($result['tags']) ? explode('##', mb_substr($result['tags'], 2, mb_strlen($result['tags']), 'UTF-8')) : array();
        }
        $return['task'] = !empty($result['task']) ? $result['task'] : null;
        $return['params'] = !empty($result['params']) ? unserialize($result['params']) : null;
        $return['data'] = isset($result['data']) ? unserialize($result['data']) : false;

        return (!$onlyFresh || $return['is_fresh']) ? $return : false;
    }


    public function getByTag($tag, $onlyFresh = false)
    {

        $tags = array_values((array) $tag);
        $return = array();

        $query = 'SELECT id, fresh_until, queue_fresh_until, date_set, task, params, data, tags FROM '.$this->tableName.' WHERE';

        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
            $query .= ' MATCH (tags) AGAINST ("'.$tags.'" IN BOOLEAN MODE) ';
        } else {
            $query .= ' (tags LIKE "%##'.implode('%" OR tags LIKE "%##', $tags).'%") ';
        }

        if ($onlyFresh) {
            $query .= ' AND fresh_until > '.time();
        }


        if (!$stmt = $this->db->query($query)) {
            return false;
        }
        foreach ($stmt->fetchAll(\PDO::FETCH_ASSOC) as $result) {
            $entry = array();
            $entry['key'] = $result['id'];
            //$return['queued'] = !empty($result['queued']);
            $entry['fresh_until'] = !empty($result['fresh_until']) ? $result['fresh_until'] : 0;
            $entry['is_fresh'] = $entry['fresh_until'] > time();

            $entry['date_set'] = !empty($result['date_set']) ? $result['date_set'] : 0;

            $entry['queue_fresh_until'] = !empty($result['queue_fresh_until']) ? $result['queue_fresh_until'] : 0;
            $entry['queue_is_fresh'] = $entry['queue_fresh_until'] > time();
            if ($this->useFulltextTags) {
                $entry['tags'] = !empty($result['tags']) ? explode(' ', $result['tags']) : array();
            } else {
                $entry['tags'] = !empty($result['tags']) ? explode('##', mb_substr($result['tags'], 2, mb_strlen($result['tags']), 'UTF-8')) : array();
            }
            $entry['task'] = !empty($result['task']) ? $result['task'] : null;
            $entry['params'] = !empty($result['params']) ? unserialize($result['params']) : null;
            $entry['data'] = isset($result['data']) ? unserialize($result['data']) : false;

            $return[] = $entry;
        }

        return $return;
    }

    public function getValue($key, $onlyFresh = false)
    {
        $result = $this->get($key);
        if (!$result || !isset($result['data'])) {
            return false;
        }
        return (!$onlyFresh || $result['is_fresh']) ? $result['data'] : false;
    }

    public function getJob($workerId, $channel = 1)
    {
        try {
            $this->db->beginTransaction();
            $stmt = $this->stmtGetJob ?: $this->stmtGetJob = $this->db->prepare('SELECT id, queue_fresh_until, queue_start, queued, queue_priority, queue_tags, task, params, data, is_temp FROM '.$this->tableName.' WHERE queued = ? AND queue_start <= ? ORDER BY queue_priority ASC LIMIT 1 FOR UPDATE');
            $stmt->execute(array($channel, time()));
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
            if (empty($result)) {
                $this->db->commit();
                return false;
            }
            $stmt = $this->stmtUpdateJob ?: $this->stmtUpdateJob = $this->db->prepare('UPDATE '.$this->tableName.' SET queued = 0, queued_worker = ? WHERE id = ?');
            $stmt->execute(array($workerId, $result['id']));
            $this->db->commit();

            $return = array();

            $return['key'] = $result['id'];
            $return['fresh_until'] = !empty($result['queue_fresh_until']) ? $result['queue_fresh_until'] : 0;
            $return['fresh_for'] = !empty($result['queue_fresh_until']) && !empty($result['queue_start']) ? $result['queue_fresh_until'] - $result['queue_start'] : 0;
            if ($this->useFulltextTags) {
                $return['tags'] = !empty($result['queue_tags']) ? explode(' ', $result['queue_tags']) : array();
            } else {
                $return['tags'] = !empty($result['queue_tags']) ? explode('##', mb_substr($result['queue_tags'], 2, mb_strlen($result['queue_tags']), 'UTF-8')) : array();
            }
            $return['task'] = !empty($result['task']) ? $result['task'] : null;
            $return['params'] = !empty($result['params']) ? unserialize($result['params']) : null;
            $return['data'] = isset($result['data']) ? unserialize($result['data']) : null;
            $return['channel'] = isset($result['queued']) ? $result['queued'] : 0;
            $return['priority'] = isset($result['queue_priority']) ? $result['queue_priority'] : 50;
            $return['temp'] = !empty($result['is_temp']);

            $return['worker_id'] = $workerId;

            return $return;
        } catch (\PDOException $e) {
            $this->db->rollBack();
            return false;
        }
    }

    public function updateJobStatus($key, $workerId, $newQueueFreshFor = 0)
    {
        $stmt = $this->stmtUpdateJobStatus ?: $this->stmtUpdateJobStatus = $this->db->prepare('UPDATE '.$this->tableName.' SET queued_worker = null, queue_fresh_until = ? WHERE queued_worker = ? AND id = ?');
        return $stmt->execute(array($newQueueFreshFor > 0 ? $newQueueFreshFor + time() : 0, $workerId, $key));
    }

    public function set($key, $data, $freshFor, $force = false, $tags = array())
    {
        $freshUntil = time() + $freshFor;

        $tags = array_values((array) $tags);
        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
        } else {
            $tags = !empty($tags) ? '##'.implode('##', $tags) : '';
        }

        try {
            $this->db->beginTransaction();
            $stmt = $this->stmtSetGet ?: $this->stmtSetGet = $this->db->prepare('SELECT id, fresh_until FROM '.$this->tableName.' WHERE id = ? LIMIT 1 FOR UPDATE');
            $stmt->execute(array($key));
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
            if (empty($result)) {
                $stmt = $this->stmtSetInsert ?: $this->stmtSetInsert = $this->db->prepare('INSERT INTO '.$this->tableName.' SET
                    id = ?,
                    fresh_until = ?,
                    data = ?,
                    date_set = ?,
                    tags = ?
                    ');
                $stmt->execute(array($key, $freshUntil, serialize($data), time(), $tags));
                $this->db->commit();
                return true;
            }

            if ($force || $result['fresh_until'] < time()) {
                $stmt = $this->stmtSetUpdate ?: $this->stmtSetUpdate = $this->db->prepare('UPDATE '.$this->tableName.' SET
                    fresh_until = ?,
                    data = ?,
                    date_set = ?,
                    tags = ?
                    WHERE id = ?
                    ');
                $stmt->execute(array($freshUntil, serialize($data), time(), $tags, $key));
                $this->db->commit();
                return true;
            } else {
                $this->db->commit();
                return true;
            }
        } catch (\PDOException $e) {
            $this->db->rollBack();
            return false;
        }
    }

    public function refresh($key, $freshFor, $force = false)
    {
        $freshUntil = time() + $freshFor;

        try {
            $this->db->beginTransaction();
            $stmt = $this->stmtSetGet ?: $this->stmtSetGet = $this->db->prepare('SELECT id, fresh_until FROM '.$this->tableName.' WHERE id = ? LIMIT 1 FOR UPDATE');
            $stmt->execute(array($key));
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
            if (empty($result)) {
                return false;
            }

            if ($force || $result['fresh_until'] < time()) {
                $stmt = $this->stmtRefresh ?: $this->stmtRefresh = $this->db->prepare('UPDATE '.$this->tableName.' SET
                    fresh_until = ?,
                    date_set = ?
                    WHERE id = ?
                    ');
                $stmt->execute(array($freshUntil, time(), $key));
                $this->db->commit();
                return true;
            } else {
                $this->db->commit();
                return true;
            }
        } catch (\PDOException $e) {
            $this->db->rollBack();
            return false;
        }
    }

    public function queue($key, $task, $params, $freshFor, $force = false, $tags = array(), $priority = 50, $delay = 0, $channel = 1)
    {
        if ($key === true) {
            $key = 'temp_'.md5(microtime(true).rand(10000,99999));
            $force = true;
            $freshFor = 0;
            $temp = true;
        } else {
            $temp = false;
        }

        $freshUntil = time() + $freshFor + $delay;
        $queueStart = time() + $delay;

        $tags = array_values((array) $tags);
        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
        } else {
            $tags = !empty($tags) ? '##'.implode('##', $tags) : '';
        }

        try {
            $this->db->beginTransaction();
            $stmt = $this->stmtQueueGet ?: $this->stmtQueueGet = $this->db->prepare('SELECT id, fresh_until, queue_fresh_until FROM '.$this->tableName.' WHERE id = ? LIMIT 1 FOR UPDATE');
            $stmt->execute(array($key));
            $result = $stmt->fetch(\PDO::FETCH_ASSOC);
            if (empty($result)) {
                $stmt = $this->stmtQueueInsert ?: $this->stmtQueueInsert = $this->db->prepare('INSERT INTO '.$this->tableName.' SET
                    id = ?,
                    queue_fresh_until = ?,
                    queued = ?,
                    queued_worker = null,
                    task = ?,
                    params = ?,
                    queue_priority = ?,
                    queue_tags = ?,
                    queue_start = ?,
                    is_temp = ?
                    ');
                $stmt->execute(array($key, $freshUntil, $channel, $task, serialize($params), $priority, $tags, $queueStart, $temp ? 1 : 0));
                $this->db->commit();
                return true;
            }

            if ($force || ($result['fresh_until'] < $queueStart && $result['queue_fresh_until'] < $queueStart)) {
                $stmt = $this->stmtQueueUpdate ?: $this->stmtQueueUpdate = $this->db->prepare('UPDATE '.$this->tableName.' SET
                    queue_fresh_until = ?,
                    queued = ?,
                    queued_worker = null,
                    task = ?,
                    params = ?,
                    queue_priority = ?,
                    queue_tags = ?,
                    queue_start = ?,
                    is_temp = ?
                    WHERE id = ?
                    ');
                $stmt->execute(array($freshUntil, $channel, $task, serialize($params), $priority, $tags, $queueStart, $temp ? 1 : 0, $key));
                $this->db->commit();
                return true;
            } else {
                $this->db->commit();
                return true;
            }
        } catch (\PDOException $e) {
            $this->db->rollBack();
            return false;
        }

    }

    public function getQueueCount($channel = true)
    {
        if ($channel == true) {
            $stmt = $this->stmtQueueCountAll ?: $this->stmtQueueCountAll = $this->db->prepare('SELECT COUNT(*) FROM '.$this->tableName.' WHERE queued != 0');
            $stmt->execute();
        } else {
            $stmt = $this->stmtQueueCount ?: $this->stmtQueueCount = $this->db->prepare('SELECT COUNT(*) FROM '.$this->tableName.' WHERE queued = ?');
            $stmt->execute(array($channel));
        }
        return $stmt->fetchColumn();
    }

    public function countAll($fresh = null)
    {
        $query = 'SELECT COUNT(*) as num FROM '.$this->tableName.'';


        if ($fresh !== null) {
            if ($fresh) {
                $query .= ' WHERE fresh_until > '.time();
            } else {
                $query .= ' WHERE fresh_until <= '.time();
            }
        }


        if (!$stmt = $this->db->query($query)) {
            return false;
        }
        $stmt->execute();
        return $stmt->fetchColumn();
    }

    public function countByTag($tag, $fresh = null)
    {
        $tags = array_values((array) $tag);

        $query = 'SELECT COUNT(*) as num FROM '.$this->tableName.' WHERE';

        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
            $query .= ' MATCH (tags) AGAINST ("'.$tags.'" IN BOOLEAN MODE) ';
        } else {
            $query .= ' (tags LIKE "%##'.implode('%" OR tags LIKE "%##', $tags).'%") ';
        }

        if ($fresh !== null) {
            if ($fresh) {
                $query .= ' AND fresh_until > '.time();
            } else {
                $query .= ' AND fresh_until <= '.time();
            }
        }

        if (!$stmt = $this->db->query($query)) {
            return false;
        }
        $stmt->execute();
        return $stmt->fetchColumn();
    }

    public function remove($key, $force = false)
    {
        $query = 'DELETE FROM '.$this->tableName.' WHERE id = ? ';
        $values = array($key);
        if (!$force) {
            $query .= ' AND fresh_until < ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function removeByTag($tag, $force = false)
    {
        $tags = array_values((array) $tag);
        $query = 'DELETE FROM '.$this->tableName.' WHERE ';

        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
            $query .= ' MATCH (tags) AGAINST ("'.$tags.'" IN BOOLEAN MODE) ';
        } else {
            $query .= ' (tags LIKE "%##'.implode('%" OR tags LIKE "%##', $tags).'%") ';
        }

        $values = array();
        if (!$force) {
            $query .= ' AND fresh_until < ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function removeAll($force = false)
    {
        $query = 'DELETE FROM '.$this->tableName.' ';
        $values = array();
        if (!$force) {
            $query .= ' WHERE fresh_until < ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function outdate($key, $force = false)
    {
        $query = 'UPDATE '.$this->tableName.' SET
            fresh_until = ?,
            queue_fresh_until = 0,
            queued = 0
            WHERE id = ? ';

        $values = array(time()-1, $key);
        if (!$force) {
            $query .= ' AND fresh_until >= ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function outdateByTag($tag, $force = false)
    {
        $tags = array_values((array) $tag);
        $query = 'UPDATE '.$this->tableName.' SET
            fresh_until = ?,
            queue_fresh_until = 0,
            queued = 0
            WHERE ';

        if ($this->useFulltextTags) {
            $tags = implode(' ', array_map(function($tag) { return preg_replace('/[^a-zA-Z0-9_]/', '_', $tag); }, $tags));
            $query .= ' MATCH (tags) AGAINST ("'.$tags.'" IN BOOLEAN MODE) ';
        } else {
            $query .= ' (tags LIKE "%##'.implode('%" OR tags LIKE "%##', $tags).'%") ';
        }

        $values = array(time()-1);
        if (!$force) {
            $query .= ' AND fresh_until >= ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function outdateAll($force = falsel)
    {
        $query = 'UPDATE '.$this->tableName.' SET
            fresh_until = ?,
            queue_fresh_until = 0,
            queued = 0
            ';

        $values = array(time()-1);
        if (!$force) {
            $query .= ' WHERE fresh_until >= ?';
            $values[] = time();
        }
        $stmt = $this->db->prepare($query);
        return $stmt->execute($values);
    }

    public function clearQueue($channel = true)
    {
        if ($channel !== true) {
            $query = 'UPDATE '.$this->tableName.' SET
            queue_fresh_until = 0,
            queued = 0
            WHERE queued = ? ';

            $stmt = $this->db->prepare($query);
            return $stmt->execute(array($channel));
        } else {
            $query = 'UPDATE '.$this->tableName.' SET
            queue_fresh_until = 0,
            queued = 0
            WHERE queued != 0';

            $stmt = $this->db->prepare($query);
            return $stmt->execute(array());
        }
    }

    public function cleanup($outdatedFor = 0)
    {
        $query = 'DELETE FROM '.$this->tableName.' WHERE fresh_until < ?';
        $stmt = $this->db->prepare($query);
        return $stmt->execute(array(time()-$outdatedFor));
    }

    public function obtainLock($key, $lockFor, $timeout = null)
    {
        $waitUntil = microtime(true) + ($timeout !== null ? (float) $timeout : (float) $lockFor);
        $lockKey = md5(microtime().rand(100000,999999));
        do {
            $this->set($key.'._lock', $lockKey, $lockFor);
            $data = $this->get($key.'._lock');
            if ($data && $data['data'] == $lockKey) {
                return $lockKey;
            } elseif ($data && !$data['is_fresh']) {
                $this->releaseLock($key, $data['data']);
            } else {
                usleep(50000);
            }
        } while(microtime(true) < $waitUntil);
        return false;
    }

    public function releaseLock($key, $lockKey)
    {
        if ($lockKey === true) {
            return $this->remove($key.'._lock', true);
        }
        $stmt = $this->stmtReleaseLock ?: $this->stmtReleaseLock = $this->db->prepare('DELETE FROM '.$this->tableName. ' WHERE id = ? AND data = ?');
        $stmt->execute(array($key.'._lock', serialize($lockKey)));
        return true;
    }

}
