<?php
namespace CacheQueue\Connection;

class MongoDB implements ConnectionInterface
{
    private $dbName = null;
    private $collectionName = null;
    
    private $db = null;
    private $collection = null;
    
    public function __construct($config = array())
    {
        $driverOptions = !empty($config['driverOptions']) ? $config['driverOptions'] : array();
        $driverOptions['typeMap'] = array('root' => 'array', 'document' => 'array', 'array' => 'array');

        $mongo = new \MongoDB\Client(!empty($config['uri']) ? $config['uri'] : 'mongodb://localhost:27017', !empty($config['uriOptions']) ? $config['uriOptions'] : array(), $driverOptions);

        $this->dbName = !empty($config['database']) ? $config['database'] : 'cachequeue';
        $this->collectionName = !empty($config['collection']) ? $config['collection'] : 'cache';
        
        $this->db = $mongo->{$this->dbName};
        $this->collection = $this->db->{$this->collectionName};
    }
    
    public function setup()
    {
        $this->collection->createIndex(array('queued' => -1, 'queue_start' => 1, 'queue_priority' => 1));

        $this->collection->createIndex(array('fresh_until' => 1, 'tags' => 1));

        $this->collection->createIndex(array('tags' => 1));
    }

    public function get($key, $onlyFresh = false)
    {
        $result = $this->collection->findOne(array('_id' => $key));
        if (!$result) {
            return false;
        }
        $return = array();
        
        $return['key'] = $result['_id'];
        //$return['queued'] = !empty($result['queued']);
        $return['fresh_until'] = !empty($result['fresh_until']) ? $result['fresh_until']->toDateTime()->getTimestamp() : 0;
        $return['is_fresh'] = $return['fresh_until'] > time();

        $return['date_set'] = !empty($result['date_set']) ? $result['date_set']->toDateTime()->getTimestamp() : 0;
        
        $return['queue_fresh_until'] = !empty($result['queue_fresh_until']) ? $result['queue_fresh_until']->toDateTime()->getTimestamp() : 0;
        $return['queue_is_fresh'] = (!empty($result['queue_fresh_until']) && $result['queue_fresh_until']->toDateTime()->getTimestamp() > time());
        $return['tags'] = isset($result['tags']) ? $result['tags'] : array();
        $return['task'] = !empty($result['task']) ? $result['task'] : null;
        $return['params'] = !empty($result['params']) ? $result['params'] : null;
        $return['data'] = isset($result['data']) ? $result['data'] : false;

        return (!$onlyFresh || $return['is_fresh']) ? $return : false;
    }
    
    public function getByTag($tag, $onlyFresh = false)
    {
        $tags = array_values((array) $tag);
        $return = array();
        
        if ($onlyFresh) {
            $results = $this->collection->find(
                array(
                    'fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)),
                    'tags' => array('$in' => $tags)
                )
            );
        } else {
            $results = $this->collection->find(
                array(
                    'tags' => array('$in' => $tags)
                )
            );
        }
        
        foreach ($results as $result) {
            $entry = array();
            $entry['key'] = $result['_id'];
            //$entry['queued'] = !empty($result['queued']);
            $entry['fresh_until'] = !empty($result['fresh_until']) ? $result['fresh_until']->toDateTime()->getTimestamp() : 0;
            $entry['is_fresh'] = $entry['fresh_until'] > time();

            $entry['date_set'] = !empty($result['date_set']) ? $result['date_set']->toDateTime()->getTimestamp() : 0;
            
            $entry['queue_fresh_until'] = !empty($result['queue_fresh_until']) ? $result['queue_fresh_until']->toDateTime()->getTimestamp() : 0;
            $entry['queue_is_fresh'] = (!empty($result['queue_fresh_until']) && $result['queue_fresh_until']->toDateTime()->getTimestamp() > time());
            $entry['tags'] = isset($result['tags']) ? $result['tags'] : array();
            $entry['task'] = !empty($result['task']) ? $result['task'] : null;
            $entry['params'] = !empty($result['params']) ? $result['params'] : null;
            $entry['data'] = isset($result['data']) ? $result['data'] : false;
            $return[] = $entry;
        }
        
        unset($results);
        
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
        $result = $this->db->command(array(
            'findAndModify' => $this->collectionName,
            'query' => array('queued' => $channel, 'queue_start' => array('$lte' => new \MongoDB\BSON\UTCDateTime(time() * 1000))),
            'sort' => array('queued' => -1, 'queue_start' => 1, 'queue_priority' => 1),
            'update' => array('$set' => array('queued' => null, 'queued_worker' => $workerId))
        ));
        
        if (empty($result['ok']) || empty($result['value'])) {
            return false;
        }
        
        $return = array();
        
        $return['key'] = $result['value']['_id'];
        $return['fresh_until'] = !empty($result['value']['queue_fresh_until']) ? $result['value']['queue_fresh_until']->toDateTime()->getTimestamp() : 0;
        $return['fresh_for'] = !empty($result['value']['queue_fresh_until']) && !empty($result['value']['queue_start']) ? $result['value']['queue_fresh_until']->toDateTime()->getTimestamp() - $result['value']['queue_start']->toDateTime()->getTimestamp() : 0;
        $return['tags'] = !empty($result['value']['queue_tags']) ? $result['value']['queue_tags'] : null;
        $return['task'] = !empty($result['value']['task']) ? $result['value']['task'] : null;
        $return['params'] = !empty($result['value']['params']) ? $result['value']['params'] : null;
        $return['data'] = isset($result['value']['data']) ? $result['value']['data'] : null;
        $return['channel'] = isset($result['value']['queued']) ? $result['value']['queued'] : 0;
        $return['priority'] = isset($result['value']['queue_priority']) ? $result['value']['queue_priority'] : 50;
        $return['temp'] = !empty($result['value']['temp']);
        $return['worker_id'] = $workerId;
        
        return $return;
    }
    
    public function updateJobStatus($key, $workerId, $newQueueFreshFor = 0)
    {
        try {
            return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key,
                        'queued_worker' => $workerId
                    ),
                    array('$set' => array(
                        'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime($newQueueFreshFor > 0 ? ($newQueueFreshFor + time()) * 1000 : 0),
                        'queued_worker' => null
                    ))
                );
        }  catch (\MongoCursorException $e) {
            if ($e->getCode() == 11000) {
                return false;
            }
            throw $e;
        }
    }
    
    public function set($key, $data, $freshFor, $force = false, $tags = array())
    {
        $freshUntil = new \MongoDB\BSON\UTCDateTime((time() + $freshFor) * 1000);
        
        $tags = array_values((array) $tags);
        
        try {
            if ($force) {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key
                    ),
                    array('$set' => array(
                        'fresh_until' => $freshUntil,
                        'data' => $data,
                        'tags' => $tags,
                        'date_set' => new \MongoDB\BSON\UTCDateTime(time() * 1000)
                    )),
                    array('upsert' => true)
                );
            } else {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key,
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)))
                        ),
                    ),
                    array('$set' => array(
                        'fresh_until' => $freshUntil,
                        'data' => $data,
                        'tags' => $tags,
                        'date_set' => new \MongoDB\BSON\UTCDateTime(time() * 1000)
                    )),
                    array('upsert' => true)
                );
            }
        } catch (\MongoCursorException $e) {
            if ($e->getCode() == 11000) {
                return true;
            }
            throw $e;
        }
        
    }

    public function refresh($key, $freshFor, $force = false)
    {
        $freshUntil = new \MongoDB\BSON\UTCDateTime((time() + $freshFor) * 1000);

        try {
            if ($force) {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key
                    ),
                    array('$set' => array(
                        'fresh_until' => $freshUntil,
                        'date_set' => new \MongoDB\BSON\UTCDateTime(time() * 1000)
                    ))
                );
            } else {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key,
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)))
                        ),
                    ),
                    array('$set' => array(
                        'fresh_until' => $freshUntil,
                        'date_set' => new \MongoDB\BSON\UTCDateTime(time() * 1000)
                    ))
                );
            }
        } catch (\MongoCursorException $e) {
            if ($e->getCode() == 11000) {
                return true;
            }
            throw $e;
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
        
        $freshUntil = new \MongoDB\BSON\UTCDateTime((time() + $freshFor + $delay) * 1000);
        $queueStart = new \MongoDB\BSON\UTCDateTime((time() + $delay) * 1000);
        
        $tags = array_values((array) $tags);
        
        try {
            if ($force) {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key
                    ),
                    array('$set' => array(
                        'queue_fresh_until' => $freshUntil,
                        'queue_tags' => $tags,
                        'queued' => $channel,
                        'queued_worker' => null,
                        'task' => $task,
                        'params' => $params,
                        'temp' => $temp,
                        'queue_priority' => $priority,
                        'queue_start' => $queueStart
                    )),
                    array('upsert' => true)
                );
            } else {
                return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key,
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => $queueStart)),
                            array('queue_fresh_until' => array('$gte' => new $queueStart))
                        )
                    ),
                    array('$set' => array(
                        'queue_fresh_until' => $freshUntil,
                        'queue_tags' => $tags,
                        'queued' => $channel,
                        'queued_worker' => null,
                        'task' => $task,
                        'params' => $params,
                        'temp' => $temp,
                        'queue_priority' => $priority,
                        'queue_start' => $queueStart
                    )),
                    array('upsert' => true)
                );
            }
        }  catch (\MongoCursorException $e) {
            if ($e->getCode() == 11000) {
                return true;
            }
            throw $e;
        }
    }

    public function getQueueCount($channel = true)
    {
        return $channel === true ?  $this->collection->count(array('queued' => array('$gt' => 0))) : $this->collection->count(array('queued' => $channel));
    }
    
    public function countAll($fresh = null)
    {
        if ($fresh === null) {
                return (int) $this->collection->count();
        } else {
            if ($fresh) {
                return (int) $this->collection->count(array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000))));
            } else {
                return (int) $this->collection->count(array('fresh_until' => array('$lt' => new \MongoDB\BSON\UTCDateTime(time() * 1000))));
            }
        }
    }
    
    public function countByTag($tag, $fresh = null)
    {
        $tags = array_values((array) $tag);
        if ($fresh === null) {
            return (int) $this->collection->count(array('tags' => array('$in' => $tags)));
        } else {
            if ($fresh) {
                return (int) $this->collection->count(array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)), 'tags' => array('$in' => $tags)));
            } else {
                return (int) $this->collection->count(array('fresh_until' => array('$lt' => new \MongoDB\BSON\UTCDateTime(time() * 1000)), 'tags' => array('$in' => $tags)));
            }
        }
    }
    
    public function remove($key, $force = false)
    {
        if (!$force) {
            return (bool) $this->collection->deleteOne(
                    array(
                        '_id' => $key,
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)))
                        )
                    )
                );
        } else {
            return (bool) $this->collection->deleteOne(
                array(
                    '_id' => $key
                )
            );
        }
    }
    
    public function removeByTag($tag, $force = false)
    {
        $tags = array_values((array) $tag);
        if (!$force) {
            return (bool) $this->collection->deleteMany(
                    array(
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)))
                        ),
                        'tags' => array('$in' => $tags)  
                    )
                );
        } else {
            return (bool) $this->collection->deleteMany(
                array('tags' => array('$in' => $tags))
            );
        }
    }
    
    public function removeAll($force = false)
    {
        if (!$force) {
            return (bool) $this->collection->deleteMany(
                    array(
                        '$nor' => array(
                            array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000)))
                        )
                    )
                );
        } else {
            return (bool) $this->collection->deleteMany(
                array()
            );
        }
    }
    
    public function outdate($key, $force = false)
    {
        if (!$force) {
            return (bool) $this->collection->updateOne(
                    array(
                        '_id' => $key,
                        'fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime(time() * 1000))
                    ),
                    array('$set' => array(
                        'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queued' => false
                    ))
                );
        } else {
            return (bool) $this->collection->updateOne(
                array(
                    '_id' => $key,
                ),
                array('$set' => array(
                    'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queued' => false
                ))
            );
        }
    }
    
    public function outdateByTag($tag, $force = false)
    {
        $tags = array_values((array) $tag);
        if (!$force) {
            return (bool) $this->collection->updateMany(
                    array(
                        'fresh_until' => array('$gt' => new \MongoDB\BSON\UTCDateTime(time() * 1000)),
                        'tags' => array('$in' => $tags)
                    ),
                    array('$set' => array(
                        'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queued' => false
                    ))
                );
        } else {
            return (bool) $this->collection->updateMany(
                array(
                    'tags' => array('$in' => $tags)
                ),
                array('$set' => array(
                    'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queued' => false
                ))
            );
        }
    }
    
    public function outdateAll($force = false)
    {
        if (!$force) {
            return (bool) $this->collection->updateMany(
                    array(
                        'fresh_until' => array('$gt' => new \MongoDB\BSON\UTCDateTime(time() * 1000))
                    ),
                    array('$set' => array(
                        'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                        'queued' => false
                    ))
                );
        } else {
            return (bool) $this->collection->updateMany(
                array(
                ),
                array('$set' => array(
                    'fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                    'queued' => false
                ))
            );
        }
    }
    
    public function clearQueue($channel = true)
    {
        return (bool) $this->collection->updateMany(
            array(
                'queued' => $channel !== true ? $channel : array('$gt' => 0)
            ),
            array('$set' => array(
                'queue_fresh_until' => new \MongoDB\BSON\UTCDateTime((time() - 1) * 1000),
                'queued' => false
            ))
        );
    }
    
    public function cleanup($outdatedFor = 0)
    {
        return (bool) $this->collection->deleteMany(
                array(
                    '$nor' => array(
                        array('fresh_until' => array('$gte' => new \MongoDB\BSON\UTCDateTime((time()-$outdatedFor) * 1000))),
                    ),
                    'queued' => false
                )
            );
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
        $this->collection->deleteOne(
            array(
                '_id' => $key.'._lock',
                'data' => $lockKey
            )
        );
        return true;
    }
    
}
