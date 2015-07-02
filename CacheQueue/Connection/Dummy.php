<?php
namespace CacheQueue\Connection;

class Dummy implements ConnectionInterface
{
    private $dbName = null;
    private $collectionName = null;
    
    private $db = null;
    private $collection = null;
    
    private $safe = null;
    
    public function __construct($config = array())
    {
        
    }

    public function get($key)
    {
        return false;
    }
    
    
    
    public function getValue($key, $onlyFresh = false)
    {
        return false;
    }

    public function getJob($workerId, $channel = 1)
    {
        return false;
    }
    
    public function updateJobStatus($key, $workerId, $newQueueFreshFor = 0)
    {
        return false;
    }
    
    public function set($key, $data, $freshFor, $force = false, $tags = array())
    {
        return false;
    }

    public function refresh($key, $freshFor, $force = false)
    {
        return false;
    }

    public function queue($key, $task, $params, $freshFor, $force = false, $tags = array(), $priority = 50, $delay = 0, $channel = 1)
    {
        return false;
    }

    public function getQueueCount($channel = true)
    {
        return 0;
    }
    
    public function countAll($fresh = null)
    {
        return 0;
    }

    public function countByTag($tag, $fresh = null)
    {
        return 0;
    }

    
    public function remove($key, $force = false)
    {
        return false;
    }
    
    public function removeAll($force = false)
    {
        return false;
    }
    
    public function outdate($key, $force = false)
    {
        return false;
    }
    
    public function outdateAll($force = false)
    {
        return false;
    }

    public function obtainLock($key, $lockFor, $timeout = null)
    {
        return false;
    }

    public function outdateByTag($tag, $force = false)
    {
        return false;
    }
    
    public function clearQueue($channel = true)
    {
        return false;
    }
    
    public function releaseLock($key, $lockKey)
    {
        return false;
    }

    public function removeByTag($tag, $force = false)
    {
        return false;
    }

    public function getByTag($tag, $onlyFresh = false)
    {
        return array();
    }

    public function cleanup($outdatedFor = 0)
    {
        return false;
    }
    
}
