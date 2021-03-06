<?php
namespace CacheQueue\Connection;

interface ConnectionInterface
{
    public function __construct($config = array());
    
    /**
     * get a cached entry
     * 
     * @param string $key the key to get
     * @param bool $onlyFresh true to return the entry only if it is fresh, false (default) to return also outdated entries
     * @return mixed the value or the result array (depending on $onlyValue) or false if not found 
     */
    public function get($key, $onlyFresh = false);
    
    /**
     * get multiple cached entries by a tag
     * 
     * @param string $tag the key to get
     * @param bool $onlyFresh true to return only fresh entries, false (default) to return also outdated entries
     * @return array an array of cache entries
     */
    public function getByTag($tag, $onlyFresh = false);
    
    /**
     * get a cached entries value
     * 
     * @param string $key the key to get
     * @param bool $onlyFresh true to return the value only if it is fresh, false (default) to return also outdated values
     * @return mixed the value or false if not found 
     */
    public function getValue($key, $onlyFresh = false);
    
    /**
     * save cache data
     * 
     * @param string $key the key to save the data for
     * @param mixed $data the data to be saved
     * @param int $freshFor number of seconds that the data is fresh
     * @param bool $force true to force the save even if the data is still fresh
     * @param array|string $tags one or multiple tags to assign to the cache entry
     * @return bool if the save was sucessful 
     */
    public function set($key, $data, $freshFor, $force = false, $tags = array());

    /**
     * updated the freshFor value of an existing cache entry
     *
     * @param string $key the key to refresh the data for
     * @param int $freshFor number of seconds that the data is fresh
     * @param bool $force true to force the refresh even if the data is still fresh
     */
    public function refresh($key, $freshFor, $force = false);

    /**
     * add a queue entry 
     * 
     * @param string $key the key to save the data for or true to store as temporary entry
     * @param string $task the task to run
     * @param mixed $params parameters for the task
     * @param int $freshFor number of seconds that the data is fresh
     * @param bool $force true to force the queue even if the data is still fresh
     * @param array|string $tags one or multiple tags to assign to the cache entry
     * @param int $priority the execution priority of the queued job, 0=high prio/early execution, 100=low prio/late execution
     * @param int $delay number of seconds to wait before the task should run (actual delay may be greater if workers are too busy)
     * @param int $channel the queue channel, default = 1
     * @return bool if the queue was sucessful 
     */
    public function queue($key, $task, $params, $freshFor, $force = false, $tags = array(), $priority = 50, $delay = 0, $channel = 1);
    
    /**
     * gets a queued entry and removes it from queue
     * 
     * @param int $workerId a unique id of the current worker
     * @param int $channel the queue channel, default = 1
     *
     * @return array|bool the job data or false if no job was found 
     */
    public function getJob($workerId, $channel = 1);
    
    /**
     * resets the queue_* data
     *
     * @param string $key the key to update the job status for
     * @param int $workerId a unique id of the current worker 
     * @param int $newQueueFreshFor 0 to reset the jobData or a new freshUntil timestamp to bury the job until that date
     *
     */
    public function updateJobStatus($key, $workerId, $newQueueFreshFor = 0);
            
    /**
     * returns the number of queued cache entries
     *
     * @param int|true $channel the queue channel or true to return count for all channels
     * 
     * @return int the number of entries in the queue
     */
    public function getQueueCount($channel = true);
    
    /**
     * get the number of matching entries
     * 
     * @param bool|null $fresh if true, only fresh entries are counted, if false, only outdated entries are counted, null (default) to count all entries
     */
    public function countAll($fresh = null);
    
    /**
     * get the number of matching entries
     * 
     * @param array|string $tag multiple tags used to find the entries to remove
     * @param bool|null $fresh if true, only fresh entries are counted, if false, only outdated entries are counted, null (default) to count all entries
     */
    public function countByTag($tag, $fresh = null);
    
    /**
     * removes an entry from cache
     * 
     * @param string $key the key of the entry to remove from cache
     * @param bool $force if false (default), the entry will only be removed if it is outdated
     * @return bool if the request was successful 
     */
    public function remove($key, $force = false);
    
    /**
     * removes all entries from cache
     * 
     * @param bool $force if false (default), only outdated entries will be removed
     * @return bool if the request was successful 
     */
    public function removeAll($force = false);
    
    /**
     * removes all entries with the given tag(s) from cache
     * 
     * @param array|string $tag multiple tags used to find the entries to remove
     * @param bool $force if false (default), only outdated entries will be removed
     * @return bool if the request was successful 
     */
    public function removeByTag($tag, $force = false);
    
    /**
     * outdates an entry in cache (sets fresh_until to the past)
     * 
     * @param string $key the key of the entry to outdate
     * @param bool $force if false (default), the entry will only get outdated if it is fresh
     * @return bool if the request was successful 
     */
    public function outdate($key, $force = false);
    
    /**
     * outdates all entries in cache (sets fresh_until to the past)
     * 
     * @param bool $force if false (default), only fresh entries will be outdated 
     * @return bool if the request was successful 
     */
    public function outdateAll($force = false);
    
    /**
     * outdates all entries with the given tag(s) in cache (sets fresh_until to the past)
     * 
     * @param array|string $tag multiple tags used to find the entries to outdate
     * @param bool $force if false (default), only fresh entries will be outdated 
     * @return bool if the request was successful 
     */
    public function outdateByTag($tag, $force = false);
    
    /**
     * resets the queue status for all queued entries
     *
     * @param int|true $channel the queue channel or true to return count for all channels
     * 
     * @return bool if the request was successful 
     */
    public function clearQueue($channel = true);
    
    /**
     * removes all entries which are outdated for a specific time
     * 
     * @param int $outdatedFor remove only those entries that are outdated for at lease this number of seconde (default = 0)
     */
    public function cleanup($outdatedFor = 0);
    
    /**
     * tries to obtain a lock for the given key
     * 
     * @param string $key the key
     * @param int $lockFor locktime in seconds, ater that another lock can be obtained
     * @param float|null $timeout time to wait (in seconds, eg 0.05 for 50ms) for another lock to be released or null to use $lockFor
     * @return string|bool returns the lockkey if successful, false if not
     */
    public function obtainLock($key, $lockFor, $timeout = null);
    
    /**
     * release a lock
     * 
     * @param string $key the key to release the lock for
     * @param string|bool $lockKey only release the lock with this lockKey, true to force a release
     * @return bool returns true if the lock was released, false if not (eg wrong lockKey)
     */
    public function releaseLock($key, $lockKey);
}
