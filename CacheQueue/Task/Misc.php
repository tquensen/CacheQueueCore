<?php
namespace CacheQueue\Task;

/**
 * the 'misc' class contains various simple tasks
 * 
 * if a task returns null/nothing, the cache entry will get updated with the new
 * fresh until date without updating the data
 * if you want do remove/clear the data, you should return false
 * 
 * throw a \CacheQueue\Exception\Exception for non-critical errors (this will be logged)
 * any other exceptions will terminate the (default) worker process 
 * in any case, when throwing an exception, the cache entry is removed from queue and wont get updated,
 * which means that it will get queued again on the next request
 *
 * throw a \CacheQueue\Exception\BuryException to bury the job for a given time,
 * meaning the data wont get updated and no new job will get queued for that key
 * for the given buryTime (default = null will bury the job for the remaining queue_fresh_for time)
 *
 * throw a \CacheQueue\Exception\RequeueException to queue the job again with the same data (task, params, ...)
 * that will be executed after the given delay (defaults to the remaining queue_fresh_for time)
 * with a new freshFor time (in addition to the delay, defaults to the remaining queue_fresh_for time)
 *
 */
class Misc
{
    /**
     * the 'store' task simply caches the submitted params as the data
     */
    public function store($params, $config, $job, $worker)
    {
        return $params;
    }
    
    /**
     * reads and stores the content of a url
     */
    public function loadUrl($params, $config, $job, $worker)
    {
        if (empty($params['url'])) {
            throw new \Exception('Parameters url is required!');
        }
        
        if (!empty($params['context'])) {
            $context = @stream_context_create($params['context']);
            $result = @file_get_contents($params['url'], null, $context);
        } else {
            $result = @file_get_contents($params['url']);
        }
        
        if ($result === false) {
            if (empty($params['disableErrorLog']) && $logger = $worker->getLogger()) {
                $logger->logError('loadUrl: failed for URL '.$params['url']);
            }
            return;
        }
        
        if (!empty($params['format']) && $params['format'] == 'json') {
            $result = @json_decode($result, true);
            if ($result === null) {
                if (empty($params['disableErrorLog']) && $logger = $worker->getLogger()) {
                    $logger->logError('loadUrl: failed to convert data to json for URL '.$params['url']);
                }
                return;
            }
        } elseif (!empty($params['format']) && $params['format'] == 'xml') {
            $preverrors = libxml_use_internal_errors(true);
            $return = null;
            try {
                $xml = new \DOMDocument();
                if (!@$xml->loadXML($result)) {
                    if (empty($params['disableErrorLog']) && $logger = $worker->getLogger()) {
                        $logger->logError('loadUrl: failed to convert data to XML for URL '.$params['url']);
                    }
                } else {
                    $result = $xml;
                }   
            } catch (\Exception $e) {
                libxml_use_internal_errors($preverrors);
                throw $e;
            }
            libxml_use_internal_errors($preverrors);
        }
        
        if ($logger = $worker->getLogger()) {
            $logger->logDebug('loadUrl: successful for URL '.$params['url']);
        }
        return $result;
    }
}
