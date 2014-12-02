<?php
namespace CacheQueue\Worker;
use CacheQueue\Connection\ConnectionInterface,
    CacheQueue\Logger\LoggerInterface,
    CacheQueue\Exception\Exception,
    CacheQueue\Exception\BuryException;

class Basic implements WorkerInterface
{
    private $connection;
    private $tasks = array();
    
    private $workerId = null;
    
    private $logger = null;
    
    public function __construct(ConnectionInterface $connection, $tasks, $config = array())
    {
        $this->connection = $connection;
        $this->tasks = $tasks;
        
        $this->workerId = getmypid();
    }
    
    public function work($job)
    {
        if (!$job) {
            throw new Exception('no job given.');
        }
        
        $task = $job['task'];
        $params = $job['params'];
        $freshUntil = $job['fresh_until'];
        $temp = !empty($job['temp']);
        
        try {
            if (empty($this->tasks[$task])) {
                throw new Exception('invalid task '.$task.'.');
            }

            $result = $this->executeTask($task, $params, $job);

            if ($temp) {
                $this->connection->remove($job['key'], true);
            } elseif ($result !== null) {
                $this->connection->set($job['key'], $result, $freshUntil-time(), false, $job['tags']);
                $this->connection->updateJobStatus($job['key'], $job['worker_id']);
            } else {
                $this->connection->refresh($job['key'], $freshUntil-time(), false);
                $this->connection->updateJobStatus($job['key'], $job['worker_id']);
            }
        } catch (BuryException $e) {
            $this->connection->updateJobStatus($job['key'], $job['worker_id'], $e->getBuryTime() !== null ? $e->getBuryTime() : $freshUntil-time());
            throw $e;
        }catch (\Exception $e) {
            $this->connection->updateJobStatus($job['key'], $job['worker_id']);
            throw $e;
        }

        return $result;
    }
    
    public function executeTask($task, $params, $job = false)
    {
        $taskData = (array) $this->tasks[$task];
        $taskClass = $taskData[0];
        $taskMethod = !empty($taskData[1]) ? $taskData[1] : 'execute';
        $taskConfig = !empty($taskData[2]) ? $taskData[2] : array();

        if (!class_exists($taskClass)) {
            throw new Exception('class '.$taskClass.' not found.');
        }
        if (!method_exists($taskClass, $taskMethod)) {
            throw new Exception('method '.$taskMethod.' in in class '.$taskClass.' not found.');
        }

        $taskObject = new $taskClass;
        $result = $taskObject->$taskMethod($params, $taskConfig, $job, $this);
        unset($taskObject);
        
        return $result;
    }

    public function getJob($channel = 1)
    {
        return $this->connection->getJob($this->workerId, $channel);
    }
    
    public function getWorkerId()
    {
        return $this->workerId;
    }
    
    public function setLogger(LoggerInterface $logger)
    {
         $this->logger = $logger;
    }
    
    public function getLogger()
    {
        return $this->logger;
    }
    
    public function setConnection(ConnectionInterface $connection)
    {
         $this->connection = $connection;
    }
    
    public function getConnection()
    {
        return $this->connection;
    }

}
