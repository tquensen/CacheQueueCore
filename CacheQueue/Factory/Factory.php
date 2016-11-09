<?php
namespace CacheQueue\Factory;
        
use CacheQueue\Client\ClientInterface;
use CacheQueue\Connection\ConnectionInterface;
use CacheQueue\Logger\LoggerInterface;
use CacheQueue\Worker\WorkerInterface;

class Factory implements FactoryInterface
{
    private $config;

    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * @var WorkerInterface
     */
    private $worker;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var ConnectionInterface
     */
    private $connection;
    
    public function __construct($config)
    {
        $this->config = $config;
    }

    /**
     * @return ClientInterface
     */
    public function getClient()
    {
        if (!$this->client) {
            $clientClass = $this->config['classes']['client'];
            $this->client = new $clientClass($this->getConnection());          
            $this->client->setWorker($this->getWorker());
        }
        return $this->client;
    }

    /**
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        if (!$this->connection) {
            $connectionClass = $this->config['classes']['connection'];
            $this->connection = new $connectionClass($this->config['connection']);
        }
        return $this->connection;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        if (!$this->logger) {
            $loggerClass = $this->config['classes']['logger'];
            $this->logger = new $loggerClass($this->config['logger']);
        }
        return $this->logger;
    }

    /**
     * @return WorkerInterface
     */
    public function getWorker()
    {
        if (!$this->worker) {
            $workerClass = $this->config['classes']['worker'];
            $this->worker = new $workerClass($this->getConnection(), $this->config['tasks']);
            $this->worker->setLogger($this->getLogger());
        }
        return $this->worker;
    }

}
