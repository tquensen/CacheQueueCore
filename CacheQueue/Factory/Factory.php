<?php
namespace CacheQueue\Factory;
        
class Factory implements FactoryInterface
{
    private $config;
    private $client;
    private $worker;
    private $logger;
    private $connection;
    
    public function __construct($config)
    {
        $this->config = $config;
    }

    public function getClient()
    {
        if (!$this->client) {
            $clientClass = $this->config['classes']['client'];
            $this->client = new $clientClass($this->getConnection());          
            $this->client->setWorker($this->getWorker());
        }
        return $this->client;
    }

    public function getConnection()
    {
        if (!$this->connection) {
            $connectionClass = $this->config['classes']['connection'];
            $this->connection = new $connectionClass($this->config['connection']);
        }
        return $this->connection;
    }

    public function getLogger()
    {
        if (!$this->logger) {
            $loggerClass = $this->config['classes']['logger'];
            $this->logger = new $loggerClass($this->config['logger']);
        }
        return $this->logger;
    }

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
