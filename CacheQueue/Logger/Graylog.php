<?php
namespace CacheQueue\Logger;

class Graylog implements LoggerInterface
{
    private $graylogHostname = null;
    private $graylogPort = null;
    private $host = '';
    private $showPid = false;
    private $logLevel = 0;
    
    private $publisher = null;
    
    public function __construct($config = array())
    {
        $this->graylogHostname = $config['graylogHostname'];
        $this->graylogPort = $config['graylogPort'];
        $this->host = $config['host'];
        $this->facility = !empty($config['facility']) ? $config['facility'] : 'CacheQueue';
        $this->showPid = !empty($config['showPid']);
        $this->logLevel = !empty($config['logLevel']) ? $config['logLevel'] : self::LOG_NONE;
    }
    
    public function logException($e)
    {
        if ($this->logLevel & self::LOG_ERROR) {
            $this->doLog($e->getMessage(), 3, $e);
        }
    }

    public function logError($text)
    {
        if ($this->logLevel & self::LOG_ERROR) {
            $this->doLog($text, 3);
        }
    }

    public function logNotice($text)
    {
        if ($this->logLevel & self::LOG_NOTICE) {
            $this->doLog($text, 5);
        }
    }
    
    public function logDebug($text)
    {
        if ($this->logLevel & self::LOG_DEBUG) {
            $this->doLog($text, 7);
        }
    }
    
    private function initClient()
    {
        $transport = new \Gelf\Transport\UdpTransport($this->graylogHostname, $this->graylogPort, \Gelf\Transport\UdpTransport::CHUNK_SIZE_LAN);
        $this->publisher = new \Gelf\Publisher();
        $this->publisher->addTransport($transport);
    }
    
    private function doLog($message, $level, $exception = null)
    {
        if (empty($this->publisher)) {
            $this->initClient();
        }
        
        if ($this->showPid) {
            $message = 'PID '.getmypid().' | '.$message;
        }
        
        $gelfMessage = new \Gelf\Message();
        
        $gelfMessage->setShortMessage($message);
        $gelfMessage->setHost($this->host);
        $gelfMessage->setTimestamp(time());
        $gelfMessage->setLevel($level);
        $gelfMessage->setFacility($this->facility);
        if ($exception) {
            $longText = "";
            do {
                $longText .= sprintf(
                    "%s: %s (%d)\n\n%s\n",
                    get_class($exception),
                    $exception->getMessage(),
                    $exception->getCode(),
                    $exception->getTraceAsString()
                );

                $exception = $exception->getPrevious();
            } while ($exception && $longText .= "\n--\n\n");
            $gelfMessage->setFullMessage($longText);
            $gelfMessage->setFile($exception->getFile());
            $gelfMessage->setLine($exception->getLine());
        }
        
        $this->publisher->publish($gelfMessage);
    }

}

