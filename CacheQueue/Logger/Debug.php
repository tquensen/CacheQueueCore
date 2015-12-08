<?php
namespace CacheQueue\Logger;

use CacheQueue\Exception;

class Debug implements LoggerInterface
{
    private $showPid = false;
    private $logLevel = 0;
    private $realLogger = null;
    private $stream = null;
    
    public function __construct($config = array())
    {
        $stream = !empty($config['stream']) ? $config['stream'] : 'output';
        $this->showPid = !empty($config['showPid']);
        $this->logLevel = !empty($config['logLevel']) ? $config['logLevel'] : self::LOG_NONE;

        $this->stream = @fopen('php://'.$stream);
        if (!$this->stream) {
            throw new Exception('Could not open stream php://'.$stream);
        }

        if (!empty($config['loggerClass'])) {
            $loggerClass = $config['loggerClass'];
            if (!class_exists($loggerClass)) {
                $connectionFile = !empty($config['loggerFile']) ? $config['loggerFile'] : str_replace('\\', \DIRECTORY_SEPARATOR, trim($loggerClass, '\\')).'.php';
                require_once($connectionFile);
            }
            if (!array_key_exists('logLevel', $config['loggerConfig'])) {
                $config['loggerConfig']['logLevel'] = $this->logLevel;
            }
            $this->realLogger = new $loggerClass($config['loggerConfig']);
        }
    }

    public function logException(\Exception $e)
    {
        if ($this->logLevel & self::LOG_ERROR) {
            $this->doLog((string) $e, 'EXCEPTION');
        }
        if ($this->realLogger) {
            $this->realLogger->logException($e);
        }
    }
    
    public function logError($text)
    {
        if ($this->logLevel & self::LOG_ERROR) {
            $this->doLog($text, 'ERROR   ');
        }
        if ($this->realLogger) {
            $this->realLogger->logError($e);
        }
    }

    public function logNotice($text)
    {
        if ($this->logLevel & self::LOG_NOTICE) {
            $this->doLog($text, 'NOTICE  ');
        }
        if ($this->realLogger) {
            $this->realLogger->logNotice($e);
        }
    }
    
    public function logDebug($text)
    {
        if ($this->logLevel & self::LOG_DEBUG) {
            $this->doLog($text, 'DEBUG  ');
        }
        if ($this->realLogger) {
            $this->realLogger->logDebug($e);
        }
    }
    
    private function doLog($message, $level)
    {
        fwrite($this->stream, date('[Y-m-d H.i:s] ').($this->showPid ? 'PID '.getmypid().' | ' : '').$level.' '.$message."\n");
    }

    public function __destruct()
    {
        if (is_resource($this->stream)) {
            @fclose($this->stream);
        }
    }
}

