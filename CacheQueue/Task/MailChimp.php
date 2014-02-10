<?php
namespace CacheQueue\Task;
use CacheQueue\Exception\Exception;

/**
 * MailChimp API tasks
 *  
 */
class MailChimp
{
    
    public function execute($params, $config, $job, $worker)
    {
        if (empty($params['method'])) {
            throw new \Exception('parameter method is required!');
        }
        
        if (empty($config['apiKey']) && empty($params['apiKey'])) {
            throw new \Exception('config parameter apiKey is required!');
        }
        
        $options = array();
        if (isset($config['options'])) {
            $options = $config['options'];
        }
        if (isset($params['options'])) {
            $options = array_merge($options, $params['options']);
        }
        
        $mc = new \Mailchimp(isset($params['apiKey']) ? $params['apiKey'] : $config['apiKey'], $options);

        try {
            return $mc->call($params['method'], isset($params['parameter']) ? $params['parameter'] : array());
        } catch (\Exception $e) {
            throw new Exception('MailChimp API request '.$params['method']. ' failed with error '.$e->getCode().': '.$e->getMessage());
        }
    }
    
}
