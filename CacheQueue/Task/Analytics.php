<?php

namespace CacheQueue\Task;

use CacheQueue\Exception\Exception;

class Analytics
{

    private $client = null;
    private $service = null;
    private $token = null;
    private $tokenCacheKey = null;

    private function initClient($applicationName, $clientKey, $clientSecret, $refresh_token, $connection, $logger, $googleConfigIniLocation = null)
    {
        $client = new \Google_Client($googleConfigIniLocation);
        $client->setApplicationName($applicationName);
        $client->setClientId($clientKey);
        $client->setClientSecret($clientSecret);
        $client->addScope(\Google_Service_Analytics::ANALYTICS_READONLY);

        $service = new \Google_Service_Analytics($client);

        $this->tokenCacheKey = 'analytics_token_' . md5($clientKey . $clientSecret . $refresh_token);

        $token = $this->getToken($refresh_token, $client, $connection, $logger);
        $client->setAccessToken($token);

        $this->token = $token;
        $this->client = $client;
        $this->service = $service;
        return $this->service;
    }

    public function getMetric($params, $config, $job, $worker)
    {
        $metric = !empty($params['metric']) ? $params['metric'] : false;

        if (empty($config['clientKey']) || empty($config['clientSecret'])) {
            throw new \Exception('Config parameters clientKey and clientSecret are required!');
        }

        if (!$metric || empty($params['profileId']) || empty($params['refreshToken'])) {
            throw new \Exception('parameters metric, profileId and refreshToken are required!');
        }

//        $url = str_replace(array('https://', 'http://'), '', $params['pagePath']);
//        $tmp = explode('/', $url, 2);
//        $path = '/'. (isset($tmp[1]) ? $tmp[1] : '');

        if (empty($config['applicationName'])) {
            $config['applicationName'] = 'unknown';
        }

        $googleConfigIniLocation = !empty($config['googleConfigIniLocation']) ? $config['googleConfigIniLocation'] : null;
        
        $op = !empty($params['operator']) ? $params['operator'] : '==';
        $path = !empty($params['pagePath']) ? $params['pagePath'] : '/';
        $pathStr = !empty($params['pagePath']) ? 'ga:pagePath' . $op . $params['pagePath'] : '';
        $hostStr = !empty($params['hostname']) ? 'ga:hostname==' . $params['hostname'] . ($pathStr ? ';' : '') : '';

        $dateFrom = !empty($params['dateFrom']) ? $params['dateFrom'] : '2005-01-01';
        $dateTo = !empty($params['dateTo']) ? $params['dateTo'] : date('Y-m-d');

        $analyticsBlockKey = '_analytics_block_' . $params['profileId'] . '_' . $config['clientKey'];
        if ($blockReason = $worker->getConnection()->getValue($analyticsBlockKey, true)) {
            throw new Exception('Google Analytics Request was blocked:' . $blockReason);
        }

        if (!empty($params['bulkCacheTime'])) {
            if (!empty($params['bulkCacheFilters'])) {
                $bulkCacheFilters = $params['bulkCacheFilters'];
            } else {
                $bulkCacheFilters = '';
            }
            $bulkCacheKey = 'analytics_cache_' . $metric . '_' . $params['profileId'] . '_' . $dateFrom . '_' . $dateTo . '_' . $bulkCacheFilters;
            $bulkCacheData = $worker->getConnection()->get($bulkCacheKey);
            if (!$bulkCacheData || !$bulkCacheData['is_fresh']) {
                $lockKey = $worker->getConnection()->obtainLock($bulkCacheKey, 60, 70);
                if ($lockKey) {
                    $bulkCacheData = $worker->getConnection()->get($bulkCacheKey);
                    if (!$bulkCacheData || !$bulkCacheData['is_fresh']) {

                        $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

                        if ($logger = $worker->getLogger()) {
                            $logger->logDebug('Analytics getMetric (' . $metric . '): no BulkCache for ' . $bulkCacheKey . ', got lock ' . $lockKey . ' and fetching data');
                        }
                        $bulkCacheStart = microtime(true);
                        try {

                            $bulkCache = array();
                            $bulkCacheTmp = array();
                            if (!empty($params['bulkCacheSplitDays'])) {
                                $splitDays = (int) $params['bulkCacheSplitDays'];
                                $actualDateFrom = $dateFrom;
                                $actualDateTo = date('Y-m-d', strtotime($actualDateFrom) + (86400 * ($splitDays - 1)));
                            } else {
                                $splitDays = false;
                                $actualDateFrom = $dateFrom;
                                $actualDateTo = $dateTo;
                            }

                            $numRequests = 0;
                            $sampledDataInfo = array();

                            do {
                                if ($actualDateTo > $dateTo) {
                                    $actualDateTo = $dateTo;
                                }

                                $startIndex = 1;

                                do {
                                    $tries = 3;
                                    while (true) {
                                        try {
                                            $extra = array(
                                                'dimensions' => 'ga:hostname,ga:pagePath',
                                                'max-results' => 10000,
                                                'start-index' => $startIndex
                                            );
                                            if (!empty($bulkCacheFilters)) {
                                                $extra['filters'] = $bulkCacheFilters;
                                            }
                                            $numRequests++;
                                            $data = $service->data_ga->get('ga:' . $params['profileId'], $actualDateFrom, $actualDateTo, 'ga:' . $metric, $extra);
                                            break;
                                        } catch (\Google_Service_Exception $e) {
                                            $requestErrors = $e->getErrors();
                                            if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                                                if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                                                    if (!--$tries) {
                                                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                                    }
                                                    usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                                                } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                                                    $worker->getConnection()->set($analyticsBlockKey, print_r($e, true), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                                                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                                } else {
                                                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                                }
                                            } else {
                                                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                            }
                                        } catch (\Exception $e) {
                                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                        }
                                    };
                                    $bulkCacheTmp = array_merge($bulkCacheTmp, $data['rows']);
                                    if (!empty($data['containsSampledData'])) {
                                        $sampledDataInfo[] = $data['sampleSize'].'/'.$data['sampleSpace'].' = '.number_format($data['sampleSize'] / $data['sampleSpace'] * 100, 2, ',','.').'%) for '.$actualDateFrom.' - '.$actualDateTo . ', Start Index '.$startIndex;
                                    }
                                } while ($startIndex - 1 + count($data['rows']) < $data['totalResults'] && $startIndex+=10000);

                                if ($splitDays) {
                                    $actualDateFrom = date('Y-m-d', strtotime($actualDateTo) + (86400));
                                    $actualDateTo = date('Y-m-d', strtotime($actualDateFrom) + (86400 * ($splitDays - 1)));
                                }
                            } while ($splitDays && $actualDateFrom <= $dateTo);

                            foreach ($bulkCacheTmp as $tmp) {
                                $key = md5($tmp[1]);
                                if (isset($bulkCache[$key])) {
                                    $bulkCache[$key]['count'] += (int) $tmp[2];
                                } else {
                                    $bulkCache[$key] = array('path' => $tmp[1], 'count' => (int) $tmp[2]);
                                }
                            }
                            $worker->getConnection()->set($bulkCacheKey, $bulkCache, $params['bulkCacheTime'], false, array('analytics', 'bulkcache'));
                            $worker->getConnection()->releaseLock($bulkCacheKey, $lockKey);
                            if ($logger = $worker->getLogger()) {
                                $bulkCacheEnd = microtime(true);
                                if (count($sampledDataInfo)) {
                                    $sampleDataInfo = "\nContains Sampled Data:\n".implode("\n", $sampledDataInfo);
                                } else {
                                    $sampleDataInfo = '';
                                }
                                $logger->logDebug('Analytics getMetric (' . $metric . '): created BulkCache with ' . count($bulkCache) . ' entries for ' . $params['profileId'] . '_' . $dateFrom . '_' . $dateTo . '_' . $bulkCacheFilters . ' / took ' . (number_format($bulkCacheEnd - $bulkCacheStart, 2, ',', '.')) . 's and ' . $numRequests . ' requests'.$sampleDataInfo);
                            }
                        } catch (\Exception $e) {
                            $worker->getConnection()->releaseLock($bulkCacheKey, $lockKey);
                            throw $e;
                        }
                    } else {
                        $worker->getConnection()->releaseLock($bulkCacheKey, $lockKey);
                        $bulkCache = $bulkCacheData['data'];
                    }
                } else {
                    $bulkCache = false;
                }
            } else {
                $bulkCache = $bulkCacheData['data'];
            }
            if (empty($bulkCache)) {
                throw new Exception('Analytics getMetric (' . $metric . ') from BulkCache for ' . $bulkCacheKey . ': FAILED (no cache available or empty BulkCache)');
            }
            $count = 0;
            switch ($op) {
                case '!=':
                    foreach ($bulkCache as $currentCache) {
                        if ($currentCache['path'] != $path) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
                case '=@':
                    foreach ($bulkCache as $currentCache) {
                        if (strpos($currentCache['path'], $path) !== false) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
                case '!@':
                    foreach ($bulkCache as $currentCache) {
                        if (strpos($currentCache['path'], $path) === false) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
                case '=~':
                    $escapedPath = str_replace('/', '\\/', $path);
                    foreach ($bulkCache as $currentCache) {
                        if (preg_match('/' . $escapedPath . '/', $currentCache['path'])) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
                case '!~':
                    $escapedPath = str_replace('/', '\\/', $path);
                    foreach ($bulkCache as $currentCache) {
                        if (!preg_match('/' . $escapedPath . '/', $currentCache['path'])) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
                default:
                    foreach ($bulkCache as $currentCache) {
                        if ($currentCache['path'] == $path) {
                            $count += (int) $currentCache['count'];
                        }
                    }
                    break;
            }

            if ($logger = $worker->getLogger()) {
                $logger->logDebug('Analytics getMetric (' . $metric . ') from BulkCache: ' . (!empty($params['hostname']) ? 'Host=' . $params['hostname'] . ' | ' : '') . 'Path=' . $path . ' | COUNT=' . $count);
            }
        } else {
            $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

            if (!empty($params['splitDays'])) {
                $splitDays = (int) $params['splitDays'];
                $actualDateFrom = $dateFrom;
                $actualDateTo = date('Y-m-d', strtotime($actualDateFrom) + (86400 * ($splitDays - 1)));
            } else {
                $splitDays = false;
                $actualDateFrom = $dateFrom;
                $actualDateTo = $dateTo;
            }

            $count = 0;
            $sampledDataInfo = array();

            do {
                if ($actualDateTo > $dateTo) {
                    $actualDateTo = $dateTo;
                }

                $tries = 3;
                while (true) {
                    try {
                        $parameter = array(
                            'dimensions' => 'ga:pagePath',
                            'sort' => '-ga:' . $metric,
                            'max-results' => 1
                        );
                        if ($hostStr || $pathStr) {
                            $parameter['filters'] = $hostStr . $pathStr;
                        }
                        $data = $service->data_ga->get('ga:' . $params['profileId'], $dateFrom, $dateTo, 'ga:' . $metric, $parameter);
                        break;
                    } catch (\Google_Service_Exception $e) {
                        $requestErrors = $e->getErrors();
                        if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                            if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                                if (!--$tries) {
                                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                }
                                usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                            } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                                $worker->getConnection()->set($analyticsBlockKey, $e->getMessage(), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                            } else {
                                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                            }
                        } else {
                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                        }
                    } catch (\Exception $e) {
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    }
                };

                if ($splitDays) {
                    $actualDateFrom = date('Y-m-d', strtotime($actualDateTo) + (86400));
                    $actualDateTo = date('Y-m-d', strtotime($actualDateFrom) + (86400 * ($splitDays - 1)));
                }
                if (!empty($data['containsSampledData'])) {
                    $sampledDataInfo[] = $data['sampleSize'].'/'.$data['sampleSpace'].' = '.number_format($data['sampleSize'] / $data['sampleSpace'] * 100, 2, ',','.').'%) for '.$actualDateFrom.' - '.$actualDateTo;
                }

                $count += (int) $data['totalsForAllResults']['ga:' . $metric];

            } while ($splitDays && $actualDateFrom <= $dateTo);

            if ($logger = $worker->getLogger()) {
                if (count($sampledDataInfo)) {
                    $sampleDataInfo = "\nContains Sampled Data:\n".implode("\n", $sampledDataInfo);
                } else {
                    $sampleDataInfo = '';
                }
                $logger->logDebug('Analytics getMetric (' . $metric . '): ' . (!empty($params['hostname']) ? 'Host=' . $params['hostname'] . ' | ' : '') . 'Path=' . $path . ' | COUNT=' . $count . $sampleDataInfo);
            }
        }

        return (int) $count;
    }

    public function getPageviews($params, $config, $job, $worker)
    {
        $params['metric'] = 'pageviews';
        return $this->getMetric($params, $config, $job, $worker);
    }

    public function getVisits($params, $config, $job, $worker)
    {
        $params['metric'] = 'visits';
        return $this->getMetric($params, $config, $job, $worker);
    }

    public function getEventData($params, $config, $job, $worker)
    {
        if (empty($config['clientKey']) || empty($config['clientSecret'])) {
            throw new \Exception('Config parameters clientKey and clientSecret are required!');
        }

        if (empty($params['eventCategory']) || empty($params['eventAction']) || empty($params['profileId']) || empty($params['refreshToken'])) {
            throw new \Exception('parameters eventCategory, eventAction, profileId and refreshToken are required!');
        }

        if (empty($config['applicationName'])) {
            $config['applicationName'] = 'unknown';
        }

        $analyticsBlockKey = '_analytics_block_' . $params['profileId'] . '_' . $config['clientKey'];
        if ($blockReason = $worker->getConnection()->getValue($analyticsBlockKey, true)) {
            throw new Exception('Google Analytics Request was blocked:' . $blockReason);
        }
        
        $googleConfigIniLocation = !empty($config['googleConfigIniLocation']) ? $config['googleConfigIniLocation'] : null;

        $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

        $dateFrom = !empty($params['dateFrom']) ? $params['dateFrom'] : '2005-01-01';
        $dateTo = !empty($params['dateTo']) ? $params['dateTo'] : date('Y-m-d');

        $tries = 3;
        while (true) {
            try {
                $data = $service->data_ga->get('ga:' . $params['profileId'], $dateFrom, $dateTo, 'ga:totalEvents,ga:eventValue', array(
                    'dimensions' => 'ga:eventCategory,ga:eventAction',
                    'max-results' => 1,
                    'filters' => 'ga:eventCategory==' . $params['eventCategory'] . ';ga:eventAction==' . $params['eventAction']
                ));
                break;
            } catch (\Google_Service_Exception $e) {
                $requestErrors = $e->getErrors();
                if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                    if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                        if (!--$tries) {
                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                        }
                        usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                    } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                        $worker->getConnection()->set($analyticsBlockKey, $e->getMessage(), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    } else {
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    }
                } else {
                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                }
            } catch (\Exception $e) {
                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
            }
        };


        $numEvents = $data['totalsForAllResults']['ga:totalEvents'];
        $scoreEvents = $data['totalsForAllResults']['ga:eventValue'];

        if ($logger = $worker->getLogger()) {
            $logger->logDebug('Analytics EventData: ' . $params['eventCategory'] . ':' . $params['eventAction'] . ' | COUNT=' . $numEvents . ' | SCORE=' . $scoreEvents);
        }


        return array(
            'dateFrom' => $dateFrom,
            'dateTo' => $dateTo,
            'eventCategory' => $params['eventCategory'],
            'eventAction' => $params['eventAction'],
            'count' => $numEvents,
            'score' => $scoreEvents
        );
    }

    public function getTopUrls($params, $config, $job, $worker)
    {
        if (empty($config['clientKey']) || empty($config['clientSecret'])) {
            throw new \Exception('Config parameters clientKey and clientSecret are required!');
        }

        if (empty($params['profileId']) || empty($params['refreshToken'])) {
            throw new \Exception('parameters, profileId and refreshToken are required!');
        }

        if (empty($config['applicationName'])) {
            $config['applicationName'] = 'unknown';
        }

        $metric = !empty($params['metric']) ? $params['metric'] : 'pageviews';

        $op = !empty($params['operator']) ? $params['operator'] : '==';
        $path = !empty($params['pagePath']) ? $params['pagePath'] : '/';
        $pathStr = !empty($params['pagePath']) ? 'ga:pagePath' . $op . $params['pagePath'] : '';
        $hostStr = !empty($params['hostname']) ? 'ga:hostname==' . $params['hostname'] . ($pathStr ? ';' : '') : '';

        if (!empty($params['count'])) {
            $limit = $params['count'];
        } else {
            $limit = !empty($config['count']) ? $config['count'] : 10;
        }

        $analyticsBlockKey = '_analytics_block_' . $params['profileId'] . '_' . $config['clientKey'];
        if ($blockReason = $worker->getConnection()->getValue($analyticsBlockKey, true)) {
            throw new Exception('Google Analytics Request was blocked:' . $blockReason);
        }

        $googleConfigIniLocation = !empty($config['googleConfigIniLocation']) ? $config['googleConfigIniLocation'] : null;
        
        $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

        $dateFrom = !empty($params['dateFrom']) ? $params['dateFrom'] : '2005-01-01';
        $dateTo = !empty($params['dateTo']) ? $params['dateTo'] : date('Y-m-d');

        $tries = 3;
        while (true) {
            try {
                $parameter = array(
                    'dimensions' => 'ga:pagePath',
                    'sort' => '-ga:'.$metric,
                    'max-results' => $limit
                );
                if ($hostStr || $pathStr) {
                    $parameter['filters'] = $hostStr . $pathStr;
                }
                $data = $service->data_ga->get('ga:' . $params['profileId'], $dateFrom, $dateTo, 'ga:'.$metric, $parameter);
                break;
            } catch (\Google_Service_Exception $e) {
                $requestErrors = $e->getErrors();
                if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                    if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                        if (!--$tries) {
                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                        }
                        usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                    } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                        $worker->getConnection()->set($analyticsBlockKey, $e->getMessage(), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    } else {
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    }
                } else {
                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                }
            } catch (\Exception $e) {
                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
            }
        }

        $topUrlsTmp = array();

        foreach ($data['rows'] as $row) {
            $title = (string) $row[0];
            $count = (int) $row[1];

            $topUrlsTmp[$title] = $count;
        }

        arsort($topUrlsTmp);

        $topUrls = array();
        foreach ($topUrlsTmp as $k => $v) {
            $topUrls[] = array(
                'url' => $k,
                'count' => $v
            );
        }

        if ($logger = $worker->getLogger()) {
            $logger->logDebug('Analytics: TopUrls: ' . (!empty($params['hostname']) ? 'Host=' . $params['hostname'] . ' | ' : '') . 'Path=' . $params['pathPrefix'] . ' | COUNT=' . count($topUrls));
        }


        return $topUrls;
    }

    public function getTopKeywords($params, $config, $job, $worker)
    {
        if (empty($config['clientKey']) || empty($config['clientSecret'])) {
            throw new \Exception('Config parameters clientKey and clientSecret are required!');
        }

        if (empty($params['profileId']) || empty($params['refreshToken'])) {
            throw new \Exception('parameters, profileId and refreshToken are required!');
        }

        if (empty($config['applicationName'])) {
            $config['applicationName'] = 'unknown';
        }

        $metric = !empty($params['metric']) ? $params['metric'] : 'pageviews';
        
        $op = !empty($params['operator']) ? $params['operator'] : '==';
        $path = !empty($params['pagePath']) ? $params['pagePath'] : '/';
        $pathStr = !empty($params['pagePath']) ? 'ga:pagePath' . $op . $params['pagePath'] : '';
        $hostStr = !empty($params['hostname']) ? 'ga:hostname==' . $params['hostname'] . ($pathStr ? ';' : '') : '';

        if (!empty($params['count'])) {
            $limit = $params['count'];
        } else {
            $limit = !empty($config['count']) ? $config['count'] : 10;
        }

        $analyticsBlockKey = '_analytics_block_' . $params['profileId'] . '_' . $config['clientKey'];
        if ($blockReason = $worker->getConnection()->getValue($analyticsBlockKey, true)) {
            throw new Exception('Google Analytics Request was blocked:' . $blockReason);
        }

        $googleConfigIniLocation = !empty($config['googleConfigIniLocation']) ? $config['googleConfigIniLocation'] : null;
        
        $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

        $dateFrom = !empty($params['dateFrom']) ? $params['dateFrom'] : date('Y-m-d', mktime(0, 0, 0, date('m') - 1, date('d'), date('Y')));
        $dateTo = !empty($params['dateTo']) ? $params['dateTo'] : date('Y-m-d');

        $tries = 3;
        while (true) {
            try {
                $parameter = array(
                    'dimensions' => 'ga:keyword',
                    'sort' => '-ga:'.$metric,
                    'max-results' => $limit
                );
                if ($hostStr || $pathStr) {
                    $parameter['filters'] = $hostStr . $pathStr;
                }
                $data = $service->data_ga->get('ga:' . $params['profileId'], $dateFrom, $dateTo, 'ga:'.$metric, $parameter);
                break;
            } catch (\Google_Service_Exception $e) {
                $requestErrors = $e->getErrors();
                if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                    if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                        if (!--$tries) {
                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                        }
                        usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                    } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                        $worker->getConnection()->set($analyticsBlockKey, $e->getMessage(), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    } else {
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    }
                } else {
                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                }
            } catch (\Exception $e) {
                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
            }
        }

        $topKeywordsTmp = array();

        foreach ($data['rows'] as $row) {
            $title = (string) $row[0];
            $count = (int) $row[1];

            $topKeywordsTmp[$title] = $count;
        }

        arsort($topKeywordsTmp);

        $topKeywords = array();
        foreach ($topKeywordsTmp as $k => $v) {
            if (!$k || $k == '(not set)' || $k == '(not provided)') {
                continue;
            }
            $topKeywords[] = array(
                'keyword' => $k,
                'count' => $v
            );
        }

        if ($logger = $worker->getLogger()) {
            $logger->logDebug('Analytics: TopKeywords: ' . (!empty($params['hostname']) ? 'Host=' . $params['hostname'] . ' | ' : '') . 'Path=' . $params['pathPrefix'] . ' | COUNT=' . count($topKeywords));
        }


        return $topKeywords;
    }

    public function customRequest($params, $config, $job, $worker)
    {
        $metrics = !empty($params['metrics']) ? $params['metrics'] : false;

        if (empty($config['clientKey']) || empty($config['clientSecret'])) {
            throw new \Exception('Config parameters clientKey and clientSecret are required!');
        }

        if (!$metrics || empty($params['profileId']) || empty($params['refreshToken'])) {
            throw new \Exception('parameters metric, profileId and refreshToken are required!');
        }

        if (empty($config['applicationName'])) {
            $config['applicationName'] = 'unknown';
        }

        $googleConfigIniLocation = !empty($config['googleConfigIniLocation']) ? $config['googleConfigIniLocation'] : null;

        $parameter = array();

        if (!empty($params['sort'])) {
            $parameter['sort'] = $params['sort'];
        }
        if (!empty($params['maxResults'])) {
            $parameter['max-results'] = $params['maxResults'];
        }
        if (!empty($params['startIndex'])) {
            $parameter['start-index'] = $params['startIndex'];
        }
        if (!empty($params['dimensions'])) {
            $parameter['dimensions'] = $params['dimensions'];
        }
        if (!empty($params['filters'])) {
            $parameter['filters'] = $params['filters'];
        }
        if (!empty($params['segment'])) {
            $parameter['segment'] = $params['segment'];
        }
        if (!empty($params['samplingLevel'])) {
            $parameter['samplingLevel'] = $params['samplingLevel'];
        }

        $dateFrom = !empty($params['dateFrom']) ? $params['dateFrom'] : '2005-01-01';
        $dateTo = !empty($params['dateTo']) ? $params['dateTo'] : date('Y-m-d');

        $service = $this->initClient($config['applicationName'], $config['clientKey'], $config['clientSecret'], $params['refreshToken'], $worker->getConnection(), $worker->getLogger(), $googleConfigIniLocation);

        $analyticsBlockKey = '_analytics_block_' . $params['profileId'] . '_' . $config['clientKey'];

        $tries = 3;
        while (true) {
            try {
                $data = $service->data_ga->get('ga:' . $params['profileId'], $dateFrom, $dateTo, $metrics, $parameter);
                break;
            } catch (\Google_Service_Exception $e) {
                $requestErrors = $e->getErrors();
                if ($e->getCode() == 403 && isset($requestErrors[0]['reason'])) {
                    if ($requestErrors[0]['reason'] == 'quotaExceeded' || $requestErrors[0]['reason'] == 'userRateLimitExceeded') {
                        if (!--$tries) {
                            throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                        }
                        usleep(rand(300000, 500000) + pow(2, 2 - $tries) * 1000000);
                    } elseif ($requestErrors[0]['reason'] == 'dailyLimitExceeded') {
                        $worker->getConnection()->set($analyticsBlockKey, $e->getMessage(), 600, true, array('anayltics_block', 'analytics_block_'.$params['profileId']));
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    } else {
                        throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                    }
                } else {
                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                }
            } catch (\Exception $e) {
                throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
            }
        };

        $return = array(
            'totalResults' => $data['totalResults'],
            'totalsForAllResults' => $data['totalsForAllResults'],
            'containsSampledData' => $data['containsSampledData'],
        );

        $results = array();
        foreach ($data['rows'] as $k => $row) {
            $results[$k] = array();
            foreach ($data['columnHeaders'] as $ck => $header) {
                $results[$k][$header['name']] = $row[$ck];
            }
        }

        $return['results'] = $results;

        return $results;
    }

    private function getToken($refresh_token, $client, $connection, $logger)
    {
        $cachedTokenData = $connection->get($this->tokenCacheKey);
        if (!$cachedTokenData || !$cachedTokenData['is_fresh']) {
            $lockKey = $connection->obtainLock($this->tokenCacheKey, 5);
            if ($lockKey) {
                $cachedTokenData = $connection->get($this->tokenCacheKey);
                if (!$cachedTokenData || !$cachedTokenData['is_fresh']) {
                    try {
                        if ($logger) {
                            $logger->logDebug('Analytics: refreshing access token');
                        }

                        $tries = 3;
                        while (true) {
                            try {
                                $client->refreshToken($refresh_token);
                                break;
                            } catch (\Exception $e) {
                                if (!--$tries) {
                                    throw new Exception('Api-Error:' . $e->getMessage(), $e->getCode(), $e);
                                }
                                usleep(50000);
                            }
                        }

                        $tmpToken = $client->getAccessToken();
                        $tmp = json_decode($tmpToken, true);
                        $tmp['refresh_token'] = $refresh_token;
                        $token = json_encode($tmp);
                        $connection->set($this->tokenCacheKey, $token, $tmp['created'] + $tmp['expires_in'] - 60 - time(), true);
                        $connection->releaseLock($this->tokenCacheKey, $lockKey);
                        return $token;
                    } catch (\Exception $e) {
                        $connection->releaseLock($this->tokenCacheKey, $lockKey);
                        throw $e;
                    }
                }
                $connection->releaseLock($this->tokenCacheKey, $lockKey);
            } else {
                throw new Exception('could not generate new access token');
            }
        }

        return $cachedTokenData['data'];
    }

}
