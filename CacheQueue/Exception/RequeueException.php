<?php
namespace CacheQueue\Exception;

class RequeueException extends Exception
{
    protected $delay = null;
    protected $freshFor = null;

    public function getDelay()
    {
        return $this->delay;
    }

    /**
     *
     * @param int|null $delay the time in seconds to bury the job for,
     *                          null to bury until the jobs queueFreshUntil date
     */
    public function setDelay($delay = null)
    {
        $this->delay = $delay;
    }


    public function getFreshFor()
    {
        return $this->freshFor;
    }

    /**
     *
     * @param int|null $freshFor the time in seconds the requeued job will be fresh for (in addition to the delay)
     *                           null to use the jobs queueFreshUntil date
     */
    public function setFreshFor($freshFor)
    {
        $this->freshFor = $freshFor;
    }






}
