<?php
namespace CacheQueue\Exception;

class BuryException extends Exception
{
    protected $buryTime = null;

    function getBuryTime()
    {
        return $this->buryTime;
    }

    /**
     *
     * @param int|null $buryTime the time in seconds to bury the job for,
     *                          null to bury until the jobs queueFreshUntil date
     */
    function setBuryTime($buryTime = null)
    {
        $this->buryTime = $buryTime;
    }


}
