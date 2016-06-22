<?php

namespace Kdyby\Redis;



class RedisSessionHandlerOptimisticLocking extends RedisSessionHandler
{

	/**
	 * @var array
	 */
	private $watchedKeys = [];



	public function read($id)
	{
		$key = $this->formatKey($id);

		if (!isset($this->watchedKeys[$key])) {
			$this->client->watch($key);
			$this->watchedKeys[$key] = $key;
		}

		return (string) $this->client->get($key);
	}



	public function write($id, $data)
	{
		$key = $this->formatKey($id);
		if (!isset($this->watchedKeys[$key])) {
			$this->client->set($key, $data);

			return TRUE;
		}

		return $this->tryWrite($id, $data);
	}



	public function destroy($id)
	{
		if (!isset($this->watchedKeys[$id])) {
			return FALSE;
		}

		$key = $this->formatKey($id);
		try {
			$this->client->multi(function (RedisClient $client) use ($key) {
				$client->del($key);

				unset($this->watchedKeys[$key]);
			});

		} catch (RedisClientException $e) {
			return $this->destroy($id);
		}

		return TRUE;
	}



	public function close()
	{
		$this->client->unwatch();
		$this->watchedKeys = [];

		return TRUE;
	}



	/**
	 * Try write to session recursively with 10 attempts
	 *
	 * @param $id
	 * @param $data
	 * @param int $attempts
	 * @return bool
	 * @throws \Exception
	 * @throws \Throwable
	 */
	private function tryWrite($id, $data, $attempts = 10)
	{
		if ($attempts <= 0) {
			return FALSE;
		}

		$key = $this->formatKey($id);
		if ($attempts < 10) {
			$currentData = $this->read($key);
			$data = $this->mergeData($currentData, $data);
		}

		try {
			$this->client->multi(function (RedisClient $client) use ($id, $data, $key) {
				$client->set($key, $data);
			});

		} catch (RedisClientException $e) {
			unset($this->watchedKeys[$this->formatKey($id)]);

			return $this->tryWrite($id, $data, $attempts - 1);
		}

		return TRUE;
	}



	/**
	 * Merging sessions data
	 *
	 * @param $currentData
	 * @param $newData
	 * @return string
	 */
	private function mergeData($currentData, $newData)
	{
		if (is_array($currentData) && is_array($newData)) {
			$mergedData = array_merge($currentData, $newData);

		} elseif (is_object($currentData) && is_object($newData)) {
			$mergedData = (object) array_merge((array) $currentData, (array) $newData);

		} elseif (is_numeric($currentData) && is_numeric($newData)) {
			$mergedData = $newData > $currentData ? $newData : $currentData;

		} elseif ($currentData instanceof \DateTime && $newData instanceof \DateTime) {
			$mergedData = $newData > $currentData ? $newData : $currentData;

		} else {
			$mergedData = $newData;
		}

		return $mergedData;
	}

}
