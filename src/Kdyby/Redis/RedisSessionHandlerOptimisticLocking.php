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



	private function mergeData($currentData, $newData)
	{
		$unserializedCurrentData = unserialize($currentData);
		$unserializedNewData = unserialize($newData);

		if (is_array($unserializedCurrentData) && is_array($unserializedNewData)) {
			$mergedData = serialize(array_merge($unserializedCurrentData, $unserializedNewData));

		} elseif (is_object($unserializedCurrentData) && is_object($unserializedNewData)) {
			$mergedData = serialize((object) array_merge((array) $unserializedCurrentData, (array) $unserializedNewData));

		} elseif (is_numeric($unserializedCurrentData) && is_numeric($unserializedNewData)) {
			$mergedData = serialize($unserializedNewData > $unserializedCurrentData ? $unserializedNewData : $unserializedCurrentData);

		} elseif ($unserializedCurrentData instanceof \DateTime && $unserializedNewData instanceof \DateTime) {
			$mergedData = serialize($unserializedNewData > $unserializedCurrentData ? $unserializedNewData : $unserializedCurrentData);

		} else {
			$mergedData = $unserializedNewData;
		}

		return $mergedData;
	}

}
