<?php

namespace Gaufrette\Adapter;

use \Google_Service_Storage as GStorageService;
use Gaufrette\Adapter;

/**
 * Google Cloud Storage adapter.
 *
 * @package Gaufrette
 * @author  Saidul Islam <saidul.04@gmail.com>
 */
class GCloudStorage implements Adapter,MetadataSupporter
{
    protected $service;
    protected $bucket;
    protected $ensureBucket = false;
    protected $metadata;
    protected $options;

    private $lastKey = '';
    private $lastObj = '';
    private $acl;

    public function __construct(GStorageService $service, $bucket, $options = array())
    {
        $this->service = $service;
        $this->bucket  = $bucket;
        $this->options = array_replace_recursive(
            array('directory' => null, 'create' => false),
            $options
        );

        $this->acl = new \Google_Service_Storage_ObjectAccessControl();
        $this->acl->setEntity("allUsers");
        $this->acl->setRole("READER");
    }

    /**
     * {@inheritDoc}
     */
    public function setMetadata($key, $metadata)
    {
        $path = $this->computePath($key);

        $this->metadata[$path] = $metadata;
    }

    /**
     * {@inheritDoc}
     */
    public function getMetadata($key)
    {
        $path = $this->computePath($key);

        return isset($this->metadata[$path]) ? $this->metadata[$path] : array();
    }

    /**
     * {@inheritDoc}
     */
    public function read($key)
    {
        $this->ensureBucketExists();

        $key = $this->computePath($key);

        $url = "https://storage.googleapis.com/{$this->bucket}/$key";
        $request = new \Google_Http_Request($url, 'GET');
        $this->service->getClient()->getAuth()->sign($request);
        $response = $this->service->getClient()->getIo()->executeRequest($request);

        return $response[2] == 200 ? $response[0] : false;
    }

    /**
     * {@inheritDoc}
     */
    public function rename($sourceKey, $targetKey)
    {
        $sourceKey = $this->computePath($sourceKey);
        $targetKey = $this->computePath($targetKey);

        $this->ensureBucketExists();
        //TODO: test
        $obj = $this->getObjectData($sourceKey);
        if($obj === false) return false;

        try {
            $obj = $this->service->objects->copy($this->bucket, $sourceKey, $this->bucket, $targetKey, $obj);
            $this->delete($sourceKey);
        } catch (\Google_Service_Exception $ex) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function write($key, $content)
    {
        $this->ensureBucketExists();
        $key = $this->computePath($key);
        try {
            $obj = new \Google_Service_Storage_StorageObject();
            $obj->name = $key;
            if(isset($this->options['cache-control'])) {
                $obj->setCacheControl($this->options['cache-control']);
                $obj->setAcl('project-private');
            }
            $obj->setMetadata($this->getMetadata($key));

            $obj = $this->service->objects->insert($this->bucket, $obj, array(
                    'uploadType' => 'multipart',
                    'data' => $content
                ));
            $this->service->objectAccessControls->insert($this->bucket, $key, $this->acl);

            return $obj->getSize();
        } catch (\Google_Service_Exception $ex) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function exists($key)
    {
        $key = $this->computePath($key);
        $this->ensureBucketExists();
        try {
            $obj = $this->service->objects->get($this->bucket, $key);
        } catch (\Google_Service_Exception $ex) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function mtime($key)
    {
        $key = $this->computePath($key);
        $this->ensureBucketExists();

        $obj = $this->getObjectData($key);

        return $obj ? strtotime($obj->getUpdated()) : false;
    }

    /**
     * {@inheritDoc}
     */
    public function keys()
    {
        $this->ensureBucketExists();

        //TODO: improve
        $list = $this->service->objects->listObjects($this->bucket);

        $keys = array();
        foreach ($list as $obj) {
            /** @var \Google_Service_Storage_StorageObject $obj */
            $file = $obj->name;
            if ('.' !== dirname($file)) {
                $keys[] = dirname($file);
            }
            $keys[] = $file;
        }
        sort($keys);

        return $keys;
    }

    /**
     * {@inheritDoc}
     */
    public function delete($key)
    {
        $this->ensureBucketExists();
        $key = $this->computePath($key);
        try {
            $this->lastKey = '';
            $this->lastObj = false;
            $this->service->objects->delete($this->bucket, $key);
        } catch (\Google_Service_Exception $ex) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function isDirectory($key)
    {
        $key = $this->computePath($key);
        if ($this->exists($key.'/')) {
            return true;
        }

        return false;
    }

    /**
     * Ensures the specified bucket exists. If the bucket does not exists
     * and the create parameter is set to true, it will try to create the
     * bucket
     *
     * @throws \RuntimeException if the bucket does not exists or could not be
     *                          created
     */
    private function ensureBucketExists()
    {
        if ($this->ensureBucket) {
            return;
        }

        try{
            $bucket = $this->service->buckets->get($this->bucket);
            $this->ensureBucket = true;
        }catch (\Google_Service_Exception $ex) {
            $this->ensureBucket = false;
        }

        if($this->ensureBucket) {
            return;
        }

        if (!$this->options['create']) {
            throw new \RuntimeException(sprintf(
                'The configured bucket "%s" does not exist.',
                $this->bucket
            ));
        }

        try{
            //TODO: create bucket
            throw new \Exception("Please implement bucket creation");
            //$response = $this->service->buckets->insert();
        }catch (\Google_Service_Exception $ex) {
            throw new \RuntimeException(sprintf(
                'Failed to create the configured bucket "%s".',
                $this->bucket
            ));
        }

        $this->ensureBucket = true;
    }

    /**
     * Computes the path for the specified key taking the bucket in account
     *
     * @param string $key The key for which to compute the path
     *
     * @return string
     */
    private function computePath($key)
    {
        $directory = $this->options['directory'];
        if (null === $directory || '' === $directory) {
            return $key;
        }

        return sprintf('%s/%s', trim($directory, '/'), trim($key, '/'));
    }

    /**
     * @param $key
     * @param array $options
     * @return bool|\Google_Service_Storage_StorageObject
     */
    private function getObjectData($key, $options = array())
    {
        try {
            if($this->lastKey != $key) {
                $this->lastObj = $this->service->objects->get($this->bucket, $key, $options);
                $this->lastKey = $key;
            }
            return $this->lastObj;
        } catch (\Google_Service_Exception $ex) {
        }

        return false;
    }
}
