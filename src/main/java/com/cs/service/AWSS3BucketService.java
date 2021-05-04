package com.cs.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.util.List;

public class AWSS3BucketService {
    private final AmazonS3 s3client;

    public AWSS3BucketService() {
        this(new AmazonS3Client() {
        });
    }

    public AWSS3BucketService(AmazonS3 s3client) {
        this.s3client = s3client;
    }

    //is bucket exist?
    public boolean doesBucketExist(String BUCKET_NAME) {
        return s3client.doesBucketExist(BUCKET_NAME);
    }

    //create a bucket
    public Bucket createBucket(String BUCKET_NAME) {
        return s3client.createBucket(BUCKET_NAME);
    }

    //list all buckets
    public List<Bucket> listBuckets() {
        return s3client.listBuckets();
    }

    //delete a bucket
    public void deleteBucket(String BUCKET_NAME) {
        s3client.deleteBucket(BUCKET_NAME);
    }

    //uploading object
    public PutObjectResult putObject(String BUCKET_NAME, String key, File file) {
        return s3client.putObject(BUCKET_NAME, key, file);
    }

    //listing objects
    public ObjectListing listObjects(String BUCKET_NAME) {
        return s3client.listObjects(BUCKET_NAME);
    }

    //get an object
    public S3Object getObject(String BUCKET_NAME, String objectKey) {
        return s3client.getObject(BUCKET_NAME, objectKey);
    }

    //copying an object
    public CopyObjectResult copyObject(
            String sourceBUCKET_NAME,
            String sourceKey,
            String destinationBUCKET_NAME,
            String destinationKey
    ) {
        return s3client.copyObject(
                sourceBUCKET_NAME,
                sourceKey,
                destinationBUCKET_NAME,
                destinationKey
        );
    }

    //deleting an object
    public void deleteObject(String BUCKET_NAME, String objectKey) {
        s3client.deleteObject(BUCKET_NAME, objectKey);
    }

    //deleting multiple Objects
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest delObjReq) {
        return s3client.deleteObjects(delObjReq);
    }
}