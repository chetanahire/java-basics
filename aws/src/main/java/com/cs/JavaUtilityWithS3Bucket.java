package com.cs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cs.service.AWSS3BucketService;
import com.cs.service.Constants;
import com.cs.service.UserInput;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class JavaUtilityWithS3Bucket {

    private UserInput getFileData(String file) {
        UserInput input = new UserInput();
        try {
            Path path = Paths.get(file);
            List<String> lines = Files.readAllLines(path);
            if (lines.size() == 0) {
                System.out.println("File at [ " + file + " ] must not be empty ");
            } else {
                lines.forEach(s -> {
                    String[] keyValuePair = s.split(":");
                    String key = keyValuePair[0].trim();
                    String value = keyValuePair[1].trim();
                    if (!key.equals("") && !value.equals("")) {
                        if (key.equalsIgnoreCase(Constants.BUCKET_NAME)) {
                            input.setBucketName(value);
                        }
                        if (key.equalsIgnoreCase(Constants.ACCESS_KEY)) {
                            input.setAccessKey(value);
                        }
                        if (key.equalsIgnoreCase(Constants.SECRETE_KEY)) {
                            input.setSecreteKey(value);
                        }
                        if (key.equalsIgnoreCase(Constants.REGION)) {
                            input.setRegion(value);
                        }
                    }
                });
            }
        } catch (IOException e) {
            System.out.println(
                    "File not found in resources folder " +
                            "\nOR\n" +
                            "You have not added file path correctly in program arguments : " + file
            );
        }
        return input;
    }

    private void s3BucketActions(UserInput input) {
        AWSCredentials credentials = new BasicAWSCredentials(
                input.getAccessKey(),
                input.getSecreteKey()
        );
        //set-up the client
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(input.getRegion())
                .build();

        AWSS3BucketService awsService = new AWSS3BucketService(s3client);

        //list all the buckets
        for (Bucket s : awsService.listBuckets()) {
            System.out.println(s.getName());
        }
        //listing objects
        ObjectListing objectListing = awsService.listObjects(input.getBucketName());
        for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
            System.out.println(os.getKey());
        }
    }

    public static void main(String[] args) {
        //  get user inputs from text file
        String filePath = null;
        if (args.length == 0) {
            ClassLoader classLoader = JavaUtilityWithS3Bucket.class.getClassLoader();
            URL url = classLoader.getResource("user_inputs.txt");
            if (url != null) {
                File file = new File(url.getFile());
                filePath = file.getPath();
            }
        } else {
            filePath = args[0];
        }
        JavaUtilityWithS3Bucket javaUtilityWithS3Bucket = new JavaUtilityWithS3Bucket();
        javaUtilityWithS3Bucket.s3BucketActions(javaUtilityWithS3Bucket.getFileData(filePath));
    }
}
