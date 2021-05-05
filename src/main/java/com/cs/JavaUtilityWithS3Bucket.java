package com.cs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.cs.service.AWSS3BucketService;
import com.cs.service.Constants;
import com.cs.service.UserInput;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JavaUtilityWithS3Bucket {

    private static UserInput getFileData(String file) {
        Map<String, String> map = new HashMap<>();
        UserInput input = new UserInput();
        try {
            Path path = Paths.get(file);
            List<String> lines = Files.readAllLines(path);
            if (lines.size() == 0) {
                System.out.println("File at [ " + file + " ] must not be empty ");
            } else {
                lines.forEach(s -> {
                    String[] parts = s.split(":");
                    String name = parts[0].trim();
                    String number = parts[1].trim();
                    if (!name.equals("") && !number.equals(""))
                        map.put(name, number);
                });
                map.forEach((k, v) -> {
                    System.out.println((k + ":" + v));
                    if (k.equalsIgnoreCase(Constants.BUCKET_NAME)) {
                        input.setBucketName(v);
                    }
                    if (k.equalsIgnoreCase(Constants.ACCESS_KEY)) {
                        input.setAccessKey(v);
                    }
                    if (k.equalsIgnoreCase(Constants.SECRETE_KEY)) {
                        input.setSecreteKey(v);
                    }
                    if (k.equalsIgnoreCase(Constants.REGION)) {
                        input.setRegion(v);
                    }
                });
            }
        } catch (IOException e) {
            System.out.println("File name found in resources folder \n OR You have not added file path correctly in program arguments : " + file);
        }
        return input;
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
        UserInput input = getFileData(filePath);
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
}
