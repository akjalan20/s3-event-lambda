package org.example;


import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Hello world!
 *
 */
public class S3EventLambda implements RequestHandler<S3Event, Boolean>
{

    private static final AmazonS3 s3Client = AmazonS3Client.builder().withCredentials(new DefaultAWSCredentialsProviderChain()).build();
    @Override
    public Boolean handleRequest(S3Event input, Context context) {
        LambdaLogger logger = context.getLogger();
        if(input.getRecords().isEmpty()){
            logger.log("No records found");
            return false;
        }

        for(S3EventNotification.S3EventNotificationRecord records : input.getRecords()){
            String bucketName = records.getS3().getBucket().getName();
            String key = records.getS3().getObject().getKey();

            //1. Create S3 client
            //2. Invoke GetObject
            S3Object s3Object = s3Client.getObject(bucketName, key);
            S3ObjectInputStream objectContent = s3Object.getObjectContent();

            //3. Process CSV from S3
            try(final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(objectContent, StandardCharsets.UTF_8))){
                bufferedReader.lines().skip(1)
                        .forEach(line -> logger.log(line + "\n"));
            } catch (IOException e) {
                logger.log("exception occurred while reading file: " + e.getMessage());
                throw new RuntimeException(e);
            }

        }
        return true;
    }
}
