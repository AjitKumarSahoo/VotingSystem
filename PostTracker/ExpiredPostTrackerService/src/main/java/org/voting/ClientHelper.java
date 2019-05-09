package org.voting;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class ClientHelper {

    private static final String POST_TABLE_NAME = "PostInfo";
    private static AmazonDynamoDB dynamoDBClient;
    private static DynamoDB dynamoDB;

    public ClientHelper() {
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build();
        dynamoDB = new DynamoDB(dynamoDBClient);
    }

    /**
     * Keep on describing the table until it is found. Voting service is responsible to create PostInfo table
     */
    public void makeSurePostTableExists() {
        while (true) {
            try {
                dynamoDBClient.describeTable(new DescribeTableRequest().withTableName(POST_TABLE_NAME));
                break;
            } catch (ResourceNotFoundException ignored) {
                try {
                    TimeUnit.SECONDS.sleep(10); // try after 10 seconds
                } catch (InterruptedException ignored1) {
                }
            }
        }
    }

    public Table getPostTable() {
        return dynamoDB.getTable(POST_TABLE_NAME);
    }
}
