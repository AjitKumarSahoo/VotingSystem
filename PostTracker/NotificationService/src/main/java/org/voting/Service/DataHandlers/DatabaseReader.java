package org.voting.Service.DataHandlers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
public class DatabaseReader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseReader.class.getName());

    private static final String POST_TABLE_NAME = "PostInfo";
    private static final String POST_ID_KEY = "PostId";
    private static final String POST_STATUS = "Status";

    private final DynamoDB dynamoDB;
    private volatile static DatabaseReader instance;

    private DatabaseReader() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build();
        dynamoDB = new DynamoDB(client);
        makeSurePostTableExists(client);
    }

    public static DatabaseReader getInstance() {
        if (instance == null) {
            synchronized (DatabaseReader.class) {
                if (instance == null) {
                    instance = new DatabaseReader();
                }
            }
        }
        return instance;
    }

    /**
     * Keep on describing the table until it is found. Voting service is responsible to create PostInfo table
     * @param client - Amazon client
     */
    private void makeSurePostTableExists(AmazonDynamoDB client) {
        while (true) {
            try {
                client.describeTable(new DescribeTableRequest().withTableName(POST_TABLE_NAME));
                break;
            } catch (ResourceNotFoundException ignored) {
                try {
                    TimeUnit.SECONDS.sleep(5); // try after 5 seconds
                } catch (InterruptedException ignored1) {
                }
            }
        }
    }

    /**
     * Mark post as done (post has expired, winner is calculated and communicated to owner)
     * @param postId - post key
     */
    public void markPostAsDone(String postId) {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(POST_ID_KEY, postId)
                .withUpdateExpression("set #s=:val")
                .withNameMap(new NameMap().with("#s", POST_STATUS))
                .withValueMap(new ValueMap().withString(":val", "DONE"))
                .withReturnValues(ReturnValue.ALL_NEW);
        try {
            Table table = dynamoDB.getTable(POST_TABLE_NAME);
            table.updateItem(updateItemSpec);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
