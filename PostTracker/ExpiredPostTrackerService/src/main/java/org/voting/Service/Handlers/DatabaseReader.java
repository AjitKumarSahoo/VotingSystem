package org.voting.Service.Handlers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voting.PostData;
import org.voting.PostStatus;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
public class DatabaseReader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseReader.class.getName());

    private static final int EXPIRY_INTERVAL_IN_MIN = 2 * 60 * 1000;
    private static final String POST_TABLE_NAME = "PostInfo";
    private static final String POST_ID_KEY = "PostId";
    private static final String POST_STATUS = "Status";
    private static final String POST_END_DATE = "EndDate";
    private static final String POST_OWNER_EMAIL_ID = "OwnerEmailId";
    private static final String POST_USER_2_OPTION_MAP = "User2OptionMap";
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss");

    private Table table;
    private final DynamoDB dynamoDB;
    private volatile static DatabaseReader instance;

    private DatabaseReader() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build();
        dynamoDB = new DynamoDB(client);
        table = getTable(client);
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
     * @return PostInfo table
     */
    private Table getTable(AmazonDynamoDB client) {
        while (true) {
            try {
                client.describeTable(new DescribeTableRequest().withTableName(POST_TABLE_NAME));
                break;
            } catch (ResourceNotFoundException ignored) {
                try {
                    TimeUnit.SECONDS.sleep(10); // try after 10 seconds
                } catch (InterruptedException ignored1) {
                }
            }
        }
        return dynamoDB.getTable(POST_TABLE_NAME);
    }

    /**
     * Returns list of posts which has expired in last 2 min and status is EXPIRED
     * @return list of expired posts
     */
    public List<PostData> getExpiredPosts() {
        String startTime = getPastTimeBeforeNMinutes(EXPIRY_INTERVAL_IN_MIN);
        String endTime = getCurrentTimeWithMinutePrecision();

        String projExpr = POST_ID_KEY + ", " + POST_OWNER_EMAIL_ID + ", " +
                POST_USER_2_OPTION_MAP + ", " + POST_END_DATE + ", "  + POST_STATUS;

        ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression(projExpr)
                .withFilterExpression("#ed between :start_tm and :end_tm AND #st = :v")
                .withNameMap(new NameMap().with("#ed", "EndDate").with("#st", POST_STATUS))
                .withValueMap(new ValueMap().withString(":start_tm", startTime)
                        .withString(":end_tm", endTime).withString(":v", PostStatus.EXPIRED.toString()));
        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);
            return buildPostData(items);
        } catch (Exception e) {
            logger.error("Unable to scan the table:");
            System.err.println(e.getMessage());
        }

        return new ArrayList<>(0);
    }

    private List<PostData> buildPostData(ItemCollection<ScanOutcome> items) {
        List<PostData> posts = new ArrayList<>();
        for (Item item : items) {
            PostData post = new PostData();
            post.setPostId(item.getString(POST_ID_KEY));
            post.setOwnerEmailId(item.getString(POST_OWNER_EMAIL_ID));
            post.setUser2optionMap(item.getMap(POST_USER_2_OPTION_MAP));
            posts.add(post);
        }
        return posts;
    }

    private String getCurrentTimeWithMinutePrecision() {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.SECOND, 0);
        return dateFormatter.format(now.getTime());
    }

    private String getPastTimeBeforeNMinutes(int minutesBefore) {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.SECOND, 0);
        now.add(Calendar.MINUTE, -minutesBefore);
        return dateFormatter.format(now.getTime());
    }

    /**
     * Mark post as done (post has expired, winner is calculated and communicated to owner)
     * @param postId - post key
     */
    public void markPostAsDone(String postId) {
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(POST_ID_KEY, postId)
                .withUpdateExpression("set #s=:val")
                .withNameMap(new NameMap().with("#s", POST_STATUS))
                .withValueMap(new ValueMap().withString(":val", PostStatus.DONE.toString()))
                .withReturnValues(ReturnValue.ALL_NEW);
        try {
            table.updateItem(updateItemSpec);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
