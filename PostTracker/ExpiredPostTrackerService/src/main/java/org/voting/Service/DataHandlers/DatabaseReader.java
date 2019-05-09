package org.voting.Service.DataHandlers;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voting.ClientHelper;
import org.voting.PostData;
import org.voting.PostStatus;
import org.voting.DateUtility;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
public class DatabaseReader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseReader.class.getName());

    private static final int EXPIRY_INTERVAL_IN_MIN = 2 * 60 * 1000;
    static final String POST_ID_KEY = "PostId";
    static final String POST_STATUS = "Status";
    static final String POST_END_DATE = "EndDate";
    private static final String POST_OWNER_EMAIL_ID = "OwnerEmailId";
    private static final String POST_USER_2_OPTION_MAP = "User2OptionMap";

    private Table table;

    public DatabaseReader() {
        table = new ClientHelper().getPostTable();
    }

    /**
     * Returns list of posts which has expired in last 2 min and status is EXPIRED
     * @return list of expired posts
     */
    public List<PostData> getExpiredPosts() {
        String startTime = DateUtility.getTimeBeforeNMinutes(EXPIRY_INTERVAL_IN_MIN);
        String endTime = DateUtility.getCurrentTimeWithMinutePrecision();

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
}
