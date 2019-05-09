package org.voting.Service.DataHandlers;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voting.ClientHelper;
import org.voting.DateUtility;

import java.util.concurrent.TimeUnit;

import static org.voting.Service.DataHandlers.DatabaseReader.POST_END_DATE;
import static org.voting.Service.DataHandlers.DatabaseReader.POST_ID_KEY;
import static org.voting.Service.DataHandlers.DatabaseReader.POST_STATUS;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class PostCleanUpUtility {
    private static final Logger logger = LoggerFactory.getLogger(PostCleanUpUtility.class.getName());
    private static final int EXPIRY_INTERVAL_IN_MIN = 10;
    private final ClientHelper clientHelper;

    public PostCleanUpUtility() {
        clientHelper = new ClientHelper();
    }

    /**
     * Fetch expired posts (expired 10 minutes back) with status DONE and delete them
     */
    public void cleanUp() {
        while (true) {
            String startTime = DateUtility.getTimeBeforeNMinutes(EXPIRY_INTERVAL_IN_MIN);

            String projExpr = POST_ID_KEY + ", " + POST_END_DATE + ", " + POST_STATUS;
            ScanSpec scanSpec = new ScanSpec()
                    .withProjectionExpression(projExpr)
                    .withFilterExpression("#ed <= :tm AND #st = :v")
                    .withNameMap(new NameMap().with("#ed", "EndDate").with("#st", POST_STATUS))
                    .withValueMap(new ValueMap().withString(":tm", startTime).withString(":v", "DONE"));
            try {
                ItemCollection<ScanOutcome> items = clientHelper.getPostTable().scan(scanSpec);
                for (Item item : items) {
                    clientHelper.getPostTable().deleteItem(POST_ID_KEY, item.getString(POST_ID_KEY));
                    logger.info("Successfully deleted post " + item.toJSONPretty());
                }
            } catch (Exception e) {
                logger.error("Unable to scan the table:");
                logger.error(e.getMessage());
            }

            try {
                TimeUnit.MINUTES.sleep(9); // better to have a little bit of overlap
            } catch (InterruptedException e) {
                logger.warn("Post cleaning thread got interrupted during sleep.");
            }
        }
    }
}
