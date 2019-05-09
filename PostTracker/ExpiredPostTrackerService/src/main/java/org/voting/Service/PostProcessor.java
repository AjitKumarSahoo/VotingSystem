package org.voting.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voting.Service.DataHandlers.DatabaseReader;
import org.voting.Service.DataHandlers.MessageQueueHandler;
import org.voting.PostData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class PostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(PostProcessor.class.getName());

    private static final int POST_READ_INTERVAL_IN_MIN = 1;

    private final ExecutorService executorService;
    private final MessageQueueHandler queueHandler;
    private final DatabaseReader databaseReader;

    public PostProcessor() {
        executorService = Executors.newFixedThreadPool(100); // need to configure based upon system usage
        queueHandler = new MessageQueueHandler();
        databaseReader = new DatabaseReader();
    }

    public void processPost() {
        while (true) {
            try {
                for (PostData post : databaseReader.getExpiredPosts()) {
                    executorService.submit(new WinnerDecider(post));
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            try {
                TimeUnit.MINUTES.sleep(POST_READ_INTERVAL_IN_MIN);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    /**
     * Task to find winner and add message to message queue
     */
    private class WinnerDecider implements Runnable {
        private final PostData post;

        WinnerDecider(PostData post) {
            this.post = post;
        }

        public void run() {
            Map<String, String> user2optionMap = post.getUser2optionMap();
            Map<String, Integer> option2CountMap = getOption2CountMap(user2optionMap);
            String winner = getWinner(option2CountMap);
            queueHandler.sendMessageAndUpdatePostStatusInDB(post.getPostId(), post.getOwnerEmailId(), winner);
        }

        private String getWinner(Map<String, Integer> option2CountMap) {
            String winner = "";
            int max = 0;
            for (String option : option2CountMap.keySet()) {
                if (option2CountMap.get(option) > max) {
                    max = option2CountMap.get(option);
                    winner = option;
                }
            }
            return winner;
        }

        private Map<String, Integer> getOption2CountMap(Map<String, String> user2optionMap) {
            Map<String, Integer> option2CountMap = new HashMap<>();
            for (String user : user2optionMap.keySet()) {
                String option = user2optionMap.get(user);
                if (option2CountMap.containsKey(option)) {
                    option2CountMap.put(option, option2CountMap.get(option) + 1);
                } else {
                    option2CountMap.put(option, 1);
                }
            }
            return option2CountMap;
        }
    }
}
