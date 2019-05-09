package org.voting.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class PostCleanUpUtility {
    private static final Logger logger = LoggerFactory.getLogger(PostCleanUpUtility.class.getName());

    private final ExecutorService executorService;

    public PostCleanUpUtility() {
        executorService = Executors.newFixedThreadPool(1);
    }

    public void cleanUp() {
        while(true) {
            executorService.submit(() -> {
                //todo: delete all posts which have expired until 10 mins in past
            });

            try {
                TimeUnit.MINUTES.sleep(10);
            } catch (InterruptedException e) {
                logger.warn("Post cleaning thread got interrupted during sleep.");
            }
        }
    }
}
