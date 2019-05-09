package org.voting.Service;

import com.amazonaws.services.sqs.model.Message;
import org.voting.Service.DataHandlers.DatabaseReader;
import org.voting.Service.DataHandlers.EmailHandler;
import org.voting.Service.DataHandlers.MessageQueueHandler;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class MessageProcessor {

    private static final char KEY_VALUE_SEPARATOR = ':';
    private static final String ATTR_SEPARATOR = ",";
    private final ExecutorService executorService;
    private final EmailHandler emailHandler;

    public MessageProcessor() {
        executorService = Executors.newCachedThreadPool();
        emailHandler = new EmailHandler();
    }

    /**
     * Reads message/s from PostQueue
     * Submits a task for each message
     * Each thread -
     *      parses the message,
     *      sends winner email to owner,
     *      deletes message from queue,
     *      updates post's status to DONE in database
     */
    public void processMessage() {
        while (true) {
            List<Message> messages = MessageQueueHandler.getInstance().readMessage();
            for (final Message msg : messages) {
                executorService.submit(() -> {
                    String[] token = msg.getBody().split(ATTR_SEPARATOR);
                    String postId = token[0].substring(KEY_VALUE_SEPARATOR);
                    String emailId = token[1].substring(KEY_VALUE_SEPARATOR);
                    String option = token[2].substring(KEY_VALUE_SEPARATOR);

                    emailHandler.sendEmail(postId, emailId, option);
                    MessageQueueHandler.getInstance().deleteMessage(msg);
                    DatabaseReader.getInstance().markPostAsDone(postId);
                });
            }
        }
    }
}
