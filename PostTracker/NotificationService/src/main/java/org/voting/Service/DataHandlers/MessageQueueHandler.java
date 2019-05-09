package org.voting.Service.DataHandlers;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
public class MessageQueueHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageQueueHandler.class.getName());

    private static final String QUEUE_NAME = "PostQueue";
    private volatile static MessageQueueHandler instance;

    private final AmazonSQS sqs;
    private final String queueUrl;

    private MessageQueueHandler() {
        sqs = AmazonSQSClientBuilder.defaultClient();
        queueUrl = getQueueURL();
    }

    public static MessageQueueHandler getInstance() {
        if (instance == null) {
            synchronized (MessageQueueHandler.class) {
                if (instance == null) {
                    instance = new MessageQueueHandler();
                }
            }
        }
        return instance;
    }

    private String getQueueURL() {
        while (true) { // stay in this loop until message queue is created by Post Tracker service
            try {
                return sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
            } catch (QueueDoesNotExistException ignored) {
                try {
                    TimeUnit.SECONDS.sleep(10); // try after 10 seconds
                } catch (InterruptedException ignored1) {
                }
            }
        }
    }

    /**
     * Read messages from PostQueue
     * @return - list of messages read from queue
     */
    public List<Message> readMessage() {
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest.setMaxNumberOfMessages(10);
        try {
            return sqs.receiveMessage(receiveMessageRequest).getMessages();
        } catch (Exception e) {
            logger.error("Error while reading message from queue: " + queueUrl);
            logger.error(e.getMessage());
        }
        return new ArrayList<>(0);
    }

    /**
     * Once read off the queue, delete it from the queue.
     * @param msg - message to be deleted
     */
    public void deleteMessage(Message msg) {
        try {
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, msg.getReceiptHandle()));
        } catch (Exception e) {
            logger.error("Failed to delete message from queue.");
            logger.error(e.getMessage());
        }
    }
}