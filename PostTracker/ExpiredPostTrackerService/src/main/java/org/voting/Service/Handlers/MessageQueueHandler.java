package org.voting.Service.Handlers;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
public class MessageQueueHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageQueueHandler.class.getName());

    public static final String QUEUE_NAME = "PostQueue";
    private final AmazonSQS sqs;
    private final String queueUrl;

    public MessageQueueHandler() {
        sqs = AmazonSQSClientBuilder.defaultClient();
        queueUrl = createQueue();
    }

    private String createQueue() {
        CreateQueueRequest create_request = new CreateQueueRequest(QUEUE_NAME)
                .addAttributesEntry("DelaySeconds", "60")
                .addAttributesEntry("MessageRetentionPeriod", "86400");
        try {
            return sqs.createQueue(create_request).getQueueUrl();
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return null;
    }

    public void sendMessageAndUpdatePostStatusInDB(String postId, String emailId, String winner) {
        final SendMessageRequest sendMessageRequest = new SendMessageRequest();
        sendMessageRequest.withMessageBody("PostId=" + postId + ",EmailId=" + emailId + ",Option=" + winner);
        sendMessageRequest.withQueueUrl(queueUrl);
        try {
            sqs.sendMessage(sendMessageRequest);
            DatabaseReader.getInstance().markPostAsDone(postId);
        } catch (final AmazonClientException ace) {
            logger.error(ace.getMessage());
        }
    }
}