package org.voting.Service.DataHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class EmailHandler {
    private static final Logger logger = LoggerFactory.getLogger(EmailHandler.class.getName());
    private static final String sender = "voting-system@do-not-reply.com";
    private static final String host = "127.0.0.1";
    private static final int MAX_EMAIL_SENDING_RETRY_COUNT = 3;

    public EmailHandler(){}

    /**
     * Tries sending email on given email id. No. of retries is 3 if sending fails.
     * @param postId - post id for which email is being sent
     * @param emailId - post owner's email id
     * @param option - winner option
     */
    public void sendEmail(String postId, String emailId, String option) {
        int retryCount = MAX_EMAIL_SENDING_RETRY_COUNT;
        while (retryCount != 0) {
            try
            {
                Properties properties = System.getProperties();
                properties.setProperty("mail.smtp.host", host);
                Session session = Session.getDefaultInstance(properties);
                MimeMessage message = new MimeMessage(session);
                message.setFrom(new InternetAddress(sender));
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailId));
                message.setSubject("Winner for post: " + postId);
                message.setText("Hello " + emailId.substring('@') + ",\n\n" +
                        "Your post " + postId + " has expired. And the winner is " + option + " !!");

                Transport.send(message);
                logger.info("Successfully sent email to " + emailId + " for post: " + postId);
                break;
            } catch (MessagingException e) {
                if (--retryCount == 0) {
                    logger.error("Failed to send email to " + emailId + " for post " + postId +
                                 " with " + MAX_EMAIL_SENDING_RETRY_COUNT + " retries.");
                    logger.error(e.getMessage());
                }
            }
        }
    }
}
