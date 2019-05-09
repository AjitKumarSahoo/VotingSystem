package org.voting.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;
import java.util.Set;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/9/2019.
 */
public class NotificationHandler {
    private static final Logger logger = LoggerFactory.getLogger(NotificationHandler.class.getName());
    private static final String sender = "voting-system@do-not-reply.com";
    private static final String host = "127.0.0.1";
    private static final int MAX_EMAIL_SENDING_RETRY_COUNT = 3;

    public NotificationHandler(){}

    /**
     * Tries sending email on given email id. No. of retries is 3 if sending fails.
     * @param postId - post id for which email is being sent
     * @param emailIds - participants' email id
     */
    public boolean broadcast(String postId, Set<String> emailIds) {
        int retryCount = MAX_EMAIL_SENDING_RETRY_COUNT;
        while (retryCount != 0) {
            try
            {
                Properties properties = System.getProperties();
                properties.setProperty("mail.smtp.host", host);
                Session session = Session.getDefaultInstance(properties);
                MimeMessage message = new MimeMessage(session);
                message.setFrom(new InternetAddress(sender));
                message.addRecipients(Message.RecipientType.TO, getEmailAddresses(emailIds));
                message.setSubject("Winner for post: " + postId);
                message.setText("Hello,\n Please login to system to vote for post " + postId);

                Transport.send(message);
                logger.info("Successfully sent all participants for post: " + postId);
                return true;
            } catch (MessagingException e) {
                if (--retryCount == 0) {
                    logger.error("Failed to broadcast post notification for post " + postId +
                            " with " + MAX_EMAIL_SENDING_RETRY_COUNT + " retries.");
                    logger.error(e.getMessage());
                }
            }
        }
        return false;
    }

    private Address[] getEmailAddresses(Set<String> emailIds) throws AddressException {
        Address[] addresses = new Address[emailIds.size()];
        int i = 0;
        for (String emailId : emailIds) {
            addresses[i++] = new InternetAddress(emailId);
        }
        return addresses;
    }
}
