package org.voting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.voting.Service.MessageProcessor;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        new MessageProcessor().processMessage();
    }
}
