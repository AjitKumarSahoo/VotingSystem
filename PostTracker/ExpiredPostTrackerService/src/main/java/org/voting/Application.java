package org.voting;

//import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.voting.Service.DataHandlers.PostCleanUpUtility;
import org.voting.Service.PostProcessor;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        new PostCleanUpUtility().cleanUp();
        new PostProcessor().processPost();
    }
}
