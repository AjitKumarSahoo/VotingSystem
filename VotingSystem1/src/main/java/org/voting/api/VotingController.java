package org.voting.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.voting.repository.Post;
import org.voting.repository.User;
import org.voting.service.VotingService;

@RestController
public class VotingController {

    private static final Logger logger = LoggerFactory.getLogger(VotingController.class.getName());
    private VotingService service;

    @Autowired
    public VotingController(VotingService service) {
        this.service = service;
    }

    @RequestMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

    @RequestMapping(value = "/users/{key}",
            method = RequestMethod.POST,
            consumes = "application/json",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity createUser(@PathVariable String key, @RequestBody JsonNode jsonNode) {
        logger.info("Entered create for key: " + key);
        service.createUser(key, jsonNode);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @RequestMapping(value = "/users/{key}",
            method = RequestMethod.PUT,
            consumes = "application/json",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity updateUser(@PathVariable String key, @RequestBody JsonNode node) {
        logger.info("Entered create for key: " + key);
        service.UpdateUser(key, node);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @RequestMapping(value = "/users/{key}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<User> getUser(@PathVariable String key) {
        logger.info("Entered read for key: " + key);
        User node = service.readUser(key);
        return new ResponseEntity<>(node, HttpStatus.OK);
    }

    @RequestMapping(value = "/users/{key}",
            method = RequestMethod.DELETE)
    public ResponseEntity deleteUser(@PathVariable String key) {
        logger.info("Entered delete for key: " + key);
        service.deleteUser(key);
        return new ResponseEntity(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/post/{key}",
            method = RequestMethod.POST,
            consumes = "application/json",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity createPost(@PathVariable String key, @RequestBody JsonNode jsonNode) {
        logger.info("Entered create for key: " + key);
        service.createPost(key, jsonNode);
        return new ResponseEntity(HttpStatus.CREATED);
    }

    @RequestMapping(value = "/post/{key}",
            method = RequestMethod.PUT,
            consumes = "application/json",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity updatePost(@PathVariable String key, @RequestBody JsonNode jsonNode) {
        logger.info("Entered create for key: " + key);
        service.UpdatePost(key, jsonNode.get("OwnerId").asText(), jsonNode.get("EndDate").asText());
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/post/{key}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Post> getPost(@PathVariable String key) {
        logger.info("Entered read for key: " + key);
        Post node = service.readPost(key);
        return new ResponseEntity<>(node, HttpStatus.OK);
    }

    @RequestMapping(value = "/post/{key}",
            method = RequestMethod.DELETE)
    public ResponseEntity deletePost(@PathVariable String key) {
        logger.info("Entered delete for key: " + key);
        service.deletePost(key);
        return new ResponseEntity(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/vote/{key}",
            method = RequestMethod.PUT,
            consumes = "application/json",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity vote(@PathVariable String key, @RequestBody JsonNode jsonNode) {
        logger.info("Entered create for key: " + key);
        service.vote(key, jsonNode.get("UserId").asText(), jsonNode.get("Option").asText());
        return new ResponseEntity(HttpStatus.OK);
    }
}
