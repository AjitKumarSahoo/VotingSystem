package org.voting.repository;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Date;

public interface IDataHandler {

    /* ********** Database APIs *******/
    void createUser(String userId, User user);

    User getUser(String userId);

    void updateUser(String userId, User user);

    void deleteUser(String userId);

    void createPost(String postId, JsonNode post);

    Post getPost(String postId);

    void deletePost(String postId);

    void vote(String postId, String userId, String selectedOption);

    void updateEndDate(String postId, String ownerId, String newEndDate);
}
