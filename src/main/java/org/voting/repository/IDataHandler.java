package org.voting.repository;

import com.fasterxml.jackson.databind.JsonNode;

public interface IDataHandler {

    void createUser(String userId, JsonNode jsonNode);

    User getUser(String userId);

    void updateUser(String userId, JsonNode jsonNode);

    void deleteUser(String userId);

    void createPost(String postId, JsonNode jsonNode);

    Post getPost(String postId);

    void deletePost(String postId);

    void vote(String postId, String userId, String selectedOption);

    void updateEndDate(String postId, String ownerId, String newEndDate);
}
