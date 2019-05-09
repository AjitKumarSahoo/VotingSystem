package org.voting.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.voting.repository.IDataHandler;
import org.voting.repository.Post;
import org.voting.repository.User;

@Service
public class VotingService {

    private IDataHandler dataHandler;

    @Autowired
    public VotingService(IDataHandler dataHandler) {
        this.dataHandler = dataHandler;
    }

    public void createUser(String key, JsonNode jsonNode) {
        dataHandler.createUser(key, jsonNode);
    }

    public User readUser(String key) {
        return dataHandler.getUser(key);
    }

    public void deleteUser(String key) {
        dataHandler.deleteUser(key);
    }

    public void UpdateUser(String key, JsonNode jsonNode) {
        dataHandler.updateUser(key, jsonNode);
    }

    public void createPost(String postId, JsonNode post) {
        dataHandler.createPost(postId, post);
    }

    public Post readPost(String postId) {
        return dataHandler.getPost(postId);
    }

    public void deletePost(String postId) {
        dataHandler.deletePost(postId);
    }

    public void UpdatePost(String postId, String ownerId, String newEndDate) {
        dataHandler.updateEndDate(postId, ownerId, newEndDate);
    }

    public void vote(String postId, String userId, String selectedOption) {
        dataHandler.vote(postId, userId, selectedOption);
    }

}
