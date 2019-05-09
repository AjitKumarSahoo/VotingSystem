package org.voting.repository;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;
import org.voting.service.NotificationHandler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class DataHandler implements IDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(DataHandler.class.getName());

    private static final String USER_TABLE_NAME = "User";
    public static final String USER_ID_KEY = "UserId";
    public static final String EMAIL_ID = "EmailId";

    private static final String POST_TABLE_NAME = "PostInfo";
    private static final String POST_ID_KEY = "PostId";
    private static final String POST_OPTIONS = "Options";
    private static final String POST_OWNER_EMAIL_ID = "OwnerEmailId";
    private static final String POST_USER_2_OPTION_MAP = "User2OptionMap";
    private static final String POST_CREATION_DATE = "CreationDate";
    private static final String POST_END_DATE = "EndDate";
    private static final String POST_STATUS = "Status";

    private NotificationHandler notificationHandler;
    private static DynamoDB dynamoDB;

    private enum PostStatus {
        NEW, EXPIRED, DONE
    }

    @PostConstruct
    void init() {
        notificationHandler = new NotificationHandler();
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build();
        dynamoDB = new DynamoDB(client);
        createUserTable();
        createPostTable();
    }

    private void createPostTable() {
        try {
            createTable(POST_TABLE_NAME, "PostId", 10000L, 500L); //values can be decided upon the usage of the application
        } catch (Exception e) {
            logger.error("Unable to create table: " + POST_TABLE_NAME);
            logger.error(e.getMessage());
        }
    }

    private void createUserTable() {
        try {
            createTable(USER_TABLE_NAME, "UserId", 100L, 10L); //values can be decided upon the usage of the application
        } catch (Exception e) {
            logger.error("Unable to create " + USER_TABLE_NAME + " table.");
            logger.error(e.getMessage());
        }
    }

    private void createTable(String tableName, String hashKey,
                             long readUnit, long writeUnit) throws InterruptedException {
        logger.info("Creating " + tableName + "...");
        try {
            Table table = dynamoDB.createTable(tableName,
                    Collections.singletonList(new KeySchemaElement(hashKey, KeyType.HASH)),
                    Collections.singletonList(new AttributeDefinition(hashKey, ScalarAttributeType.S)),
                    new ProvisionedThroughput(readUnit, writeUnit));
            table.waitForActive();
            logger.info(tableName + " status: " + table.getDescription().getTableStatus());
        } catch (ResourceInUseException e) {
            logger.info(tableName + " already exists");
            //table already exists..do nothing.
        }
    }


    @Override
    public void createUser(String userId, JsonNode jsonNode) {
        Table table = dynamoDB.getTable(USER_TABLE_NAME);
        try {
            if (!jsonNode.has(EMAIL_ID)) {
                throw new RuntimeException("Email Id must be provided.");
            }
            String emailId = jsonNode.get(EMAIL_ID).asText();
            if (StringUtils.isEmpty(userId) || StringUtils.isEmpty(emailId)) {
                throw new RuntimeException("UserId and EmailId should not be null.");
            }

            Item item = new Item().withPrimaryKey("UserId", userId).withString("EmailId", emailId);
            table.putItem(item);
        } catch (Exception e) {
            logger.error("Inserting user into table failed.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public User getUser(String userId) {
        Table table = dynamoDB.getTable(USER_TABLE_NAME);
        try {
            if (StringUtils.isEmpty(userId)) {
                return null;
            }

            Item item = table.getItem(USER_ID_KEY, userId, EMAIL_ID, null);
            if (item == null) {
                logger.warn("User with " + userId + " is not present in " + USER_TABLE_NAME);
                return null;
            }

            User user = new User();
            user.setUserId(userId);
            user.setEmailId(item.get("EmailId").toString());

            return user;
        } catch (Exception e) {
            logger.error("Retrieving user with id " + userId + " failed.");
            logger.error(e.getMessage());
        }

        return null;
    }

    @Override
    public void updateUser(String userId, JsonNode jsonNode) {
        Table table = dynamoDB.getTable(USER_TABLE_NAME);
        try {
            if (!jsonNode.has(EMAIL_ID) || StringUtils.isEmpty(jsonNode.get(EMAIL_ID).asText())) {
                return;
            }
            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(USER_ID_KEY, userId)
                    .withUpdateExpression("set #a = :val").withNameMap(new NameMap().with("#a", "EmailId"))
                    .withValueMap(new ValueMap().withString(":val", jsonNode.get(EMAIL_ID).asText()))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            logger.info("User " + userId + " is successfully updated.");
            logger.info(outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            logger.error("Updating user " + userId + "failed.");
            logger.error(e.getMessage());
        }
    }

    @Override
    public void deleteUser(String userId) {
        Table table = dynamoDB.getTable(USER_TABLE_NAME);

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(USER_ID_KEY, userId).withReturnValues(ReturnValue.ALL_OLD);
            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            logger.info("User " + userId + " was deleted.");
            logger.info(outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            logger.error("Error deleting user " + userId + " from " + USER_TABLE_NAME);
            logger.error(e.getMessage());
        }
    }

    @Override
    public void createPost(String postId, JsonNode jsonNode) {
        Table table = dynamoDB.getTable(POST_TABLE_NAME);
        try {
            validateInputData(jsonNode);
            Post post = buildPost(jsonNode);
            Item item = new Item().withPrimaryKey(POST_ID_KEY, postId)
                    .withString(POST_OWNER_EMAIL_ID, post.getOwnerId())
                    .withStringSet(POST_OPTIONS, new HashSet<>(post.getOptions()))
                    .withMap(POST_USER_2_OPTION_MAP, post.getUser2OptionMap())
                    .withString(POST_CREATION_DATE, post.getCreationDate())
                    .withString(POST_END_DATE, post.getEndDate())
                    .withString(POST_STATUS, PostStatus.NEW.toString());

            PutItemOutcome putItemOutcome = table.putItem(item);
            logger.info("Successfully created a post: " + putItemOutcome);
            boolean broadCastingSuccessful = notificationHandler.broadcast(postId, getParticipantsEmailId(jsonNode));
            if (!broadCastingSuccessful) {
                table.deleteItem(POST_ID_KEY, postId);
            }

        } catch (Exception e) {
            logger.error("Creating post failed.");
            logger.error(e.getMessage());
        }
    }

    private void validateInputData(JsonNode jsonNode) throws IOException {
        validateOwnerId(jsonNode);
        validateOptions(jsonNode);
        validateParticipants(jsonNode);
        validateEndDate(jsonNode);
    }

    private Post buildPost(JsonNode jsonNode) throws IOException {
        ObjectReader reader = getJsonObjectReader();

        Post post = new Post();
        post.setPostId(UUID.randomUUID().toString());
        post.setOwnerId(jsonNode.get("OwnerId").asText());
        List<String> optionList = reader.readValue(jsonNode.get("Options"));
        post.setOptions(new HashSet<>(optionList));
        Map<String, String> user2optionMap = getUser2EmptyOptionMap(jsonNode, reader);
        post.setUser2OptionMap(user2optionMap);
        post.setEndDate(getFormattedEndDate(jsonNode.get("EndDate").asText()));

        return post;
    }

    private ObjectReader getJsonObjectReader() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readerFor(new TypeReference<List<String>>() {});
    }

    private Map<String, String> getUser2EmptyOptionMap(JsonNode jsonNode, ObjectReader reader) throws IOException {
        List<String> participants = reader.readValue(jsonNode.get("Participants"));
        Map<String, String> user2optionMap = new HashMap<>();
        for (String user : participants) {
            user2optionMap.put(user, null);
        }
        return user2optionMap;
    }

    private Set<String> getParticipantsEmailId(JsonNode jsonNode) throws IOException {
        List<String> participants = getJsonObjectReader().readValue(jsonNode.get("Participants"));
        return participants.stream().map(userId -> getUser(userId).getEmailId()).collect(Collectors.toSet());
    }

    private void validateOwnerId(JsonNode jsonNode) {
        if (!jsonNode.has("OwnerId")) {
            throw new RuntimeException("OwnerId must be provided.");
        }
        String ownerId = jsonNode.get("OwnerId").asText();
        if (StringUtils.isEmpty(ownerId)) {
            throw new RuntimeException("Invalid Owner email id.");
        }

        if (getUser(ownerId) == null) {
            throw new RuntimeException("Invalid OwnerId as it doesn't exist.");
        }
    }

    private void validateEndDate(JsonNode jsonNode) {
        if (!jsonNode.has("EndDate")) {
            throw new RuntimeException("EndDate list must be provided.");
        }
        String endDate = jsonNode.get("EndDate").asText();
        String currentTimeStamp = getDateAsString(new Date());
        if (StringUtils.isEmpty(endDate) || currentTimeStamp.compareTo(getFormattedEndDate(endDate)) >= 0 ) {
            throw new RuntimeException("End date of post can't be empty or equal to or older than current date");
        }
    }

    private void validateOptions(JsonNode jsonNode) throws IOException {
        if (!jsonNode.has("Options")) {
            throw new RuntimeException("Options list must be provided.");
        }

        List<String> optionList = getJsonObjectReader().readValue(jsonNode.get("Options"));
        if (optionList.size() == 0) {
            throw new RuntimeException("Options list can't be empty");
        }
        if (new HashSet<>(optionList).size() != optionList.size()) {
            throw new RuntimeException("Options list can't have duplicate entry.");
        }
    }

    private void validateParticipants(JsonNode jsonNode) throws IOException {
        if (!jsonNode.has("Participants")) {
            throw new RuntimeException("Participants list must be provided.");
        }
        Set<String> participants = new HashSet<>(getJsonObjectReader().readValue(jsonNode.get("Participants")));
        if (participants.size() == 0) {
            throw new RuntimeException("Participants list can't be empty");
        }
        List<String> invalidUsers = participants.stream().filter(user -> getUser(user) == null).collect(Collectors.toList());
        if (invalidUsers.size() > 0) {
            throw new RuntimeException("Invalid users: " + Arrays.toString(invalidUsers.toArray()));
        }
    }

    @Override
    public Post getPost(String postId) {
        try {
            Table table = dynamoDB.getTable(POST_TABLE_NAME);
            String projectionExpression = POST_OWNER_EMAIL_ID + ", " + POST_OPTIONS + ", " + POST_USER_2_OPTION_MAP +
                    ", " + POST_END_DATE + ", " + POST_STATUS;
            Item item = table.getItem(POST_ID_KEY, postId, projectionExpression, null);
            if (item == null) {
                logger.warn("Post with " + postId + " is not present in " + POST_TABLE_NAME);
                return null;
            }

            if (getDateAsString(new Date()).compareTo(item.getJSON(POST_END_DATE)) > 0) {
                throw new RuntimeException("The post has already expired."); // this is to avoid accessing the post for voting
            }

            Post post = new Post();
            post.setPostId(postId);
            post.setOwnerId(item.getJSON(POST_OWNER_EMAIL_ID));
            post.setOptions(item.getStringSet(POST_OPTIONS));
            post.setUser2OptionMap(item.getMap(POST_USER_2_OPTION_MAP));
            post.setEndDate(item.getJSON(POST_END_DATE));
            post.setStatus(item.getJSON(POST_STATUS));
            return post;
        } catch (Exception e) {
            logger.error("Retrieving post with id " + postId + " failed.");
            logger.error(e.getMessage());
        }

        return null;
    }

    @Override
    public void vote(String postId, String userId, String selectedOption) {
        Table table = dynamoDB.getTable(POST_TABLE_NAME);
        try {
            Post post = getPost(postId);
            if(!post.getUser2OptionMap().containsKey(userId)) {
                throw new RuntimeException("Invalid user " + userId + " for post " + postId);
            }
            if (!post.getOptions().contains(selectedOption)) {
                throw new RuntimeException("Invalid option " + selectedOption + " for post " + postId);
            }

            Map<String, String> map = post.getUser2OptionMap();
            map.replace(userId, selectedOption); // userId entry will always be present since we create the map with all participants while creating the post
            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(POST_ID_KEY, postId)
                    .withUpdateExpression("set #m=:val")
                    .withNameMap(new NameMap().with("#m", POST_USER_2_OPTION_MAP))
                    .withValueMap(new ValueMap().withMap(":val", map))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            logger.info("Successfully updated post " + postId + " with " + userId + "'s vote for " + selectedOption);
            logger.info(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            logger.error("Failed to update " + userId + "'s vote for " + selectedOption + " in " + POST_TABLE_NAME);
            logger.error(e.getMessage());
        }
    }

    @Override
    public void deletePost(String postId) {
        Table table = dynamoDB.getTable(POST_TABLE_NAME);

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(POST_ID_KEY, postId).withReturnValues(ReturnValue.ALL_OLD);
            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            logger.info("Post " + postId + " is deleted.");
            logger.info(outcome.getItem().toJSONPretty());
        } catch (Exception e) {
            logger.error("Error deleting post " + postId + " from " + POST_TABLE_NAME);
            logger.error(e.getMessage());
        }
    }

    @Override
    public void updateEndDate(String postId, String ownerId, String newEndDate) {
        Table table = dynamoDB.getTable(POST_TABLE_NAME);
        try {
            Post post = getPost(postId);
            if (ownerId == null || !post.getOwnerId().equals(ownerId)) {
                throw new RuntimeException(ownerId + " is not the owner of post " + postId + ". Only owner can modify a post's end date");
            }

            if (newEndDate == null || post.getCreationDate().compareTo(getFormattedEndDate(newEndDate)) >= 0) {
                throw new RuntimeException("Post's end date " + newEndDate + " can't be older or equal to creation date: " + post.getCreationDate());
            }

            if (PostStatus.DONE.toString().equals(post.getStatus())) {
                throw new RuntimeException("An expired post can't be modified.");
            }

            String formattedEndDate = getFormattedEndDate(newEndDate);
            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(POST_ID_KEY, postId)
                    .withUpdateExpression("set #m=:val")
                    .withNameMap(new NameMap().with("#m", POST_END_DATE))
                    .withValueMap(new ValueMap().withString(":val", formattedEndDate))
                    .withReturnValues(ReturnValue.ALL_NEW);

            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);

            logger.info("Successfully updated post " + postId + " with end date = " + formattedEndDate);
            logger.info(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            logger.error("Failed to update " + postId + " with new end date " + newEndDate + " requested by " + ownerId);
            logger.error(e.getMessage());
        }
    }

    private String getFormattedEndDate(String endDate) {
        return getDateAsString(parseEndDate(endDate));
    }

    private String getDateAsString(Date date) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                    .withZone(ZoneOffset.UTC)
                    .format(date.toInstant());
    }

    private Date parseEndDate(String date) {
        try {
            return new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss").parse(date);
        } catch (ParseException e) {
            logger.error("Failed to parse end date: " + date);
            logger.error(e.getMessage());
        }
        return null;
    }

    @PreDestroy
    private void cleanUp() {
        dynamoDB = null;
        notificationHandler = null;
    }
}
