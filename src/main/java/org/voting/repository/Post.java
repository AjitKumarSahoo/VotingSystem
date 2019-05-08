package org.voting.repository;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/7/2019.
 */
@Getter
@Setter
public class Post {
    String postId;
    String ownerId;
    Set<String> options;
    Map<String, String> user2OptionMap;
    String creationDate;
    String endDate;
}
