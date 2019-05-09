package org.voting;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
@Getter
@Setter
public class PostData {
    String postId;
    String ownerEmailId;
    Map<String, String> user2optionMap;
}
