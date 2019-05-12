## Voting System ##

**Problem statement**: VotingSystem/Voting_System_problem_statement.txt </br>
**Design Doc**: [link](https://docs.google.com/document/d/1dMLvaUTW1AOPfGZnHg1seIuN5jXd82uNSFpQCCAcm8w/edit?usp=sharing)

There are totally 3 services present in this voting system. PostTracker project has 2 modules representing 2 services: Post Tracker service and Notification service. VotingSystem1 folder contains Voting service.

* Voting Service
  * It exposes all the Rest APIs to client
  * When an owner creates a post, the service creates a post entry with status NEW in the PostInfo table in Amazon DynamoDB and then it notifies all the participants in the post through email
  * When a participant votes the service directly updates the User2OptionMap in the post entry in PostInfo table

* Post Tracker Service
  * Singleton service that reads latest expired posts from PostInfo table
  * Multithreaded service where each thread: 
    * Processes an expired post to calculate the winner option
    * Sends a message to PostQueue (Amazon SQS) with relevant info
  * Has a dedicated clean up thread that runs periodically and deletes posts which have expired and have status DONE 

* Notification Service
  * Reads messages from PostQueue
  * Multithreaded service where each thread: 
    * Processes one message
    * Send winner email to post owner
    * Updates the post status to DONE in PostInfo table 
