# `caa-streamworx`

caa-streamworx is a code repository for the backend data pipeline development of a CRA data science project aimed at investigating the security value of siteminder logs with a view towards automated intrusion detection. This work is being undertaken as part of a BCIP initiative in collaboration with streamworx.ai, and development work here is being carried out by AISS and CAA teams at the CRA.

The code in this repository is written in python and meant to be deployed on a databricks service running in a CRA azure subscription. Thus, it is largely a spark-based project. Since the deployment takes place in Azure, some dev ops are staged in the appropriate azure services, and some additional development takes place in notebooks which are not version-controlled here.

# Installation/Deployment
[Deployment of a new release on databricks using Azure Dev Ops](https://github.com/CRA-CAA/caa-streamworx/files/6894297/library.Release.pdf)

# Project Structure
This project consists of three broad categories of assets for constructing pipelines and machine learning models:

- Scripts for performing repeatable tasks, like ingesting data from a fixed source in a specific format
- Transformers, implemented as python classes extending the appropriate notions in either scikitlearn or spark
- Development related items like unit tests and test data.

Spark related scripts and transformers are located in `/src/caaswx/spark/`. Testing modules are located in `/tests/`. Testing data is located in `/data/`.

# Sample Usage
In a databricks notebook:

```python
import caaswx

df = table("raw_logs")
feature_generator = caaswx.spark.transformers.UserFeatureGenerator(window_step = 900, window_length = 600)

feature_generator.transform(df).take(50)
```

# Feature Documentation

serverfeaturegenerator.py 


| **Features**                 | **Description**                                                                                                        |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| StartTime                          | The beginning of a time window                                                                                                          |
| EndTime                            | The end of a time window                                                                                                                |
| VolOfAllLoginAttempts              | Number of all login attempts in the specified time window                                                                               |
| VolOfAllFailedLogins               | Number of all failed login attempts in the specified time window                                                                        |
| MaxOfFailedLoginsWithSameIPs       | Maximum Number of all failed login attempts with same IPs                                                                               |
| NumOfIPsLoginMultiAccounts         | Number of IPs used for logging into multiple accounts                                                                                   |
| NumOfReqsToChangePasswords         | Number of requests to change passwords; see #65                                                                                         |
| NumOfUsersWithEqualIntervalBtnReqs | Number of users with at least interval_threshold intervals between consecutive requests that are equal up to precision interval_epsilon |

Userfeaturegenerator.py

| **Features**                 | **Description**                                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| COUNT\_ADMIN\_LOGOUT              | Count of Admin Logout events during the time window, defined by sm\_eventid = 8.                                                                                                                                                                                   |
| COUNT\_AUTH\_ACCEPT               | Count of Auth Accept events during the time window, defined by sm\_eventid = 1.                                                                                                                                                                                    |
| COUNT\_ADMIN\_ATTEMPT             | Count of Admin Accept events during the time window, defined by sm\_eventid = 3.                                                                                                                                                                                   |
| COUNT\_ADMIN\_REJECT              | Count of Admin Reject events during the time window, defined by sm\_eventid = 2.                                                                                                                                                                                   |
| COUNT\_AZ\_ACCEPT                 | Count of Az Accept events during the time window, defined by sm\_eventid = 5.                                                                                                                                                                                      |
| COUNT\_AZ\_REJECT                 | Count of Az Reject events during the time window, defined by sm\_eventid = 6.                                                                                                                                                                                      |
| COUNT\_AUTH\_LOGOUT               | Count of Auth Logout events during the time window, defined by sm\_eventid = 10.                                                                                                                                                                                   |
| COUNT\_VISIT                      | Count of Visit events during the time window, defined by sm\_eventid = 13.                                                                                                                                                                                         |
| COUNT\_AUTH\_CHALLENGE            | Count of Auth Challenge events during the time window, defined by sm\_eventid = 4.                                                                                                                                                                                 |
| COUNT\_ADMIN\_REJECT              | Count of Admin Reject events during the time window, defined by sm\_eventid = 9.                                                                                                                                                                                   |
| COUNT\_ADMIN\_LOGIN               | Count of Admin Login events during the time window, defined by sm\_eventid = 7.                                                                                                                                                                                    |
| COUNT\_VALIDATE\_ACCEPT           | Count of Validate Accept events during the time window, defined by sm\_eventid = 11.                                                                                                                                                                               |
| COUNT\_VALIDATE\_REJECT           | Count of Validate Reject events during the time window, defined by sm\_eventid = 12.                                                                                                                                                                               |
| COUNT\_FAILED                     | Count of all Reject events during the time window, defined by sm\_eventid = 2, 6 and 9.                                                                                                                                                                            |
| COUNT\_GET                        | Count of all GET HTTP actions in SM\_ACTION during the time window.                                                                                                                                                                                                |
| COUNT\_POST                       | Count of all POST HTTP actions in SM\_ACTION during the time window.                                                                                                                                                                                               |
| COUNT\_HTTP\_METHODS              | Count of all GET and POST HTTP actions in SM\_ACTION  during the time window.                                                                                                                                                                                      |
| COUNT\_OU\_AMS                    | Count of all “ams” or “AMS” occurrences in SM\_USERNAME OR SM\_RESOURCE during the time window.                                                                                                                                                                    |
| COUNT\_OU\_CMS                    | Count of all “cra-cp” occurrences in SM\_USERNAME during the time window.                                                                                                                                                                                          |
| COUNT\_OU\_IDENTITY               | Count of all “ou=Identity” occurrences in SM\_USERNAME during the time window.                                                                                                                                                                                     |
| COUNT\_OU\_CRED                   | Count of all “ou=Credential” occurrences in SM\_USERNAME during the time window.                                                                                                                                                                                   |
| COUNT\_OU\_SECUREKEY              | Count of all “ou=SecureKey” occurrences in SM\_USERNAME during the time window.                                                                                                                                                                                    |
| COUNT\_PORTAL\_MYA                | Count of all “mima” occurrences in SM\_RESOURCE during the time window.                                                                                                                                                                                            |
| COUNT\_PORTAL\_MYBA               | Count of all “myba” occurrences in SM\_RESOURCE during the time window.                                                                                                                                                                                            |
| COUNT\_UNIQUE\_ACTIONS            | Count of distinct HTTP Actions in SM\_ACTION during the time window.                                                                                                                                                                                               |
| COUNT\_UNIQUE\_IPS                | Count of distinct IPs in SM\_CLIENTIP during the time window.                                                                                                                                                                                                      |
| COUNT\_UNIQUE\_EVENTS             | Count of distinct EventIDs in SM\_EVENTID  during the time window.                                                                                                                                                                                                 |
| COUNT\_UNIQUE\_USERNAME           | Count of distinct CNs in CN during the time window.                                                                                                                                                                                                                |
| COUNT\_UNIQUE\_RESOURCES          | Count of distinct Resource Strings in SM\_RESOURCE during the time window.                                                                                                                                                                                         |
| COUNT\_UNIQUE\_SESSIONS           | Count of distinct SessionIDs in SM\_SESSIONID during the time window.                                                                                                                                                                                              |
| COUNT\_RECORDS                    | Counts number of CRA\_SEQs (dataset primary key)                                                                                                                                                                                                                   |
| UNIQUE\_SM\_ACTIONS               | A distinct list of HTTP Actions in SM\_ACTION during time window.                                                                                                                                                                                                  |
| UNIQUE\_SM\_CLIENTIPS             | A distinct list of IPs in SM\_CLIENTIPS during time window.                                                                                                                                                                                                        |
| UNIQUE\_SM\_PORTALS               | A distinct list of Resource Strings in SM\_RESOURCE during time window.                                                                                                                                                                                            |
| UNIQUE\_SM\_TRANSACTIONS          | A distinct list of Transaction Ids in SM\_TRANSACTIONID during time window.                                                                                                                                                                                        |
| SM\_SESSION\_IDS                  | A distinct list of SessionIDs in SM\_SESSIONID during the time window.                                                                                                                                                                                             |
| COUNT\_UNIQUE\_OU                 | A count of distinct Entries containing “ou=” and a string ending in “,” in SM\_USERNAME during time window.                                                                                                                                                        |
| UNIQUE\_USER\_OU                  | A distinct list of Entries containing “ou=” and a string ending in “,” in SM\_USERNAME during time window.                                                                                                                                                         |
| COUNT\_PORTAL\_RAC                | A count of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window.                                                                                                                                                         |
| UNIQUE\_PORTAL\_RAC               | A distinct list of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window.                                                                                                                                                 |
| UNIQUE\_USER\_APPS                | A distinct list of root nodes from each record in SM\_RESOURCE during time window.                                                                                                                                                                                 |
| COUNTUNIQUE\_USER\_APPS           | A count of distinct root nodes from each record in SM\_RESOURCE during time window.                                                                                                                                                                                |
| USER\_TIMESTAMP                   | Minimum timestamp in SM\_TIMESTAMP during time window.                                                                                                                                                                                                             |
| AVG\_TIME\_BT\_RECORDS            | Average time between records during the time window.                                                                                                                                                                                                               |
| MAX\_TIME\_BT\_RECORDS            | Maximum time between records during the time window.                                                                                                                                                                                                               |
| MIN\_TIME\_BT\_RECORDS            | Minimum time between records during the time window.                                                                                                                                                                                                               |
| UserLoginAttempts                 | Total number of login attempts from the user within the specified time window                                                                                                                                                                                      |
| UserAvgFailedLoginsWithSameIPs    | Average number of failed logins with same IPs from the user (Note: the user may use multiple IPs; for each of the IPs, count the failed logins; then compute the average values of failed logins from all the IPs used by the same user)                           |
| UserNumOfAccountsLoginWithSameIPs | Total number of accounts visited by the IPs used by this user (this might be tricky to implement and expensive to compute, open to nixing).                                                                                                                        |
| UserNumOfPasswordChange           | Total number of requests for changing passwords by the user (See Seeing a password change from the events in \`raw\_logs\` #65)                                                                                                                                    |
| UserIsUsingUnusualBrowser         | Whether or not the browser used by the user in current time window is same as that in the previous time window, or any change within the current time window                                                                                                       |

Sessionfeaturegenerator.py

| **Features**                 | **Description**                                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| SESSION\_APPS                  | A distinct list of root nodes from each record in SM\_RESOURCE during time window.                                 |
| COUNT\_UNIQUE\_APPS            | A count of distinct root nodes from each record in SM\_RESOURCE during time window.                                |
| SESSION\_USER                  | A distinct list of CNs in CN during time window.                                                                   |
| COUNT\_ADMIN\_LOGIN            | Count of Admin Login events during the time window, defined by sm\_eventid = 7.                                    |
| COUNT\_ADMIN\_LOGOUT           | Count of Admin Logout events during the time window, defined by sm\_eventid = 8.                                   |
| COUNT\_ADMIN\_REJECT           | Count of Admin Reject events during the time window, defined by sm\_eventid = 2.                                   |
| COUNT\_FAILED                  | Count of all Reject events during the time window, defined by sm\_eventid = 2, 6 and 9.                            |
| COUNT\_VISIT                   | Count of Visit events during the time window, defined by sm\_eventid = 13.                                         |
| COUNT\_GET                     | Count of all GET HTTP actions in SM\_ACTION during the time window.                                                |
| COUNT\_POST                    | Count of all POST HTTP actions in SM\_ACTION during the time window.                                               |
| COUNT\_HTTP\_METHODS           | Count of all GET and POST HTTP actions in SM\_ACTION  during the time window.                                      |
| COUNT\_RECORDS                 | Counts number of CRA\_SEQs (dataset primary key)                                                                   |
| COUNT\_UNIQUE\_ACTIONS         | Count of distinct HTTP Actions in SM\_ACTION during the time window.                                               |
| COUNT\_UNIQUE\_EVENTS          | Count of distinct EventIDs in SM\_EVENTID  during the time window.                                                 |
| COUNT\_UNIQUE\_USERNAME        | Count of distinct CNs in CN during the time window.                                                                |
| COUNT\_UNIQUE\_RESOURCES       | Count of distinct Resource Strings in SM\_RESOURCE during the time window.                                         |
| COUNT\_UNIQUE\_REP             | A count of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window.         |
| SESSION\_SM\_ACTION            | A distinct list of HTTP Actions in SM\_ACTION during time window.                                                  |
| SESSION\_RESOURCE              | A distinct list of Resource Strings in SM\_RESOURCE during time window.                                            |
| SESSION\_REP\_APP              | A distinct list of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window. |
| SESSSION\_FIRST\_TIME\_SEEN    | Minimum time at which a record was logged during the time window.                                                  |
| SESSSION\_LAST\_TIME\_SEEN     | Maximum time at which a record was logged during the time window.                                                  |
| SDV\_BT\_RECORDS               | Standard deviation of timestamp deltas during the time window.                                                     |

IPfeaturegenerator.py

| **Features**                 | **Description**                                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| IP\_APP                      | A distinct list of root nodes from each record in SM\_RESOURCE during time window.                                 |
| IP\_AVG\_TIME\_BT\_RECORDS   | Average time between records during the time window.                                                               |
| IP\_MAX\_TIME\_BT\_RECORDS   | Maximum time between records during the time window.                                                               |
| IP\_MIN\_TIME\_BT\_RECORDS   | Minimum time between records during the time window.                                                               |
| IP\_COUNT\_ADMIN\_LOGIN      | Count of Admin Login events during the time window, defined by sm\_eventid = 7.                                    |
| IP\_COUNT\_ADMIN\_LOGOUT     | Count of Admin Logout events during the time window, defined by sm\_eventid = 8.                                   |
| IP\_COUNT\_ADMIN\_REJECT     | Count of Admin Reject events during the time window, defined by sm\_eventid = 9.                                   |
| IP\_COUNT\_AUTH\_ACCEPT      | Count of Auth Accept events during the time window, defined by sm\_eventid = 1.                                    |
| IP\_COUNT\_ADMIN\_ATTEMPT    | Count of Admin Accept events during the time window, defined by sm\_eventid = 3.                                   |
| IP\_COUNT\_AUTH\_CHALLENGE   | Count of Auth Challenge events during the time window, defined by sm\_eventid = 4.                                 |
| IP\_COUNT\_AUTH\_LOGOUT      | Count of Auth Logout events during the time window, defined by sm\_eventid = 10.                                   |
| IP\_COUNT\_ADMIN\_REJECT     | Count of Admin Reject events during the time window, defined by sm\_eventid = 2.                                   |
| IP\_COUNT\_AZ\_ACCEPT        | Count of Az Accept events during the time window, defined by sm\_eventid = 5.                                      |
| IP\_COUNT\_AZ\_REJECT        | Count of Az Reject events during the time window, defined by sm\_eventid = 6.                                      |
| IP\_COUNT\_FAILED            | Count of all Reject events during the time window, defined by sm\_eventid = 2, 6 and 9.                            |
| IP\_COUNT\_GET               | Count of all GET HTTP actions in SM\_ACTION during the time window.                                                |
| IP\_COUNT\_POST              | Count of all POST HTTP actions in SM\_ACTION during the time window.                                               |
| IP\_COUNT\_HTTP\_METHODS     | Count of all GET and POST HTTP actions in SM\_ACTION  during the time window.                                      |
| IP\_COUNT\_OU\_AMS           | Count of all “ams” or “AMS” occurrences in SM\_USERNAME OR SM\_RESOURCE during the time window.                    |
| IP\_COUNT\_OU\_CMS           | Count of all “cra-cp” occurrences in SM\_USERNAME during the time window.                                          |
| IP\_COUNT\_OU\_IDENTITY      | Count of all “ou=Identity” occurrences in SM\_USERNAME during the time window.                                     |
| IP\_COUNT\_OU\_CRED          | Count of all “ou=Credential” occurrences in SM\_USERNAME during the time window.                                   |
| IP\_COUNT\_OU\_SECUREKEY     | Count of all “ou=SecureKey” occurrences in SM\_USERNAME during the time window.                                    |
| IP\_COUNT\_PORTAL\_MYA       | Count of all “mima” occurrences in SM\_RESOURCE during the time window.                                            |
| IP\_COUNT\_PORTAL\_MYBA      | Count of all “myba” occurrences in SM\_RESOURCE during the time window.                                            |
| IP\_COUNT\_UNIQUE\_ACTIONS   | Count of distinct HTTP Actions in SM\_ACTION during the time window.                                               |
| IP\_COUNT\_UNIQUE\_EVENTS    | Count of distinct EventIDs in SM\_EVENTID  during the time window.                                                 |
| IP\_COUNT\_UNIQUE\_USERNAME  | Count of distinct CNs in CN during the time window.                                                                |
| IP\_COUNT\_UNIQUE\_RESOURCES | Count of distinct Resource Strings in SM\_RESOURCE during the time window.                                         |
| IP\_COUNT\_UNIQUE\_SESSIONS  | Count of distinct SessionIDs in SM\_SESSIONID during the time window.                                              |
| IP\_COUNT\_PORTAL\_RAC       | A count of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window.         |
| IP\_COUNT\_RECORDS           | Counts number of CRA\_SEQs (dataset primary key)                                                                   |
| IP\_COUNT\_VISIT             | Count of Visit events during the time window, defined by sm\_eventid = 13.                                         |
| IP\_COUNT\_VALIDATE\_ACCEPT  | Count of Validate Accept events during the time window, defined by sm\_eventid = 11.                               |
| IP\_COUNT\_VALIDATE\_REJECT  | Count of Validate Reject events during the time window, defined by sm\_eventid = 12.                               |
| IP\_UNIQUE\_SM\_ACTIONS      | A distinct list of HTTP Actions in SM\_ACTION during time window.                                                  |
| IP\_UNIQUE\_USERNAME         | A distinct list of CNs in CN during time window.                                                                   |
| IP\_UNIQUE\_SM\_SESSION      | A distinct list of SessionIDs in SM\_SESSIONID during time window.                                                 |
| IP\_UNIQUE\_SM\_PORTALS      | A distinct list of Resource Strings in SM\_RESOURCE during time window.                                            |
| IP\_UNIQUE\_SM\_TRANSACTIONS | A distinct list of Transaction Ids in SM\_TRANSACTIONID during time window.                                        |
| IP\_UNIQUE\_USER\_OU         | A distinct list of Entries containing “ou=” and a string ending in “,” in SM\_USERNAME during time window.         |
| IP\_UNIQUE\_REP\_APP         | A distinct list of Entries containing “rep” followed by a string ending in “/” in SM\_RESOURCE during time window. |
| IP\_TIMESTAMP                | Earliest timestamp during time window.                                                                             |
| IP\_COUNT\_UNIQUE\_OU        | A count of distinct Entries containing “ou=” and a string ending in “,” in SM\_USERNAME during time window.        |


# License
[MIT](https://choosealicense.com/licenses/mit/)
