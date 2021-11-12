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

# Description of dataset's columns

|    **Column Name**                 |           **Description**                                                                                                        |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| CRA_SEQ |  Serves as the primary key for the Siteminder data and can be used for counting unique rows via aggregation steps.
| CRA_TZ_OFFSET | Time zone offset (Majority of rows have 5 and 6 as the value of this column.
| SM_ACTION | Records the  HTTP action. Get, Post, and Put. It can contain NULLs.
| SM_AGENTNAME | Name associated with the agent that is being used in conjunction with the policy server.
| SM_AUTHDIRNAME | This is not used by the reports generator and by the programs of this project.
| SM_AUTHDIRNAMESPACE | This is not used by the reports generator and by the programs of this project.
| SM_AUTHDIRSERVER | This is not used by the reports generator and by the programs of this project.
| SM_CATEGORYID | The identifier for the type of logging.
| SM_CLIENTIP | The IP address for the client machine that is trying to utilize a protected resource.
| SM_DOMAINNAME | The unique name for the domain in which the realm and resource the user is accessing exist.
| SM_DOMAINOID | The unique identifier for the domain in which the realm and resource the user is accessing exist.
| SM_EVENTID | Marks the particular event that caused the logging to occur.
| SM_HOSTNAME | Machine on which the server is running.
| SM_IMPERSONATORDIRNAME | Login name of the administrator directory that is acting as the impersonator in an impersonated session.
| SM_IMPERSONATORNAME | Login name of the administrator directory that is acting as the impersonator in an impersonated session.
| SM_REALMNAME | Current realm's name in which the resource that the user wants resides.
| SM_REALMOID | Current realm's identifier in which the resource that the user wants resides.
| SM_REASON | Motivations for logging. 32000 and above are user defined.
| SM_RESOURCE | Resource, for example a web page, that the user is requesting.
| SM_HOSTNAME | Machine on which the server is running.
| SM_SESSIONID | Session identifier for this user’s activity.
| SM_STATUS | Some descriptive text about the action.
| SM_TIMESTAMP | Marks the time at which the entry was made to the database.
| SM_TIMESTAMPTRUNC | Stores the truncated timestamp recording the date from the SM_TIMESTAMP.
| SM_TIMESTAMPTRUNC | Machine on which the server is running.
| SM_TRANSACTIONID | This is not used by the reports generator.
| SM_USERNAME | Username logged into the session

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


# Working with Sphinx docstrings
We have utilized the [Sphinx API](https://www.sphinx-doc.org/en/master/) for documenting every module. `Sphinx` makes sure that the parsed docstrings (written as per `Sphinx` API format ) get converted into flexible HTML documents which can be viewed on browsers in an easy-to-read fashion. The recommended way of adding documentation is appropriate utilization of `Sphinx` by formatting the comment docstrings as per `Sphinx` parser's design. This section explains the pre-requisites and steps for creating/updating the `Sphinx` web documents. 

## Prerequisites
- The system must have `Sphinx` API pre-installed. If not, open the `Terminal` and install it via: `sudo apt-get install python3-sphinx -y`
- The required `Python` libraries utilized in the code to be documented must be installed in the system.

## Steps for creating Sphinx documentation for the first time

Please follow the following steps __only if there are no existing Sphinx documents in your project repository__. If there is a folder named docs in your repository, please follow the steps written in the next section (Steps for adding documents to existing ones).

### Sphinx setup steps
- Let's assume that the `Python` code to be documented lies in the relative path within the repository: *./Folder_A/Folder_B/*. Navigate the `terminal` into the project repository.
- Write the required `Sphinx` docstrings within each `Python` file. (Refer: [Sphinx Documentation](https://pythonhosted.org/an_example_pypi_project/sphinx.html))
- Create a documentation directory (Let's say *docs*) within the project repository and navigate the terminal inside it (`mkdir docs` and then `cd docs`).
- Execute: `sphinx-quickstart` at this path. Fill the options like *project name*, *author name*, etc. (__Important: Select the autodoc option to "y"__). A bunch of files will be created within the docs folder.
- Edit `conf.py`. Uncomment the `import` commands and set the root path by un-commenting and writing `sys.path.insert(0, os.path.abspath('../Folder_A/Folder_B/'))`.
- Edit `index.rst` by writing _modules_ in the next line of _:caption: Contents:_ (__with the same indentation__).
- Execute `sphinx-apidoc -o . ../Folder_A/Folder_B/` ("." represents the output_path which is inside docs folder and "../Folder_A/Folder_B/" represents the path to the package to document.
- Execute `make html` and the HTML pages will be built in __build/html_.
- View the HTML pages in a browser.

## Steps for adding documents to existing ones
Let's assume that there are some `Sphinx` documents already available in the project repository and we want to edit the existing documents or append new ones. Please follow the steps given below for achieving these goals:

- Write your new docstrings as per [Sphinx](https://pythonhosted.org/an_example_pypi_project/sphinx.html) format.
- Open a new `terminal` inside the `docs` folder.
- Execute `sh update_documentation.sh`. 

__update_documentation.sh__ is a Shell script ensuring a smooth creation of new `Sphinx` documents as per the updated Sphinx docstrings by performing the required cleaning and execution of `Sphinx` steps.



# License
[MIT](https://choosealicense.com/licenses/mit/)
