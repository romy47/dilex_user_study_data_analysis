import pymongo
from pymongo import MongoClient
import time
import pandas as pd
import dateutil.parser as dp
import json





# Initial FIle Read and Variable Setup ##########################################################################
json_data = pd.read_json('loglists.json')
timing = pd.read_csv('user_log_timing.csv')
logfrequencyProtoS1 = pd.read_csv('log_frequency_proto_extended_full.csv')
logfrequencyProtoS1Unnamed = []
logfrequencyProtoS2 = pd.read_csv('log_frequency_proto_extended_full.csv')
logfrequencyProtoS2Unnamed = []

logfrequencyProtoS1 = logfrequencyProtoS1.drop_duplicates(subset=["Event ID"], keep="first")
logfrequencyProtoS2 = logfrequencyProtoS2.drop_duplicates(subset=["Event ID"], keep="first")

logfrequencyProtoS1ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoS1["Event ID"]])
logfrequencyProtoS1ByUser.insert(0, 'userID', '--')

logfrequencyProtoS2ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoS2["Event ID"]])
logfrequencyProtoS2ByUser.insert(0, 'userID', '--')

logfrequencyProtoS1["Count"] = logfrequencyProtoS1["Count"].fillna(0)
logfrequencyProtoS2["Count"] = logfrequencyProtoS2["Count"].fillna(0)

print(logfrequencyProtoS1.count())
# Change Workspace interaction-id according to the new classification ##########################################################################
finalLogs = [];
for user in json_data["logs"]:
    current_user_timing = timing.loc[timing['userID'] == user["userID"]]
    currentState = 'SERP'
    # print("*********************")
    if(current_user_timing.empty == False):
        for log in user["logs"]:
            if(log.get("eventDetails").get("name") != None):
                if(log.get("eventDetails").get("name")=="workspace-opened"):
                    currentState = 'Workspace'
                elif(log.get("eventDetails").get("name") == "dashboard-opened" or log.get("eventDetails").get("name") == "serp-opened" or log.get("eventDetails").get("name") == "task-detail-opened"):
                    currentState = 'SERP'
                log["state"] = currentState

            if (log.get("eventDetails").get("name") != None):
                if(log["state"]=="Workspace"):
                    if (log.get("eventDetails").get("name") == "view-doc"):
                        log["eventDetails"]["name"] = "view-doc-workspace"
                    if (log.get("eventDetails").get("name") == "get-doc"):
                        log["eventDetails"]["name"] = "get-doc-workspace"
                    if (log.get("eventDetails").get("name") == "doc-view-modal-close-clicked"):
                        log["eventDetails"]["name"] = "doc-view-modal-close-clicked-workspace"
                    if (log.get("eventDetails").get("name") == "doc-view-modal-close-top-clicked"):
                        log["eventDetails"]["name"] = "doc-view-modal-close-top-clicked-workspace"
                    if (log.get("eventDetails").get("name") == "facet-selected"):
                        log["eventDetails"]["name"] = "facet-selected-workspace"
                    if (log.get("eventDetails").get("name") == "facet-unselected"):
                        log["eventDetails"]["name"] = "facet-unselected-workspace"
                    if (log.get("eventDetails").get("name") == "facet-category-changed"):
                        log["eventDetails"]["name"] = "facet-category-changed-workspace"


finalLogs = []
## Log Frequency without wilson's classification
for user in json_data["logs"]:
    current_user_timing = timing.loc[timing['userID'] == user["userID"]]
    S1Count = 0;
    S2Count = 0;
    if(current_user_timing.empty == False):
        print(current_user_timing["name"].values[0] + "#############################################################")
        startTaskS1 = dp.parse(current_user_timing["s1_start_datetime"].values[0])
        endTaskS1 = dp.parse(current_user_timing["s1_end_datetime"].values[0])
        startTaskS2 = dp.parse(current_user_timing["s2_start_datetime"].values[0])
        endTaskS2 = dp.parse(current_user_timing["s2_end_datetime"].values[0])
        userLogs = {'userID': user["userID"],'userName': current_user_timing["name"].values[0], 'session1Logs': [], 'session2Logs': []}

        for log in user["logs"]:
            logTime = dp.parse(log["timestamps"]["eventTimestamp"])
            if (logTime >= startTaskS1 and logTime <= endTaskS1):
                userLogs["session1Logs"].append(log)
                S1Count+=1
            if (logTime >= startTaskS2 and logTime <= endTaskS2):
                userLogs["session2Logs"].append(log)
                S2Count+=1

        row = [0 for i in logfrequencyProtoS1["Event ID"]]
        row.insert(0, user["userID"])
        print(row)
        logfrequencyProtoS1ByUser.loc[len(logfrequencyProtoS1ByUser)] = row
        logfrequencyProtoS2ByUser.loc[len(logfrequencyProtoS2ByUser)] = row
        logfrequencyProtoS1ByUser.append(row)
        logfrequencyProtoS2ByUser.append(row)
        # logfrequencyProtoWilsonS2.loc[len(logfrequencyProtoWilsonS2)] = row
        for logs1 in userLogs["session1Logs"]:
            logfrequencyProtoS1Row = logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Event ID"]]
            if(logfrequencyProtoS1Row.empty==False):
                logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Event ID"]] = wilsonRow[logfrequencyProtoS1Row["Event ID"]]+1
                print(logs1.get("eventDetails").get("name"))

            else:
                if str(logs1.get("eventDetails").get("name")) not in logfrequencyProtoS1Unnamed:
                    logfrequencyProtoS1Unnamed.append(str(logs1.get("eventDetails").get("name")))
                    # print("S1 Unmatched " + str(logs1.get("eventDetails").get("name")))

        print("''''''''''''''''''''''''''''''''''''''''''''")
        for logs2 in userLogs["session2Logs"]:
            logfrequencyProtoS2Row = logfrequencyProtoS2.loc[
                logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS2ByUser.loc[
                logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Event ID"]]
            if (logfrequencyProtoS2Row.empty == False):
                logfrequencyProtoS2ByUser.loc[logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Event ID"]] = wilsonRow[logfrequencyProtoS2Row["Event ID"]] + 1
                print(logs2.get("eventDetails").get("name"))
            else:
                if str(logs2.get("eventDetails").get("name")) not in logfrequencyProtoS2Unnamed:
                    logfrequencyProtoS2Unnamed.append(str(logs2.get("eventDetails").get("name")))
                    # print("S2 Unmatched "+ str(logs2.get("eventDetails").get("name")))
logfrequencyProtoS1ByUser.to_csv("logfrequencyProtoS1ByUser.csv", encoding='utf-8', index=False)
logfrequencyProtoS2ByUser.to_csv("logfrequencyProtoS2ByUser.csv", encoding='utf-8', index=False)



## Log Frequency with wilson's classification
logfrequencyProtoS1ByUser = None
logfrequencyProtoS2ByUser = None

logfrequencyProtoWilsonS1 = logfrequencyProtoS1.drop_duplicates(subset=["Wilson's Classification"], keep="first")
logfrequencyProtoWilsonS2 = logfrequencyProtoS2.drop_duplicates(subset=["Wilson's Classification"], keep="first")

logfrequencyProtoS1ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoWilsonS1["Wilson's Classification"]])
logfrequencyProtoS1ByUser.insert(0, 'userID', '--')

logfrequencyProtoS2ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoWilsonS2["Wilson's Classification"]])
logfrequencyProtoS2ByUser.insert(0, 'userID', '--')

for user in json_data["logs"]:
    current_user_timing = timing.loc[timing['userID'] == user["userID"]]
    S1Count = 0;
    S2Count = 0;
    if(current_user_timing.empty == False):
        print(current_user_timing["name"].values[0] + "#############################################################")
        startTaskS1 = dp.parse(current_user_timing["s1_start_datetime"].values[0])
        endTaskS1 = dp.parse(current_user_timing["s1_end_datetime"].values[0])
        startTaskS2 = dp.parse(current_user_timing["s2_start_datetime"].values[0])
        endTaskS2 = dp.parse(current_user_timing["s2_end_datetime"].values[0])
        userLogs = {'userID': user["userID"],'userName': current_user_timing["name"].values[0], 'session1Logs': [], 'session2Logs': []}

        for log in user["logs"]:
            logTime = dp.parse(log["timestamps"]["eventTimestamp"])
            if (logTime >= startTaskS1 and logTime <= endTaskS1):
                userLogs["session1Logs"].append(log)
                S1Count+=1
            if (logTime >= startTaskS2 and logTime <= endTaskS2):
                userLogs["session2Logs"].append(log)
                S2Count+=1

        row = [0 for i in logfrequencyProtoWilsonS1["Wilson's Classification"]]
        row.insert(0, user["userID"])
        print(row)
        logfrequencyProtoS1ByUser.loc[len(logfrequencyProtoS1ByUser)] = row
        logfrequencyProtoS2ByUser.loc[len(logfrequencyProtoS2ByUser)] = row
        logfrequencyProtoS1ByUser.append(row)
        logfrequencyProtoS2ByUser.append(row)
        # logfrequencyProtoWilsonS2.loc[len(logfrequencyProtoWilsonS2)] = row
        for logs1 in userLogs["session1Logs"]:
            logfrequencyProtoS1Row = logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Wilson's Classification"]]
            if(logfrequencyProtoS1Row.empty==False):
                logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Wilson's Classification"]] = wilsonRow[logfrequencyProtoS1Row["Wilson's Classification"]]+1
                print(logs1.get("eventDetails").get("name"))
                print(logfrequencyProtoS1Row["Wilson's Classification"])
                print(logfrequencyProtoS1ByUser.loc[
                    logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row[
                        "Wilson's Classification"]])
                print("- - -")
            else:
                if str(logs1.get("eventDetails").get("name")) not in logfrequencyProtoS1Unnamed:
                    logfrequencyProtoS1Unnamed.append(str(logs1.get("eventDetails").get("name")))
                    # print("S1 Unmatched " + str(logs1.get("eventDetails").get("name")))

        print("''''''''''''''''''''''''''''''''''''''''''''")
        for logs2 in userLogs["session2Logs"]:
            logfrequencyProtoS2Row = logfrequencyProtoS2.loc[
                logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS2ByUser.loc[
                logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Wilson's Classification"]]
            if (logfrequencyProtoS2Row.empty == False):
                logfrequencyProtoS2ByUser.loc[logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Wilson's Classification"]] = wilsonRow[logfrequencyProtoS2Row["Wilson's Classification"]] + 1
                print(logs2.get("eventDetails").get("name"))
            else:
                if str(logs2.get("eventDetails").get("name")) not in logfrequencyProtoS2Unnamed:
                    logfrequencyProtoS2Unnamed.append(str(logs2.get("eventDetails").get("name")))
                    # print("S2 Unmatched "+ str(logs2.get("eventDetails").get("name")))
logfrequencyProtoS1ByUser.to_csv("logfrequencyWithWilsonProtoS1ByUser.csv", encoding='utf-8', index=False)
logfrequencyProtoS2ByUser.to_csv("logfrequencyWithWilsonProtoS2ByUser.csv", encoding='utf-8', index=False)





















