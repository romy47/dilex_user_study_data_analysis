import pymongo
from pymongo import MongoClient
import time
import pandas as pd
import dateutil.parser as dp
import json

# with open('loglists.json',encoding="utf8") as fd, open ("user_log_timing.csv") as tm:
json_data = pd.read_json('loglists.json')
timing = pd.read_csv('user_log_timing.csv')
logfrequencyProtoS1 = pd.read_csv('log_frequency_proto.csv')
logfrequencyProtoS1Unnamed = []
logfrequencyProtoS2 = pd.read_csv('log_frequency_proto.csv')
logfrequencyProtoS2Unnamed = []

logfrequencyProtoS1 = logfrequencyProtoS1.drop_duplicates(subset=['Event ID'], keep='first')
logfrequencyProtoS2 = logfrequencyProtoS2.drop_duplicates(subset=['Event ID'], keep='first')
# for logEvent in logfrequencyProtoS1:
#     print(logEvent)
#     logEvent.at["Count"] = 0
logfrequencyProtoS1["Count"] = logfrequencyProtoS1["Count"].fillna(0)
logfrequencyProtoS2["Count"] = logfrequencyProtoS2["Count"].fillna(0)




finalLogs = [];
# json_data = json.load(fd)
for user in json_data["logs"]:
    current_user_timing = timing.loc[timing['userID'] == user["userID"]]
    S1Count = 0;
    S2Count = 0;
    if(current_user_timing.empty == False):
        print(current_user_timing["name"].values[0])
        startTaskS1 = dp.parse(current_user_timing["s1_start_datetime"].values[0])
        endTaskS1 = dp.parse(current_user_timing["s1_end_datetime"].values[0])
        startTaskS2 = dp.parse(current_user_timing["s2_start_datetime"].values[0])
        endTaskS2 = dp.parse(current_user_timing["s2_end_datetime"].values[0])
        userLogs = {'userID': user["userID"],'userName': current_user_timing["name"].values[0], 'session1Logs': [], 'session2Logs': []}

        for log in user["logs"]:
            logTime = dp.parse(log["timestamps"]["eventTimestamp"])
            # print(startTaskS1,endTaskS1,startTaskS2,endTaskS2 )
            if (logTime >= startTaskS1 and logTime <= endTaskS1):
                userLogs["session1Logs"].append(log)
                print('S1  ', log.get("eventDetails").get("name"))
                S1Count+=1
            if (logTime >= startTaskS2 and logTime <= endTaskS2):
                userLogs["session2Logs"].append(log)
                print('S2  ', log.get("eventDetails").get("name"))
                S2Count+=1
        finalLogs.append(userLogs)
    print("S1 Count: ", str(S1Count), "  S2 Count: ", str(S2Count))
    print('*****************************************************************')
    print()
# logs=logfrequencyProtoS1.pivot_table(columns='Event ID',fill_value='--')
# print(logfrequencyProtoS1)
# logfrequencyProtoS1.to_csv('test.csv', sep='\t')

for log in finalLogs:
    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    print(log.get("userName"))
    print(len(log["session1Logs"]))
    print(len(log["session2Logs"]))
    print("------------------------ Session 1----------------------------------------------------")
    for logs1 in log["session1Logs"]:
        logfrequencyProtoS1Row = logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name"))]
        if(logfrequencyProtoS1Row.empty==False):
            logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name")), "Count"] = logfrequencyProtoS1Row["Count"]+1
        else:
            if str(logs1.get("eventDetails").get("name")) not in logfrequencyProtoS1Unnamed:
                logfrequencyProtoS1Unnamed.append(str(logs1.get("eventDetails").get("name")))
        # print (str(logs1.get("eventDetails").get("name")))
    print("------------------------ Session 2----------------------------------------------------")
    for logs2 in log["session2Logs"]:
        logfrequencyProtoS2Row = logfrequencyProtoS2.loc[logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name"))]
        if (logfrequencyProtoS2Row.empty == False):
            logfrequencyProtoS2.loc[
                logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name")), "Count"] = \
            logfrequencyProtoS2Row["Count"] + 1
        else:
            if str(logs2.get("eventDetails").get("name")) not in logfrequencyProtoS2Unnamed:
                logfrequencyProtoS2Unnamed.append(str(logs2.get("eventDetails").get("name")))
        # print(str(logs2.get("eventDetails").get("name")))
    print("\n")
    print("\n")
    print("\n")
    print("\n")
logfrequencyProtoS1.to_csv("logfrequencyProtoS1.csv", encoding='utf-8', index=False)
logfrequencyProtoS2.to_csv("logfrequencyProtoS2.csv", encoding='utf-8', index=False)
# print(logfrequencyProtoS1)
# for index,s1log in logfrequencyProtoS1.iterrows():
#     print(s1log['Event ID'], s1log['Count'])
#
# for index, s2log in logfrequencyProtoS1.iterrows():
#     print(s1log['Event ID'], s1log['Count'])




    # print(s1log["Event ID"].values[0] + '  ' + s1log["Count"].values[0])
# for s2log in logfrequencyProtoS2:
#     print(s2log["Event ID"].values[0] + '  ' + s2log["Count"].values[0])


#
print(logfrequencyProtoS1Unnamed)
print(logfrequencyProtoS2Unnamed)




    # for logs2 in userLogs["session2Logs"]:
    #     print (logs2.get("eventDetails").type + '   ' + logs2.get("eventDetails").name)
























# cluster = MongoClient("mongodb+srv://dilex:xperia88@cluster0.pgfiy.mongodb.net/DataAnalysis?retryWrites=true&w=majority");
# db = cluster["DataAnalysis"]
# collection = db["loglists"]
# results = collection.find()
# results = results.sort("timestamps.eventTimestamp",-1)
#
# finalLogs = [];
# for user in results:
#     userLogs = {'userID':user["userID"], 'session1Logs':[],'session2Logs':[]}
#     for log in user["logs"]:
#         logTime = dp.parse(log["timestamps"]["eventTimestamp"])
#         startTask = dp.parse("2021-07-31T17:16:16.902Z")
#         if(logTime>=startTask):
#             userLogs["session1Logs"].append(log)
#     finalLogs.append(userLogs)


# with open('loglists.json',encoding="utf8") as fd:
#     finalLogs = [];
#     json_data = json.load(fd)
#     for user in json_data["logs"]:
#         if(user["userID"]=="6160ef6653c939867a1e8596"):
#             userLogs = {'userID': user["userID"], 'session1Logs': [], 'session2Logs': []}
#             index = 0;
#             for log in user["logs"]:
#                 index+=1
#                 logTime = dp.parse(log["timestamps"]["eventTimestamp"])
#                 startTask = dp.parse("2021-07-31T17:16:16.902Z")
#                 if(log.get("eventDetails")):
#                     event = log.get("eventDetails")
#                     name = event.get("name")
#                     if(name=="task-clicked"):
#                         print(name + '  ' + str(index) + '  ' + str(logTime.date()) +' - '+str(logTime.time()))
#                 if (logTime >= startTask):
#                     # print(log)
#                     userLogs["session1Logs"].append(log)
#             finalLogs.append(userLogs)
#
#     for log in finalLogs:
#         print(log.get("eventDetails"))

