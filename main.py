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
    if(user["userID"]=="610efac0fb24c5db2f10cf7e"):
        print('************************************************************************')
        print(user["userID"])
        S1Count = 0
        for log in user["logs"]:
            # if(log.get("eventDetails").get("name")=="search-query-issued"):
            # if(log.get("eventDetails").get("name")=="task-created"):
            # if(log.get("eventDetails").get("name")=="serp-reached"):
            if (log.get("eventDetails").get("name") == "user-signed-out"):
                print("S1 Count: ", str(S1Count))
                print(log.get("eventDetails").get("name"))
                print('-------------')
            S1Count +=1
print()
