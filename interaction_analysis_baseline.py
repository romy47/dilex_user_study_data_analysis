import pandas as pd
import dateutil.parser as dp

# Initial FIle Read and Variable Setup ##########################################################################
json_data = pd.read_json('baselineloglists.json')
timing = pd.read_csv('user_log_timing_baseline.csv')
logfrequencyProtoS1 = pd.read_csv('log_frequency_baseline.csv')
logfrequencyProtoS1Unnamed = []
logfrequencyProtoS2 = pd.read_csv('log_frequency_baseline.csv')
logfrequencyProtoS2Unnamed = []

logfrequencyProtoS1 = logfrequencyProtoS1.drop_duplicates(subset=["Event ID"], keep="first")
logfrequencyProtoS2 = logfrequencyProtoS2.drop_duplicates(subset=["Event ID"], keep="first")

logfrequencyProtoS1ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoS1["Event ID"]])
logfrequencyProtoS1ByUser.insert(0, 'userID', '--')

logfrequencyProtoS2ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoS2["Event ID"]])
logfrequencyProtoS2ByUser.insert(0, 'userID', '--')

logfrequencyProtoS1["Count"] = logfrequencyProtoS1["Count"].fillna(0)
logfrequencyProtoS2["Count"] = logfrequencyProtoS2["Count"].fillna(0)

# Change Workspace interaction-id according to the new classification ##########################################################################
finalLogs = [];
for user in json_data["logs"]:
    currentState = 'SERP'
    index = 0
    for log in user["logs"]:
        log["index"] = index
        index += 1
        if(log.get("eventDetails").get("name") != None):
            if(log.get("eventDetails").get("name")=="myfolder-reached"):
                currentState = 'Workspace'
            elif(log.get("eventDetails").get("name") == "serp-reached"):
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

finalLogs = []
## Log Frequency without wilson's classification
for user in json_data["logs"]:
    current_user_timing = timing.loc[timing['userID'] == user["userID"]]
    S1Count = 0;
    S2Count = 0;
    if(current_user_timing.empty == False):
        # print(current_user_timing["name"].values[0] + "#############################################################")
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

        userLogs["session1Logs"].sort(key=lambda x: dp.parse(x.get("timestamps").get("eventTimestamp")))
        userLogs["session1Logs"].sort(key=lambda x: dp.parse(x.get("timestamps").get("eventTimestamp")))
        row = [0 for i in logfrequencyProtoS1["Event ID"]]
        row.insert(0, user["userID"])
        logfrequencyProtoS1ByUser.loc[len(logfrequencyProtoS1ByUser)] = row
        logfrequencyProtoS2ByUser.loc[len(logfrequencyProtoS2ByUser)] = row
        logfrequencyProtoS1ByUser.append(row)
        logfrequencyProtoS2ByUser.append(row)

        for logs1 in userLogs["session1Logs"]:
            logfrequencyProtoS1Row = logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Event ID"]]
            if(logfrequencyProtoS1Row.empty==False):
                logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Event ID"]] = wilsonRow[logfrequencyProtoS1Row["Event ID"]]+1
                # print(logs1.get("eventDetails").get("name"))
            else:
                if str(logs1.get("eventDetails").get("name")) not in logfrequencyProtoS1Unnamed:
                    logfrequencyProtoS1Unnamed.append(str(logs1.get("eventDetails").get("name")))
                    # print("S1 Unmatched " + str(logs1.get("eventDetails").get("name")))

        # print("''''''''''''''''''''''''''''''''''''''''''''")
        for logs2 in userLogs["session2Logs"]:
            logfrequencyProtoS2Row = logfrequencyProtoS2.loc[
                logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name"))]
            wilsonRow = logfrequencyProtoS2ByUser.loc[
                logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Event ID"]]
            if (logfrequencyProtoS2Row.empty == False):
                logfrequencyProtoS2ByUser.loc[logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                    "Event ID"]] = wilsonRow[logfrequencyProtoS2Row["Event ID"]] + 1
                # print(logs2.get("eventDetails").get("name"))
            else:
                if str(logs2.get("eventDetails").get("name")) not in logfrequencyProtoS2Unnamed:
                    logfrequencyProtoS2Unnamed.append(str(logs2.get("eventDetails").get("name")))
                    # print("S2 Unmatched "+ str(logs2.get("eventDetails").get("name")))
logfrequencyProtoS1ByUser.to_csv("logfrequencyProtoS1ByUser_baseline.csv", encoding='utf-8', index=False)
logfrequencyProtoS2ByUser.to_csv("logfrequencyProtoS2ByUser_baseline.csv", encoding='utf-8', index=False)



## Log Frequency with wilson's classification
logfrequencyProtoS1ByUser = None
logfrequencyProtoS2ByUser = None

logfrequencyProtoWilsonS1 = logfrequencyProtoS1.drop_duplicates(subset=["Wilson's Classification"], keep="first")
logfrequencyProtoWilsonS2 = logfrequencyProtoS2.drop_duplicates(subset=["Wilson's Classification"], keep="first")

logfrequencyProtoS1ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoWilsonS1["Wilson's Classification"]])
logfrequencyProtoS1ByUser.insert(0, 'userID', '--')

logfrequencyProtoS2ByUser = pd.DataFrame(columns=[str(i) for i in logfrequencyProtoWilsonS2["Wilson's Classification"]])
logfrequencyProtoS2ByUser.insert(0, 'userID', '--')


logfrequencyStatesS1ByUser = pd.DataFrame(columns=["serpTime","serpVisits","workspaceTime","workspaceVisits", "totalTime"])
logfrequencyStatesS1ByUser.insert(0, 'userID', '--')

logfrequencyStatesS2ByUser = pd.DataFrame(columns=["serpTime","serpVisits","workspaceTime","workspaceVisits", "totalTime"])
logfrequencyStatesS2ByUser.insert(0, 'userID', '--')


for user in json_data["logs"]:
        current_user_timing = timing.loc[timing['userID'] == user["userID"]]
        S1Count = 0;
        S2Count = 0;
        if(current_user_timing.empty == False):
            print(current_user_timing["name"].values[0] + "#  " + user["userID"] + "  ############################################################")
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

            userLogs["session1Logs"].sort(key=lambda x: dp.parse(x.get("timestamps").get("eventTimestamp")))
            userLogs["session1Logs"].sort(key=lambda x: dp.parse(x.get("timestamps").get("eventTimestamp")))

            row = [0 for i in logfrequencyProtoWilsonS1["Wilson's Classification"]]
            row.insert(0, user["userID"])
            # print(row)
            logfrequencyProtoS1ByUser.loc[len(logfrequencyProtoS1ByUser)] = row
            logfrequencyProtoS2ByUser.loc[len(logfrequencyProtoS2ByUser)] = row
            logfrequencyProtoS1ByUser.append(row)
            logfrequencyProtoS2ByUser.append(row)
            ## State and Timing calculation
            rowState = [user["userID"], 0, 0, 0, 0, 0]
            logfrequencyStatesS1ByUser.loc[len(logfrequencyStatesS1ByUser)] = rowState
            logfrequencyStatesS2ByUser.loc[len(logfrequencyStatesS2ByUser)] = rowState
            logfrequencyStatesS1ByUser.append(rowState)
            logfrequencyStatesS2ByUser.append(rowState)

            prevS1State="Empty"
            prevS2State="Empty"

            # For debugging
            prevS1Event = "Empty"
            prevS2Event = "Empty"

            if(len(userLogs["session1Logs"])>0):
                S1SerpTime = dp.parse((userLogs["session1Logs"][0].get("timestamps").get("eventTimestamp")))
                S1WorkTime = dp.parse((userLogs["session1Logs"][0].get("timestamps").get("eventTimestamp")))
                d1 = dp.parse(
                    userLogs["session1Logs"][len(userLogs["session1Logs"]) - 1].get("timestamps").get("eventTimestamp"))
                d2 = dp.parse(userLogs["session1Logs"][0].get("timestamps").get("eventTimestamp"))
                print('--s1--')
                print(d1)
                print(d2)
                s1TotalTime = (d1 - d2).total_seconds()
                print(s1TotalTime)

            if(len(userLogs["session2Logs"])>0):
                S2SerpTime = dp.parse((userLogs["session2Logs"][0].get("timestamps").get("eventTimestamp")))
                S2WorkTime = dp.parse((userLogs["session2Logs"][0].get("timestamps").get("eventTimestamp")))
                d1 = dp.parse(
                    userLogs["session2Logs"][len(userLogs["session2Logs"]) - 1].get("timestamps").get("eventTimestamp"))
                d2 = dp.parse(userLogs["session2Logs"][0].get("timestamps").get("eventTimestamp"))
                print('--s2--')
                print(d1)
                print(d2)
                s2TotalTime = (d1 - d2).total_seconds()
                print(s2TotalTime)

            if(len(userLogs["session1Logs"])>0):
                for logs1 in userLogs["session1Logs"]:
                    # for debugging
                    print(str(logs1.get("eventDetails").get("name")) + '-------' + str(
                        dp.parse((logs1.get("timestamps").get("eventTimestamp")))) + '----' + str(logs1["index"]))
                    if (logs1.get("eventDetails").get("name") != None and prevS1Event == logs1.get("eventDetails").get(
                            "name")):
                        print('>>>>>> ERR 1: Same Event')
                    if (logs1.get("eventDetails").get("name") != None and (
                            "issued" in logs1.get("eventDetails").get("name")) and
                            logs1.get("eventDetails").get("submissionValues") == None):
                        print('>>>>>> ERR 2: Empty Query')
                    prevS1Event = logs1.get("eventDetails").get("name")

                    logfrequencyProtoS1Row = logfrequencyProtoS1.loc[logfrequencyProtoS1["Event ID"] == str(logs1.get("eventDetails").get("name"))]
                    wilsonRow = logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Wilson's Classification"]]
                    # print(str(logs1.get("eventDetails").get("name")) + '     ' + str(logs1.get("timestamps").get("eventTimestamp")))
                    if(logfrequencyProtoS1Row.empty==False):
                        logfrequencyProtoS1ByUser.loc[logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row["Wilson's Classification"]] = wilsonRow[logfrequencyProtoS1Row["Wilson's Classification"]]+1
                        # print(logfrequencyProtoS1Row["Wilson's Classification"])
                        # print(logfrequencyProtoS1ByUser.loc[
                        #     logfrequencyProtoS1ByUser["userID"] == user["userID"], logfrequencyProtoS1Row[
                        #         "Wilson's Classification"]])
                        # print("- - -")
                    else:
                        if str(logs1.get("eventDetails").get("name")) not in logfrequencyProtoS1Unnamed:
                            logfrequencyProtoS1Unnamed.append(str(logs1.get("eventDetails").get("name")))
                            # print("S1 Unmatched " + str(logs1.get("eventDetails").get("name")))

                    #Timing and State Calculation
                    # if(user["userID"]=="615e605553c939867a1e4ba2"):
                    # print("--")
                    # print(logs1.get("state"))
                    if(logs1.get("state") and logs1.get("state")!=prevS1State):
                        if(logs1.get("state")=="Workspace"):
                            # print('* w -block')
                            logfrequencyStatesS1ByUser.loc[logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceVisits"] = logfrequencyStatesS1ByUser.loc[logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceVisits"] + 1
                            S1WorkTime = dp.parse((logs1.get("timestamps").get("eventTimestamp")))
                            logfrequencyStatesS1ByUser.loc[
                                logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpTime"] = logfrequencyStatesS1ByUser.loc[
                                logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpTime"] + (S1WorkTime-S1SerpTime).total_seconds()
                        elif(logs1.get("state")=="SERP"):
                            # print('# s -block')
                            logfrequencyStatesS1ByUser.loc[logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpVisits"] = logfrequencyStatesS1ByUser.loc[logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpVisits"] + 1
                            S1SerpTime = dp.parse((logs1.get("timestamps").get("eventTimestamp")))
                            logfrequencyStatesS1ByUser.loc[
                                logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceTime"] = logfrequencyStatesS1ByUser.loc[
                                logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceTime"] + (
                                S1SerpTime - S1WorkTime).total_seconds()
                        prevS1State = logs1.get("state")

                lastLogTime = dp.parse((userLogs["session1Logs"][len(userLogs["session1Logs"])-1].get("timestamps").get("eventTimestamp")))

                if(prevS1State=="Workspace"):
                    logfrequencyStatesS1ByUser.loc[
                        logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceTime"] = logfrequencyStatesS1ByUser.loc[
                        logfrequencyStatesS1ByUser["userID"] == user["userID"], "workspaceTime"] + (
                            lastLogTime - S1WorkTime).total_seconds()
                elif(prevS1State=="SERP"):
                    logfrequencyStatesS1ByUser.loc[
                        logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpTime"] = logfrequencyStatesS1ByUser.loc[
                        logfrequencyStatesS1ByUser["userID"] == user["userID"], "serpTime"] + (
                            lastLogTime - S1SerpTime).total_seconds()
            print("''''''''''''''''''''''''''''''''''''''''''''")
            if(len(userLogs["session2Logs"])>0):
                for logs2 in userLogs["session2Logs"]:
                    # for debugging
                    print(str(logs2.get("eventDetails").get("name")) + '-------' + str(
                        dp.parse((logs2.get("timestamps").get("eventTimestamp")))) + '----' + str(logs2["index"]))
                    if (logs2.get("eventDetails").get("name") != None and prevS2Event == logs2.get("eventDetails").get(
                            "name")):
                        print('>>>>>> ERR 1: Same Event')
                    if (logs2.get("eventDetails").get("name") != None and (
                            "issued" in logs2.get("eventDetails").get("name")) and
                        logs2.get("eventDetails").get("submissionValues") == None):
                        print('>>>>>> ERR 2: Empty Query')
                    prevS2Event = logs2.get("eventDetails").get("name")

                    logfrequencyProtoS2Row = logfrequencyProtoS2.loc[
                        logfrequencyProtoS2["Event ID"] == str(logs2.get("eventDetails").get("name"))]
                    wilsonRow = logfrequencyProtoS2ByUser.loc[
                        logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                            "Wilson's Classification"]]
                    if (logfrequencyProtoS2Row.empty == False):
                        logfrequencyProtoS2ByUser.loc[logfrequencyProtoS2ByUser["userID"] == user["userID"], logfrequencyProtoS2Row[
                            "Wilson's Classification"]] = wilsonRow[logfrequencyProtoS2Row["Wilson's Classification"]] + 1
                        # print(logs2.get("eventDetails").get("name"))
                    else:
                        if str(logs2.get("eventDetails").get("name")) not in logfrequencyProtoS2Unnamed:
                            logfrequencyProtoS2Unnamed.append(str(logs2.get("eventDetails").get("name")))
                            # print("S2 Unmatched "+ str(logs2.get("eventDetails").get("name")))
                    # print(str(logs2.get("eventDetails").get("name")) + '-------' + str(dp.parse((logs2.get("timestamps").get("eventTimestamp")))))
                    if (logs2.get("state") and logs2.get("state") != prevS2State):
                        if (logs2.get("state") == "Workspace"):
                            # print('* w -block')
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceVisits"] = \
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceVisits"] + 1
                            S2WorkTime = dp.parse((logs2.get("timestamps").get("eventTimestamp")))
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpTime"] = \
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpTime"] + (
                                        S2WorkTime - S2SerpTime).total_seconds()
                        elif (logs2.get("state") == "SERP"):
                            # print('# s -block')
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpVisits"] = \
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpVisits"] + 1
                            S2SerpTime = dp.parse((logs2.get("timestamps").get("eventTimestamp")))
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceTime"] = \
                            logfrequencyStatesS2ByUser.loc[
                                logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceTime"] + (
                                    S2SerpTime - S2WorkTime).total_seconds()
                        prevS2State = logs2.get("state")
                lastLogTime = dp.parse(
                    (userLogs["session2Logs"][len(userLogs["session2Logs"]) - 1].get("timestamps").get("eventTimestamp")))

                if (prevS2State == "Workspace"):
                    logfrequencyStatesS2ByUser.loc[
                        logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceTime"] = \
                    logfrequencyStatesS2ByUser.loc[
                        logfrequencyStatesS2ByUser["userID"] == user["userID"], "workspaceTime"] + (
                            lastLogTime - S2WorkTime).total_seconds()
                elif (prevS2State == "SERP"):
                    logfrequencyStatesS2ByUser.loc[
                        logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpTime"] = \
                    logfrequencyStatesS2ByUser.loc[
                        logfrequencyStatesS2ByUser["userID"] == user["userID"], "serpTime"] + (
                            lastLogTime - S2SerpTime).total_seconds()
                logfrequencyStatesS1ByUser.loc[logfrequencyStatesS1ByUser["userID"] == user["userID"], "totalTime"] = s1TotalTime
                logfrequencyStatesS2ByUser.loc[logfrequencyStatesS2ByUser["userID"] == user["userID"], "totalTime"] = s2TotalTime

logfrequencyProtoS1ByUser.to_csv("logfrequencyWithWilsonProtoS1ByUser_baseline.csv", encoding='utf-8', index=False)
logfrequencyProtoS2ByUser.to_csv("logfrequencyWithWilsonProtoS2ByUser_baseline.csv", encoding='utf-8', index=False)

logfrequencyStatesS1ByUser.to_csv("logfrequencyStatesS1ByUser_baseline.csv", encoding='utf-8', index=False)
logfrequencyStatesS2ByUser.to_csv("logfrequencyStatesS2ByUser_baseline.csv", encoding='utf-8', index=False)






















