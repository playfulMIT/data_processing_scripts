from datetime import datetime
import json
import numpy as np
import pandas as pd
from sklearn import metrics
import math
import csv
import os
import statistics
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# USAGE EXAMPLE
#path = '/Users/pedroantonio/Desktop/TFG/notebooks/anonamyze_all_data_collection.csv'
#dataEvents = pd.read_csv(path, sep=";")


student_id = 'user'
timestamp = 'initial timestamp'
student_column_number = 1
group_column_number = 0
completed = 'n_completed'
puzzle_name = 'task_id'
puzzle_column_number = 2
kc_column = 'kc'
kc_column_number = 4

kcs = ['GMD.4', 'CO.5', 'CO.6','MG.1']
mg1Puzzles = ['Bird Fez', 'Pi Henge', 'Bull Market']
gmd4Puzzles = ['Angled Silhouettes', 'Not Bird', 'Stranger Shapes', 'Ramp Up and Can It', 'Few Clues']
co5Puzzles = ['45-Degree Rotations', 'Boxes Obscure Spheres', 'More Than Meets the Eye']
co6Puzzles = ['Tall and Small', 'Not Bird', 'Ramp Up and Can It', 'Stretch a Ramp', 'Max 2 Boxes']

typeMappingDifficulty = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial',
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

tutorialPuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Tutorial'):
        tutorialPuzzles.append(desc[0])
        
advancedPuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Hard Puzzles'):
        advancedPuzzles.append(desc[0])
        
        
intermediatePuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Easy Puzzles'):
        intermediatePuzzles.append(desc[0])

# mapping to positions

typeMappingKC = {'Sandbox': 'GMD.4~CO.5~CO.6', '1. One Box': 'GMD.4~CO.5~CO.6', '2. Separated Boxes': 'GMD.4~CO.5~CO.6', '3. Rotate a Pyramid': 'GMD.4~CO.5~CO.6', '4. Match Silhouettes': 'GMD.4~CO.5~CO.6', '5. Removing Objects': 'GMD.4~CO.5~CO.6', '6. Stretch a Ramp': 'GMD.4~CO.5~CO.6', '7. Max 2 Boxes': 'GMD.4~CO.5~CO.6', '8. Combine 2 Ramps': 'GMD.4~CO.5~CO.6', '9. Scaling Round Objects': 'GMD.4~CO.5~CO.6',
               'Square Cross-Sections': 'GMD.4~CO.5~CO.6', 'Bird Fez': 'MG.1~GMD.4~CO.5~CO.6' , 'Pi Henge': 'MG.1~GMD.4~CO.5~CO.6', '45-Degree Rotations': 'GMD.4~CO.5~CO.6',  'Pyramids are Strange': 'GMD.4~CO.5~CO.6', 'Boxes Obscure Spheres': 'GMD.4~CO.5~CO.6', 'Object Limits': 'GMD.4~CO.5~CO.6', 'Tetromino': 'GMD.4~CO.5~CO.6', 'Angled Silhouette': 'GMD.4~CO.5~CO.6',
               'Warm Up':'GMD.4~CO.5~CO.6','Sugar Cones': 'GMD.4~CO.5~CO.6', 'Stranger Shapes': 'GMD.4~CO.5~CO.6', 'Tall and Small': 'GMD.4~CO.5~CO.6', 'Ramp Up and Can It': 'GMD.4~CO.5~CO.6', 'More Than Meets Your Eye': 'GMD.4~CO.5~CO.6', 'Not Bird': 'GMD.4~CO.5~CO.6', 'Unnecessary': 'GMD.4~CO.5~CO.6', 'Zzz': 'GMD.4~CO.5~CO.6', 'Bull Market': 'MG.1~GMD.4~CO.5~CO.6', 'Few Clues': 'GMD.4~CO.5~CO.6', 'Orange Dance': 'GMD.4~CO.5~CO.6', 'Bear Market': 'GMD.4~CO.5~CO.6'}




def adaptedData(dataEvents, group = 'all'):
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')
    
    #iterates in the groups and users of the data
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['task_id'] = [json.loads(x)['task_id'] if 'task_id' in json.loads(x).keys() else '' for x in dataEvents['data']]
    
    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']
    dataEvents['group_user_task_id'] = dataEvents['group'] + '~' + dataEvents['user']+'~'+dataEvents['task_id']

         
    # filtering to only take the group passed as argument
          
    activity_by_user = dataEvents.groupby(['group_user_id']).agg({'id':'count',
                                             'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                              'type':'different_events'})
    
    
                                                                                              
    #initialize the metrics
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_completed'] = 0
    activity_by_user['kc'] = ''
    #initialize the data structures
    puzzleEvents = dict()
    timePuzzle = dict()
    puzzCom= dict()
    puzzDestr = dict()
    initialTime = dict()
    
    n_attempts = dict()
    attData = dict()
    
    userPuzzleInit = dict()
    n_attemptsAux = dict()
    
    userTrain = set()
    userTest = set()
    userTotal = set()
    
    
    for user in dataEvents['group_user_id'].unique():
        
        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []
        
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None

        for enum, event in user_events.iterrows():
            
            if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):
                
                if(json.loads(event['data'])['task_id'] == 'Sandbox'): continue
                
                partialKey = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']
                
                if(event['user'] not in userTotal):
                    userTotal.add(event['user'])

                
                if(partialKey not in n_attemptsAux.keys()):
                    n_attemptsAux[partialKey] = 0
                    puzzCom[partialKey] = 0
                    
                    
                if(partialKey not in userPuzzleInit.keys()):
                    
                    n_attempts[partialKey] = 1
                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id'] + '~' + str(n_attempts[partialKey])
                    userPuzzleInit[partialKey] = 1
                    
                else:
                    
                    n_attempts[partialKey] += 1
                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id'] + '~' + str(n_attempts[partialKey])
                    
            
                # initialize if the id is new
                if(user_puzzle_key not in puzzleEvents.keys()):
                    attData[user_puzzle_key] = {'att': 0, 'completed': 0,'dataCompleted': 0, 'accept': 0, 'timestamp': event['time'], 'repeat':0}
                    puzzleEvents[user_puzzle_key]= 1
                    timePuzzle[user_puzzle_key] = 0
                    puzzDestr[user_puzzle_key] = ''
                    initialTime[user_puzzle_key] = 0
                                        
                    
                if(event['type'] in ['ws-puzzle_started']):
                    attData[user_puzzle_key]['timestamp'] = event['time']
                    
            # the event is not final event
            if(event['type'] not in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-create_user', 'ws-login_user']):
                if(user_puzzle_key in puzzleEvents.keys()):
                    puzzleEvents[user_puzzle_key] += 1
                    splitDes = user_puzzle_key.split("~")
                    puzzDestr[user_puzzle_key] = typeMappingKC[splitDes[2]]
                    if(event['type'] == 'ws-check_solution'):
                        attData[user_puzzle_key]['accept'] = 1
                        
                        
                       
                        
            # the puzzle ends
            if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-disconnect']):
                
                if(user_puzzle_key in puzzleEvents.keys()):
                    #the data is consistent
                    attData[user_puzzle_key]['dataCompleted'] += 1
                    #the data is valid
                    if(attData[user_puzzle_key]['accept'] == 1 and attData[user_puzzle_key]['dataCompleted']==1):
                        n_attemptsAux[partialKey]+=1
                        attData[user_puzzle_key]['att'] = n_attemptsAux[partialKey]
                        #attempt after solving
                        if(event['type'] in ['ws-puzzle_complete']):
                            if(puzzCom[partialKey] !=0 and n_attemptsAux[partialKey] > 1):
                                attData[user_puzzle_key]['repeat'] = 1
                    
                    if(event['type'] in ['ws-puzzle_complete']):
                        if(puzzCom[partialKey] ==0):
                            attData[user_puzzle_key]['completed'] = 1
                            if(attData[user_puzzle_key]['accept'] == 1):
                                puzzCom[partialKey] +=1

                    
    
    
    # add the data by group_user_task_id
    for i in attData.keys():
        key_split = i.split('~')
            
        if(len(userTrain) < round(len(userTotal)*0.7)):
            userTrain.add(key_split[1])
        else:
            if(key_split[1] not in userTrain): userTest.add(key_split[1])
                
                
        
        if(key_split[2] != '' and key_split[2] != 'Sandbox' and key_split[3] != '' and i != '' and key_split[1] != ''):
            if(attData[i]['accept'] != 0 and attData[i]['dataCompleted'] != 0 and attData[i]['repeat'] == 0):
               
                # data output preparation
                activity_by_user.at[i, 'group_user_task_att'] = key_split[0] + '~' + key_split[1] + '~' + key_split[2] + '~' + str(attData[i]['att'])
                activity_by_user.at[i, 'group'] = key_split[0]
                activity_by_user.at[i, 'user'] = key_split[1]
                activity_by_user.at[i, 'task_id'] = key_split[2]
                activity_by_user.at[i, 'attempt'] = attData[i]['att']
                activity_by_user.at[i, 'repeat'] = attData[i]['repeat']
                activity_by_user.at[i, 'kc'] = puzzDestr[i]
                activity_by_user.at[i, 'n_completed'] = attData[i]['completed']
                activity_by_user.at[i, 'initial timestamp'] = attData[i]['timestamp']

    
    #delete row with NaN
    activity_by_user.dropna(subset = ['user'], inplace=True)
  
    #data output preparation
    activity_by_user = pd.DataFrame(activity_by_user, columns = ['group_user_task_att', 'group','user','task_id','n_completed', 'kc', 'initial timestamp'])


    train = activity_by_user[activity_by_user['user'].isin(userTrain)]
    test = activity_by_user[activity_by_user['user'].isin(userTest)]
    
    return activity_by_user, train, test


# Dict users: uDict
def usersDict(datafile):
    csv_file = datafile
    mapUsers = {}
    mapGroups = {}
    cont =0
    for row in csv_file.iterrows():
        user = row[1]['user']
        group = row[1]['group']
        if user not in mapUsers.keys():
            mapUsers[user]=cont
            mapGroups[user] = group
            cont = cont+1
    return mapUsers, mapGroups


# Dict puzzles: qDict
def puzzlesDict(datafile):
    csv_file = datafile
    mapPuzzles = {}
    cont =0
    for row in csv_file.iterrows():
        question = row[1]['task_id']
        if question not in mapPuzzles.keys():
            mapPuzzles[question]=cont
            cont = cont+1
    return mapPuzzles



# Dict kcs: kcDict
def kcsDict(datafile):
    QT = []
    csv_file = datafile
    mapKc = {}
    cont =0
    for row in csv_file.iterrows():
        tags = row[1]['kc']
        if tags:
            tag = tags.split("~")
            for topics in tag:
                if topics not in mapKc.keys():
                    mapKc[topics]=cont
                    cont = cont + 1
    return mapKc

def createKcDict(datafile):
    
    QTMat = dict()
    csv_file = datafile
    for row in csv_file.iterrows():
        qid = row[1]['task_id']
        kcs = row[1]['kc']
        if(qid not in QTMat.keys()):
            QTMat[qid]=dict()
        if kcs:
            kc = kcs.split("~")
            for k in kc:
                QTMat[qid][k] =0


    for puzzle in QTMat.keys():
        tam = len(QTMat[puzzle])
        if tam>0:
            if(puzzle in mg1Puzzles):
                QTMat[puzzle]['MG.1'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'MG.1'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in gmd4Puzzles):
                QTMat[puzzle]['GMD.4'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'GMD.4'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in co5Puzzles):
                QTMat[puzzle]['CO.5'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'CO.5'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in co6Puzzles):
                QTMat[puzzle]['CO.6'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'CO.6'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            else:
                for x in QTMat[puzzle].keys():
                    QTMat[puzzle][x] = 1/tam
    return QTMat


def loadDataset(datafile):
    uDict, gDict = usersDict(datafile)
    qDict =puzzlesDict(datafile)
    kcDict =kcsDict(datafile)
    kcsPuzzleDict =  createKcDict(datafile)

    return uDict, gDict,qDict,kcDict, kcsPuzzleDict



def rmseFunction(prob, ans, lenProb):
    prob = np.array(prob)
    ground = np.array(ans)
    error = (prob - ans)
    err_sqr = error*error
    rmse = math.sqrt(err_sqr.sum()/lenProb)
    return rmse



def accuracyFunction(ans, prob):
    ans = np.array(ans)
    prob = np.array(prob)
    prob[prob >= 0.5] = 1
    prob[prob < 0.5] = 0
    acc = metrics.accuracy_score(ans, prob)
    return acc

def get_cohenKappa(y, pred):
    y = np.array(y)
    pred = np.array(pred)
    pred[pred >= 0.5] = 1
    pred[pred < 0.5] = 0
    cohenKappa = metrics.cohen_kappa_score(y, pred, labels=None, weights=None, sample_weight=None)
    return cohenKappa

def auc_roc(y, pred):
    y = np.array(y)
    pred = np.array(pred)
    fpr, tpr, thresholds = metrics.roc_curve(y, pred, pos_label=1)
    auc = metrics.auc(fpr, tpr)
    return auc




def normalized_PCA(array):
    minPCA = round(np.nanmin(array),3)
    maxPCA = round(np.nanmax(array),3)
    array2=[]
    for i in range(len(array)):
        array2.append((array[i] - minPCA) / (maxPCA-minPCA))
    
    return array2

# First stage function: difficulty
def arrayDifficulty(inputData, Competency, Diff, A_count, Q_count, kcsPuzzleDict ,gDict,gamma, beta):

    alpha = 1
    alpha_denominator = 0
    correct = 0
    
    arrayDiff = dict()

    response = np.zeros((len(inputData), 1))
    
    for count, (index, item) in enumerate(inputData.iterrows()):
            
        alpha_denominator = 0
        uid = item[student_id]
        qid = item[puzzle_name]
        
        ## NEW ##
        if(qid not in arrayDiff.keys()): arrayDiff[qid] = dict()
        
        diff = dict()
        diff[qid]=[]
        comp= dict()
        comp[uid]=[]
        
        # The student's current competence by component is multiplied by each component of the question he or she is facing.
        for k in kcsPuzzleDict[qid]:
            comp[uid].append(Competency[uid][k] * kcsPuzzleDict[qid][k])
            diff[qid].append(Diff[qid][k] * kcsPuzzleDict[qid][k])
            
        # Adding up the competencies per component to obtain the global competence
        compTotal = np.sum(comp[uid])
        diffTotal = np.sum(diff[qid])
        
        
        # With the global competition and the difficulty of the question, the probability of solving it is calculated
        probability = (1)/(1 + math.exp( -1 * (compTotal - diffTotal)))
        
        q_answered_count = Q_count[qid]
        
        # The puzzle is completed or no
        if item[completed] == 1:

            response[count] = 1
            correct = 1
        else:
            response[count] = 0
            correct = 0
        

        #Alpha component is calculated (normalization factor)
        alpha_numerator = probability - correct
        for k in kcsPuzzleDict[qid]:
            c_lambda = Competency[uid][k]
            probability_lambda = (1)/(1 + math.exp( -1 * (c_lambda - Diff[qid][k])))
            alpha_denominator = alpha_denominator + (correct - probability_lambda)
        alpha = abs(alpha_numerator / alpha_denominator)

        
        Q_count[qid] += 1
        A_count[uid] += 1
        
        for k in kcsPuzzleDict[qid]:
            
            u_answered_count = A_count[uid]
            prevDiff = Diff[qid][k]
                        
            # Competency probability is calculated
            probability = (1)/(1 + math.exp( -1 * (Competency[uid][k] - prevDiff)))
            
            # Update the difficulty
            changeDiff = ((gamma)/(1 + beta * q_answered_count)) *alpha* (probability - correct)
            Diff[qid][k] = Diff[qid][k] + kcsPuzzleDict[qid][k] * changeDiff
            # Add difficulty
            if(k not in arrayDiff[qid].keys()): arrayDiff[qid][k] = []
            arrayDiff[qid][k].append(Diff[qid][k])
            
            # Update the competency
            changeComp = kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability)
            Competency[uid][k] = Competency[uid][k]+changeComp
                            
                
    return arrayDiff


# ELO algorithm with static difficulty
def multiTopic_ELO(inputData, Competency, Diff, A_count, Q_count, kcsPuzzleDict ,gDict,gamma, beta):

    alpha = 1
    alpha_denominator = 0
    correct = 0
    prob_test = dict()
    ans_test = dict()
    probUser = dict()
    competencyPartial = dict()
    userPuzzles = dict()
    completedPartialData = dict()
    
    failAtt = dict()
    
    probUserTest = dict()
    ansUserTest = dict()
    
    contPuzzlesUser = dict()

    response = np.zeros((len(inputData), 1))
    
    for count, (index, item) in enumerate(inputData.iterrows()):
            
        alpha_denominator = 0
        uid = item[student_id]
        qid = item[puzzle_name]
        time = item[timestamp]
        
        if(uid not in failAtt.keys()):
            failAtt[uid]= dict()
        if(qid not in failAtt[uid].keys()):
            failAtt[uid][qid] = 0
        
        if(uid not in userPuzzles.keys()): userPuzzles[uid] = []
        userPuzzles[uid].append(qid)
        
        # Cont the puzzles per user (intermediate and advanced)
        if(uid not in contPuzzlesUser.keys()):
            contPuzzlesUser[uid] = set()
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            contPuzzlesUser[uid].add(qid)
        
        diff = dict()
        diff[qid]=[]
        comp= dict()
        comp[uid]=[]
        
        # The student's current competence by component is multiplied by each component of the question he or she is facing.
        for k in kcsPuzzleDict[qid]:
            comp[uid].append(Competency[uid][k] * kcsPuzzleDict[qid][k])
            diff[qid].append(Diff[qid][k] * kcsPuzzleDict[qid][k])
            
        # Adding up the competencies per component to obtain the global competence
        compTotal = np.sum(comp[uid])
        diffTotal = np.sum(diff[qid])
        
        # With the global competition and the difficulty of the question, the probability of solving it is calculated
        probability = (1)/(1 + math.exp( -1 * (compTotal - diffTotal)))
        
        if(uid not in prob_test.keys()):
            prob_test[uid] = dict()
            
        if(uid not in probUserTest.keys()):
            probUserTest[uid] = []
            
        if(uid not in ansUserTest.keys()):
            ansUserTest[uid] = []
        
        # Save the probabilities
        prob_test[uid][qid]=probability
        q_answered_count = Q_count[qid]
        
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            probUserTest[uid].append(probability)
        
        # The puzzle is completed or no
        if item[completed] == 1:

            response[count] = 1
            correct = 1
        else:
            response[count] = 0
            correct = 0
            failAtt[uid][qid] +=1
        
        if(uid not in ans_test.keys()):
            ans_test[uid] = dict()
            
        # Save the real result
        ans_test[uid][qid] = correct
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            ansUserTest[uid].append(correct)

        #Alpha component is calculated (normalization factor)
        alpha_numerator = probability - correct
        for k in kcsPuzzleDict[qid]:
            c_lambda = Competency[uid][k]
            probability_lambda = (1)/(1 + math.exp( -1 * (c_lambda - Diff[qid][k])))
            alpha_denominator = alpha_denominator + (correct - probability_lambda)
        alpha = abs(alpha_numerator / alpha_denominator)

        # Initialize new data
        if(uid not in probUser.keys()):
            probUser[uid] = dict()
            competencyPartial[uid] = dict()
        
        probUser[uid][qid]= probability
        
        Q_count[qid] += 1
        A_count[uid] += 1
        for k in kcsPuzzleDict[qid]:
            
            u_answered_count = A_count[uid]
            c = Competency[uid][k]
            prevDiff = Diff[qid][k]
            
            key = uid+'~'+qid+'~'+k+'~'+str(round(Competency[uid][k],3)) + '~'+str(round(prevDiff,3))
            
            # Competency probability is calculated
            probability = (1)/(1 + math.exp( -1 * (Competency[uid][k] - prevDiff)))
            
            # Update the difficulty
            #changeDiff = ((gamma)/(1 + beta * q_answered_count)) *alpha* (probability - correct)
            #Diff[qid][k] = Diff[qid][k] + kcsPuzzleDict[qid][k] * changeDiff
            
            # Update the competency
            # if puzzle is in tutorial puzzles, we do not update the competency
            weightAtt = 0
            if(qid not in tutorialPuzzles and correct ==1):
                # Fail limit
                if(failAtt[uid][qid] >= 5): failAtt[uid][qid] == 5
                    
                weightAtt = (1-(failAtt[uid][qid]/10))
                complete_change = kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability)
                changeComp = kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability) * weightAtt
                Competency[uid][k] = Competency[uid][k]+changeComp
                
            else:
                
                changeComp = 0
                complete_change = 0
                
            # Save the new data
            completedPartialData[key] = {'prob': 0, 'kcs importance': 0, 'correct': -1, 'Difficulty': 0, 'Group Difficulty': 0, 'update competency': 0}
            completedPartialData[key]['prob'] = probability
            completedPartialData[key]['kcs importance'] = kcsPuzzleDict[qid][k]
            completedPartialData[key]['correct'] = correct
            completedPartialData[key]['Difficulty'] = round(Diff[qid][k],3)
            completedPartialData[key]['Weight'] = weightAtt
            completedPartialData[key]['cont_puzzles'] = len(contPuzzlesUser[uid])
            completedPartialData[key]['timestamp'] = time
            completedPartialData[key]['changeComp'] = changeComp
            completedPartialData[key]['complete_change_comp'] = complete_change
            #completedPartialData[key]['changeDiff'] = kcsPuzzleDict[qid][k] * changeDiff
            
            if(k not in competencyPartial[uid].keys()): competencyPartial[uid][k] = []
            competencyPartial[uid][k].append(Competency[uid][k])
            
                
    return Competency, A_count , Q_count, prob_test, ans_test, competencyPartial, probUser, userPuzzles, completedPartialData, probUserTest, ansUserTest, contPuzzlesUser


def run(gamma, beta, output, totalData, train_set, test_set):
   
   
   uDict,gDict,qDict,kcDict,kcsPuzzleDict = loadDataset(totalData)
   competency_ELO = pd.DataFrame()
   competency_ELO_PCA = pd.DataFrame()
   difficulty_ELO = pd.DataFrame()



   # First stage
   question_difficulty_array = dict()
   question_counter_array = dict()


   for q in qDict.keys():
       if(q not in question_difficulty_array.keys()):
           question_difficulty_array[q]=dict()
           question_counter_array[q]=dict()
           question_counter_array[q]=0

       for k in kcDict.keys():
           question_difficulty_array[q][k]=0


   learner_competency_array = dict()
   response_counter_array = dict()
   for user in uDict.keys():
       if(user not in learner_competency_array.keys()):
           learner_competency_array[user]=dict()
           response_counter_array[user]=dict()
           response_counter_array[user]=0
       for k in kcDict.keys():
           learner_competency_array[user][k]=0

   # Array with the difficulty array
   arrayDiff = arrayDifficulty(totalData, learner_competency_array, question_difficulty_array, response_counter_array, question_counter_array, kcsPuzzleDict,gDict,gamma, beta)

   puzzleDiffMean = dict()
   #arrayDiffComp = dict()
   #arrayDiffComp = arrayDiff
   for puzzle in qDict.keys():
       puzzleDiffMean[puzzle] = dict()
       for k in kcsPuzzleDict[puzzle]:
           puzzleDiffMean[puzzle][k] = 0
           if(len(arrayDiff[puzzle][k]) > 30):
               for i in range(10):
                   arrayDiff[puzzle][k].pop(i)
                   arrayDiff[puzzle][k].pop(-i)
                   
           puzzleDiffMean[puzzle][k] = statistics.mean(arrayDiff[puzzle][k])


   # Second Stage
   
   if(output == 'metrics'):
       
       question_counter_Model = dict()
       for q in qDict.keys():
           if(q not in question_counter_Model.keys()):
               question_counter_Model[q]=dict()
               question_counter_Model[q]=0



       learner_competency_Model = dict()
       response_counter_Model = dict()
       for user in uDict.keys():
           if(user not in learner_competency_Model.keys()):
               learner_competency_Model[user]=dict()
               response_counter_Model[user]=dict()
               response_counter_Model[user]=0
           for k in kcDict.keys():
               learner_competency_Model[user][k]=0

       learner_competency_train, response_counter_train, question_counter_train, prob_train, ans_train, competencyPartial_train, probUser_train, userPuzzles_train, completedPartialData, probUserTrain, ansUserTrain, contPuzzlesUser_Train = multiTopic_ELO(train_set, learner_competency_Model, puzzleDiffMean, response_counter_Model, question_counter_Model, kcsPuzzleDict,gDict,gamma, beta)
       learner_competency_test ,response_counter_test, question_counter_test, prob_test, ans_test,competencyPartial_test, probUser_test, userPuzzles_test, completedPartialData, probUserT, ansUserT, contPuzzlesUser_Test = multiTopic_ELO(test_set, learner_competency_train, puzzleDiffMean, response_counter_train, question_counter_train, kcsPuzzleDict,gDict,gamma, beta)



       # Quality metrics
       group_prob_test = []
       contUser =0
       contT = 0
       for user in prob_test.keys():
           contUser+=1
           for task in prob_test[user].keys():
               contT+=1
               group_prob_test.append(prob_test[user][task])

       group_ans_test = []
       for user in ans_test.keys():
           for task in ans_test[user].keys():
               group_ans_test.append(ans_test[user][task])


       accuracy = accuracyFunction(group_ans_test, group_prob_test)
       auc = auc_roc(group_ans_test, group_prob_test)
       kappa = get_cohenKappa(group_ans_test, group_prob_test)

       return accuracy, auc, kappa
       
       
       
   else:


       # Data for step by step data output
       question_counter = dict()

       for q in qDict.keys():
           if(q not in question_counter.keys()):
               question_counter[q]=dict()
               question_counter[q]=0

       learner_competency = dict()
       response_counter = dict()
       for user in uDict.keys():
           if(user not in learner_competency.keys()):
               learner_competency[user]=dict()
               response_counter[user]=dict()
               response_counter[user]=0
           for k in kcDict.keys():
               learner_competency[user][k]=0

       # Multi-ELO function
       learner_competency_total, response_counter_total, question_counter_total, prob_total, ans_total, competencyPartial_total, probUser_total, userPuzzles_total, completedPartialData, probUserTest, ansUserTest, contPuzzlesUser = multiTopic_ELO(totalData, learner_competency, puzzleDiffMean, response_counter, question_counter, kcsPuzzleDict,gDict,gamma, beta)


       totalCompetencyGMD = []
       totalCompetencyCO5 = []
       totalCompetencyCO6 = []
       totalCompetencyMG1 = []


       for user in learner_competency.keys():
           for x in learner_competency[user]:
               if(x == 'GMD.4'):
                   totalCompetencyGMD.append(learner_competency[user][x])
               elif(x == 'CO.5'):
                   totalCompetencyCO5.append(learner_competency[user][x])
               elif(x == 'CO.6'):
                   totalCompetencyCO6.append(learner_competency[user][x])
               elif(x == 'MG.1'):
                   totalCompetencyMG1.append(learner_competency[user][x])


       minCompetencyGMD = min(totalCompetencyGMD)
       maxCompetencyGMD = max(totalCompetencyGMD)

       minCompetencyCO5 = min(totalCompetencyCO5)
       maxCompetencyCO5 = max(totalCompetencyCO5)

       minCompetencyCO6 = min(totalCompetencyCO6)
       maxCompetencyCO6 = max(totalCompetencyCO6)

       minCompetencyMG1 = min(totalCompetencyMG1)
       maxCompetencyMG1 = max(totalCompetencyMG1)

       normalized_learner_competency = dict()
       normalized_global_competency = dict()
       for user in learner_competency.keys():
           normalized_learner_competency[user]=dict()
           normalized_global_competency[user] = 0
           for x in learner_competency[user]:
               if(x == 'GMD.4'):
                   normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyGMD)/(maxCompetencyGMD-minCompetencyGMD)
                   normalized_global_competency[user] += normalized_learner_competency[user][x]

               elif(x == 'CO.5'):
                   normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyCO5)/(maxCompetencyCO5-minCompetencyCO5)
                   normalized_global_competency[user] += normalized_learner_competency[user][x]

               elif(x == 'CO.6'):
                   normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyCO6)/(maxCompetencyCO6-minCompetencyCO6)
                   normalized_global_competency[user] += normalized_learner_competency[user][x]

               elif(x == 'MG.1'):
                   normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyMG1)/(maxCompetencyMG1-minCompetencyMG1)
                   normalized_global_competency[user] += normalized_learner_competency[user][x]


       for user in normalized_global_competency.keys():
           normalized_global_competency[user] = normalized_global_competency[user]/len(kcs)


       # Normalization Difficulty
       totalDiffGMD = []
       totalDiffCO5 = []
       totalDiffCO6 = []
       totalDiffMG1 = []

       for puzzle in puzzleDiffMean.keys():
           for x in puzzleDiffMean[puzzle]:
               if(x == 'GMD.4'):
                   totalDiffGMD.append(puzzleDiffMean[puzzle][x])
               elif(x == 'CO.5'):
                   totalDiffCO5.append(puzzleDiffMean[puzzle][x])
               elif(x == 'CO.6'):
                   totalDiffCO6.append(puzzleDiffMean[puzzle][x])
               elif(x == 'MG.1'):
                   totalDiffMG1.append(puzzleDiffMean[puzzle][x])


       minDiffGMD = min(totalDiffGMD)
       maxDiffGMD = max(totalDiffGMD)

       minDiffCO5 = min(totalDiffCO5)
       maxDiffCO5 = max(totalDiffCO5)

       minDiffCO6 = min(totalDiffCO6)
       maxDiffCO6 = max(totalDiffCO6)

       minDiffMG1 = min(totalDiffMG1)
       maxDiffMG1 = max(totalDiffMG1)

       normalized_question_difficulty = dict()

       for puzzle in puzzleDiffMean.keys():
           normalized_question_difficulty[puzzle]=dict()
           for x in puzzleDiffMean[puzzle]:
               if(x == 'GMD.4'):
                   normalized_question_difficulty[puzzle][x]= (puzzleDiffMean[puzzle][x]- minDiffGMD)/(maxDiffGMD-minDiffGMD)

               elif(x == 'CO.5'):
                   normalized_question_difficulty[puzzle][x]= (puzzleDiffMean[puzzle][x]- minDiffCO5)/(maxDiffCO5-minDiffCO5)

               elif(x == 'CO.6'):
                   normalized_question_difficulty[puzzle][x]= (puzzleDiffMean[puzzle][x]- minDiffCO6)/(maxDiffCO6-minDiffCO6)

               elif(x == 'MG.1'):
                   normalized_question_difficulty[puzzle][x]= (puzzleDiffMean[puzzle][x]- minDiffMG1)/(maxDiffMG1-minDiffMG1)

       if(output == 'step by step'):

           for i in completedPartialData.keys():
                   key_split = i.split('~')
                   competency_ELO.at[i, 'group'] = gDict[key_split[0]]
                   competency_ELO.at[i, 'user'] = key_split[0]
                   competency_ELO.at[i, 'task_id'] = key_split[1]
                   competency_ELO.at[i, 'kc'] = key_split[2]
                   competency_ELO.at[i, 'final_kc_competency'] = round(normalized_learner_competency[key_split[0]][key_split[2]],3)
                   competency_ELO.at[i, 'final_global_competency'] = round(normalized_global_competency[key_split[0]],3)
                   competency_ELO.at[i, 'current_competency'] = key_split[3]
                   competency_ELO.at[i, 'probability'] = round(completedPartialData[i]['prob'],3)
                   competency_ELO.at[i, 'correct'] = completedPartialData[i]['correct']
                   competency_ELO.at[i, 'kcs_importance'] = round(completedPartialData[i]['kcs importance'],3)
                   competency_ELO.at[i, 'difficulty'] = round(puzzleDiffMean[key_split[1]][key_split[2]],3)
                   competency_ELO.at[i, 'weight_att'] = round(completedPartialData[i]['Weight'],3)
                   competency_ELO.at[i, 'timestamp'] = completedPartialData[i]['timestamp']
                   if(len(ansUserTest[key_split[0]]) > 0): competency_ELO.at[i, 'accuracy'] = str(round(accuracyFunction(ansUserTest[key_split[0]], probUserTest[key_split[0]]), 3))
                   else: competency_ELO.at[i, 'accuracy'] = str(np.nan)
                   competency_ELO.at[i, 'n_puzzles_attempted'] = len(contPuzzlesUser[key_split[0]])
                   competency_ELO.at[i, 'p_attempted'] = round((len(contPuzzlesUser[key_split[0]]) * 100)/(len(intermediatePuzzles) + len(advancedPuzzles)), 3)
                   competency_ELO.at[i, 'change_competency'] = round(completedPartialData[i]['changeComp'],3)
                   competency_ELO.at[i, 'complete_change_comp'] = round(completedPartialData[i]['complete_change_comp'],3)
                   #competency_ELO.at[i, 'change_difficulty'] = round(completedPartialData[i]['changeDiff'],3)

           #data output preparation
           competency_ELO = pd.DataFrame(competency_ELO, columns = ['group','user','task_id', 'timestamp','kc','kcs_importance','final_kc_competency', 'final_global_competency','current_competency','change_competency','weight_att','complete_change_comp', 'probability', 'correct','accuracy','n_puzzles_attempted','p_attempted', 'difficulty'])

           return competency_ELO

       
       if(output == 'standard'):
           
           # Data for final data output (difficulty)
           concatedTaskKc = dict()

           for q in qDict.keys():
               for k in kcsPuzzleDict[q].keys():
                   concatedTaskKc[q+'~'+k] = 0
                   

           for i in concatedTaskKc.keys():
               key_split = i.split('~')
               difficulty_ELO.at[i, 'task_id'] = key_split[0]
               difficulty_ELO.at[i, 'kc'] = key_split[1]
               difficulty_ELO.at[i, 'difficulty'] = round(puzzleDiffMean[key_split[0]][key_split[1]],3)
               difficulty_ELO.at[i, 'normalized_difficulty'] = round(normalized_question_difficulty[key_split[0]][key_split[1]],3)



           idComplet = dict()
           for g in gDict.values():
               for u in gDict.keys():
                   for k in kcs:
                       iCom = g+'~'+u+'~'+k
                       idComplet[iCom] = 0

           for i in idComplet.keys():
               key_split = i.split('~')
               competency_ELO.at[i, 'group'] = key_split[0]
               competency_ELO.at[i, 'user'] = key_split[1]
               competency_ELO.at[i, 'kc'] = key_split[2]
               competency_ELO.at[i, 'competency'] = round(normalized_learner_competency[key_split[1]][key_split[2]],3)
               if(len(ansUserTest[key_split[1]]) > 0): competency_ELO_PCA.at[i, 'accuracy'] = str(round(accuracyFunction(ansUserTest[key_split[1]], probUserTest[key_split[1]]), 3))
               else: competency_ELO_PCA.at[i, 'accuracy'] = np.nan
               if(len(ansUserTest[key_split[1]]) > 0): competency_ELO.at[i, 'accuracy'] = str(round(accuracyFunction(ansUserTest[key_split[1]], probUserTest[key_split[1]]), 3))
               else: competency_ELO.at[i, 'accuracy'] = str(np.nan)
               competency_ELO.at[i, 'n_puzzles_attempted'] = len(contPuzzlesUser[key_split[1]])
               competency_ELO_PCA.at[i, 'n_puzzles_attempted'] = len(contPuzzlesUser[key_split[1]])
               competency_ELO.at[i, 'p_attempted'] = round((len(contPuzzlesUser[key_split[1]]) * 100)/(len(intermediatePuzzles) + len(advancedPuzzles)), 3)
           
           # Replace NaN values by 0
           competency_ELO_PCA['accuracy'] = competency_ELO_PCA['accuracy'].replace(np.nan, 0)
           # Data preprocesing to match variable weights
           scaler = StandardScaler()
           scaler.fit(competency_ELO_PCA)
           scaled_data = scaler.transform(competency_ELO_PCA)
           
           # PCA object and look for the main variables
           pca = PCA(n_components=1)
           pca.fit(scaled_data)
           # Dimensionality reduction
           x_pca = pca.transform(scaled_data)
           
           # Re-enter the NaN values
           x_pca = np.round(np.where(x_pca == min(x_pca), np.nan, x_pca),3)
           
           # Normalized
           x_pca_normalized = np.round(normalized_PCA(x_pca),3)
           
           #data output preparation
           difficulty_ELO = pd.DataFrame(difficulty_ELO, columns = ['task_id','kc', 'difficulty','normalized_difficulty'])
           competency_ELO = pd.DataFrame(competency_ELO, columns = ['group','user','kc', 'competency', 'accuracy','n_puzzles_attempted','p_attempted'])
           

           competency_ELO['pca'] = x_pca.astype(str)
           competency_ELO['pca_normalized'] = x_pca_normalized.astype(str)
           
           
           return competency_ELO, difficulty_ELO








############################################################

# USAGE EXAMPLE
# totalData, train_set, test_set = adaptedData(dataEvents)
# competency_ELO, difficulty_ELO= run(1.8, 0.05, 'standard', totalData, train_set, test_set)
############################################################

