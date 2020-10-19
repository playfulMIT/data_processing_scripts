from datacollection.models import Event, URL, CustomSession
from django_pandas.io import read_frame
from datetime import datetime
import json
import numpy as np
import pandas as pd
import math
import csv
import os

all_data_collection_urls = ['ginnymason', 'chadsalyer', 'kristinknowlton', 'lori day', 'leja', 'leja2', 'debbiepoull', 'juliamorgan']

student_id = 'user'
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

# mapping to positions

typeMapping = {'Sandbox': 'GMD.4~CO.5~CO.6', '1. One Box': 'GMD.4~CO.5~CO.6', '2. Separated Boxes': 'GMD.4~CO.5~CO.6', '3. Rotate a Pyramid': 'GMD.4~CO.5~CO.6', '4. Match Silhouettes': 'GMD.4~CO.5~CO.6', '5. Removing Objects': 'GMD.4~CO.5~CO.6', '6. Stretch a Ramp': 'GMD.4~CO.5~CO.6', '7. Max 2 Boxes': 'GMD.4~CO.5~CO.6', '8. Combine 2 Ramps': 'GMD.4~CO.5~CO.6', '9. Scaling Round Objects': 'GMD.4~CO.5~CO.6',
               'Square Cross-Sections': 'GMD.4~CO.5~CO.6', 'Bird Fez': 'MG.1~GMD.4~CO.5~CO.6' , 'Pi Henge': 'MG.1~GMD.4~CO.5~CO.6', '45-Degree Rotations': 'GMD.4~CO.5~CO.6',  'Pyramids are Strange': 'GMD.4~CO.5~CO.6', 'Boxes Obscure Spheres': 'GMD.4~CO.5~CO.6', 'Object Limits': 'GMD.4~CO.5~CO.6', 'Tetromino': 'GMD.4~CO.5~CO.6', 'Angled Silhouette': 'GMD.4~CO.5~CO.6',
               'Warm Up':'GMD.4~CO.5~CO.6','Sugar Cones': 'GMD.4~CO.5~CO.6', 'Stranger Shapes': 'GMD.4~CO.5~CO.6', 'Tall and Small': 'GMD.4~CO.5~CO.6', 'Ramp Up and Can It': 'GMD.4~CO.5~CO.6', 'More Than Meets Your Eye': 'GMD.4~CO.5~CO.6', 'Not Bird': 'GMD.4~CO.5~CO.6', 'Unnecessary': 'GMD.4~CO.5~CO.6', 'Zzz': 'GMD.4~CO.5~CO.6', 'Bull Market': 'MG.1~GMD.4~CO.5~CO.6', 'Few Clues': 'GMD.4~CO.5~CO.6', 'Orange Dance': 'GMD.4~CO.5~CO.6', 'Bear Market': 'GMD.4~CO.5~CO.6'}




def adaptedData(group = 'all'):
    
    if group == 'all' :
        toFilter = all_data_collection_urls
    else:
        toFilter = group

    urls = URL.objects.filter(name__in=toFilter)
    sessions = CustomSession.objects.filter(url__in=urls)
    qs = Event.objects.filter(session__in=sessions)
    dataEvents = read_frame(qs)
    
    
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
    if(group != 'all'):
        dataEvents = dataEvents[dataEvents['group'].isin(group)]
          
    # the data is grouped by the necessary variables
    activity_by_user = dataEvents.groupby(['group_user_id','group', 'user','group_user_task_id','task_id']).agg({'id':'count',
                                             'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                              'type':'different_events'})
    
    
    #indicate the index variable
    activity_by_user.index = activity_by_user['group_user_task_id'].values
                                                                                              
    #initialize the metrics
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_completed'] = 0
    activity_by_user['kc'] = ''
    #initialize the data structures
    puzzleEvents = dict()
    timePuzzle = dict()
    puzzCom= dict()
    puzzDestr = dict()
    
    
    
    for user in dataEvents['group_user_id'].unique():
        
        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []
        
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None

        for enum, event in user_events.iterrows():
            
            if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):
                                                                          
                user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']
               
                # initialize if the id is new
                if(user_puzzle_key not in puzzleEvents.keys()):
                    puzzleEvents[user_puzzle_key]= 1
                    timePuzzle[user_puzzle_key] = 0
                    puzzCom[user_puzzle_key] = 0
                    puzzDestr[user_puzzle_key] = ''
                    
            # the event is not final event
            if(event['type'] not in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-create_user', 'ws-login_user']):
                    puzzleEvents[user_puzzle_key] += 1
                    splitDes = user_puzzle_key.split("~")
                    puzzDestr[user_puzzle_key] = typeMapping[splitDes[2]]
                    
                       
                        
            # the puzzle ends
            if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete']):
                    if(event['type'] in ['ws-puzzle_complete']):
                        if(puzzCom[user_puzzle_key] ==0):
                            puzzCom[user_puzzle_key] +=1

    
    
    cont = dict()
    # add the data by group_user_task_id
    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[1] not in cont.keys()): cont[key_split[1]] = 0
        if(key_split[2] != ''):
            activity_by_user.at[i, 'kc'] = puzzDestr[i]
            activity_by_user.at[i, 'n_completed'] = puzzCom[i]
            activity_by_user.at[i, 'active_time'] = timePuzzle[i]


    #delete row with NaN
    activity_by_user.dropna(inplace=True)
  
    #data output preparation
    activity_by_user = pd.DataFrame(activity_by_user, columns = ['group_user_task_id','group','user','task_id', 'n_completed', 'kc'])

    train = activity_by_user.head(round(len(activity_by_user)*0.7))
    test = activity_by_user.tail(len(activity_by_user) - round(len(activity_by_user)*0.7))
    
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



def multiTopic_ELO(inputData, Competency, Diff,groupDiff, A_count, Q_count, kcsPuzzleDict ,gDict,gamma, beta):

    alpha = 1
    alpha_denominator = 0
    correct = 0
    prob_test = dict()
    ans_test = dict()

    response = np.zeros((len(inputData), 1))

    for count, (index, item) in enumerate(inputData.iterrows()):
        alpha_denominator = 0
        uid = item[student_id]
        qid = item[puzzle_name]
        diff = Diff[qid]
        comp= dict()
        comp[uid]=[]
        for k in kcsPuzzleDict[qid]:
            comp[uid].append(Competency[uid][k] * kcsPuzzleDict[qid][k])
        compTotal = np.sum(comp[uid])
        probability = (1)/(1 + math.exp( -1 * (compTotal - diff)))
        if(uid not in prob_test.keys()):
            prob_test[uid] = dict()
        prob_test[uid][qid]=probability
        q_answered_count = Q_count[qid]
        
        if item[completed] == 1:

            response[count] = 1
            correct = 1
        else:
            response[count] = 0
            correct = 0
        
        if(uid not in ans_test.keys()):
            ans_test[uid] = dict()
        ans_test[uid][qid] = correct
        
        groupDiff[gDict[uid]][qid] = groupDiff[gDict[uid]][qid] + ((gamma)/(1 + beta * q_answered_count)) * (probability - correct)
        
        Diff[qid] = Diff[qid] + ((gamma)/(1 + beta * q_answered_count)) * (probability - correct)
        Q_count[qid] += 1
        
        alpha_numerator = probability - correct
        for k in kcsPuzzleDict[qid]:
            c_lambda = Competency[uid][k]
            probability_lambda = (1)/(1 + math.exp( -1 * (c_lambda - diff)))
            alpha_denominator = alpha_denominator + (correct - probability_lambda)
        alpha = abs(alpha_numerator / alpha_denominator)

        for k in kcsPuzzleDict[qid]:
            u_answered_count = A_count[uid][k]
            c = Competency[uid][k]
            probability = (1)/(1 + math.exp( -1 * (compTotal - diff)))
            
            Competency[uid][k] = Competency[uid][k]+kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability)
            A_count[uid][k] += 1
                
    return Competency, Diff,groupDiff, A_count , Q_count, prob_test, ans_test




def run(model, gamma, beta, totalData, train_set, test_set):
  
  
  uDict,gDict,qDict,kcDict,kcsPuzzleDict = loadDataset(totalData)

  competency_ELO = pd.DataFrame()
  difficulty_ELO = pd.DataFrame()
                                                                                            
  #initialize the metrics
  difficulty_ELO['group'] = ''
  difficulty_ELO['task_id'] = ''
  difficulty_ELO['difficulty'] = np.nan
  competency_ELO['group'] = ''
  competency_ELO['user'] = ''
  competency_ELO['kc'] = ''
  competency_ELO['competency'] = np.nan
  
  
  
  idComplet = dict()
  for g in gDict.values():
      for u in gDict.keys():
          for k in kcs:
              iCom = g+'~'+u+'~'+k
              idComplet[iCom] = 0
  

  if model == 'multiTopic':
      
      group_difficulty = dict()
      question_difficulty = dict()
      question_counter = dict()
      concatedGroupTask = dict()
      for g in gDict.values():
          group_difficulty[g] = dict()
          for q in qDict.keys():
              question_difficulty[q]=0
              question_counter[q]=0
              group_difficulty[g][q]=0
              concatedGroupTask[g+'~'+q] = 0
      
      
      learner_competency = dict()
      response_counter = dict()
      for user in uDict.keys():
          if(user not in learner_competency.keys()):
              learner_competency[user]=dict()
              response_counter[user]=dict()
          for k in kcDict.keys():
              learner_competency[user][k]=0
              response_counter[user][k]=0


      learner_competency_train, question_difficulty_train,group_difficulty_train, response_counter_train, question_counter_train, prob_train, ans_train   = multiTopic_ELO(train_set, learner_competency, question_difficulty,group_difficulty, response_counter, question_counter, kcsPuzzleDict,gDict,gamma, beta)
      learner_competency_test, question_difficulty_test,group_difficulty_test, response_counter_test, question_counter_test, prob_test, ans_test   = multiTopic_ELO(test_set, learner_competency_train, question_difficulty_train,group_difficulty_train, response_counter_train, question_counter_train, kcsPuzzleDict,gDict,gamma, beta)

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
  for user in learner_competency.keys():
      normalized_learner_competency[user]=dict()
      for x in learner_competency[user]:
          if(x == 'GMD.4'):
              normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyGMD)/(maxCompetencyGMD-minCompetencyGMD)
          elif(x == 'CO.5'):
              normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyCO5)/(maxCompetencyCO5-minCompetencyCO5)
          elif(x == 'CO.6'):
              normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyCO6)/(maxCompetencyCO6-minCompetencyCO6)
          elif(x == 'MG.1'):
              normalized_learner_competency[user][x]= (learner_competency[user][x]- minCompetencyMG1)/(maxCompetencyMG1-minCompetencyMG1)
          
  
  
  normalized_question_difficulty = dict()
  for puzzle in question_difficulty.keys():
      if(puzzle not in question_difficulty.keys()):
          normalized_question_difficulty[puzzle] = 0
      normalized_question_difficulty[puzzle] = (question_difficulty[puzzle]-min(question_difficulty.values()))/(max(question_difficulty.values())-min(question_difficulty.values()))
      

  normalized_group_difficulty = dict()
  for group in group_difficulty.keys():
      normalized_group_difficulty[group] = dict()
      for puzzle in group_difficulty[group].keys():
          normalized_group_difficulty[group][puzzle]=0
          normalized_group_difficulty[group][puzzle] = (group_difficulty[group][puzzle]-min(group_difficulty[group].values()))/(max(group_difficulty[group].values())-min(group_difficulty[group].values()))
     
          
  groupUser = set()
  
  for i in concatedGroupTask.keys():
      key_split = i.split('~')
      difficulty_ELO.at[i, 'group'] = key_split[0]
      difficulty_ELO.at[i, 'task_id'] = key_split[1]
      difficulty_ELO.at[i, 'difficulty'] = normalized_group_difficulty[key_split[0]][key_split[1]]
          
  for i in idComplet.keys():
      key_split = i.split('~')
      competency_ELO.at[i, 'group'] = key_split[0]
      competency_ELO.at[i, 'user'] = key_split[1]
      competency_ELO.at[i, 'kc'] = key_split[2]
      competency_ELO.at[i, 'competency'] = normalized_learner_competency[key_split[1]][key_split[2]]
          

  #data output preparation
  difficulty_ELO = pd.DataFrame(difficulty_ELO, columns = ['group','task_id', 'difficulty'])
  competency_ELO = pd.DataFrame(competency_ELO, columns = ['group','user','kc', 'competency'])

  return difficulty_ELO, competency_ELO


  ###################################################################

#  totalData, train_set, test_set = adaptedData(group=['kristinknowlton'])
#  difficulty_ELO, competency_ELO = run('multiTopic',1.8, 0.05,totalData, train_set, test_set)

  ###################################################################





