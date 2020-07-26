import pandas as pd
from datetime import datetime
import numpy as np
import json

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/jruipere/Dropbox (MIT)/Game-based Assessment/data_processing_scripts/data/anonymized_dataset.csv', sep=";")
# dataEvents = pd.read_csv('/Users/pedroantonio/Desktop/data/anonymized_dataset.csv', sep=";")
# lelevelsOfActivity = computeLevelsOfActivity(dataEvents)

# mapping to positions
typeMapping = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial',
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

tutorialPuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Tutorial'):
        tutorialPuzzles.append(desc[0])
        
advancedPuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Hard Puzzles'):
        advancedPuzzles.append(desc[0])
        
        
intermediatePuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Easy Puzzles'):
        intermediatePuzzles.append(desc[0])
        
allPuzzles = []
for puzzle in typeMapping:
    desc = puzzle.split("~")
    allPuzzles.append(desc[0])


def computePersistence(dataEvents, group = 'all'):

    
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
    
    typeEvents = ['ws-snapshot','ws-paint', 'ws-rotate_view','ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape','ws-undo_action','ws-redo_action', 'ws-check_solution']
    
   
        
    #initialize the metrics
    activity_by_user['completed'] = np.nan
    activity_by_user['active_time'] = np.nan
    activity_by_user['event'] = np.nan
    activity_by_user['different_events'] = np.nan
    activity_by_user['persistence'] = np.nan
    activity_by_user['percentileAtt'] = np.nan
    activity_by_user['percentileActiveTime'] = np.nan
    #activity_by_user['percTimeAbove85'] = np.nan
   # activity_by_user['percTimeAbove15'] = np.nan
    #activity_by_user['percAttAbove85'] = np.nan
    #activity_by_user['percAttAbove15'] = np.nan
    
    for event in typeEvents:
        activity_by_user[event] = 0
    
    #initialize the data structures
    userFunnelDict = dict()
    puzzleEvents = dict()
    eventsDiff = []
    eventsDiff_puzzle = dict()
    timePuzzle = dict()
    globalTypesEvents = dict()
    eventInitial = dict()
    totalTime = dict()
    n_attempts = dict()
    completados = dict()
    
    percentilAtt = dict()
    percentilTime = dict()
    
    percentilAttValue = 90
    percentilTimeValue = 90
    
      
    for user in dataEvents['group_user_id'].unique():
        
        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []
        
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None

        for enum, event in user_events.iterrows():
            
            # If it is the first event
            if(previousEvent is None):
                previousEvent = event
                continue
            
            if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):
                
                #create id: group+user+task_id
                user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']
                
                #if(user_puzzle_key not in eventInitial.keys()):
                eventInitial[user_puzzle_key] = event['time']
                       
                # initialize if the id is new
                if(user_puzzle_key not in puzzleEvents.keys()):
                    
                    percentilAtt[user_puzzle_key] = percentilAttValue
                    percentilTime[user_puzzle_key] = percentilTimeValue
                    
                    completados[user_puzzle_key] = 0
                    n_attempts[user_puzzle_key] = 0
                    totalTime[user_puzzle_key] = 0
                    puzzleEvents[user_puzzle_key]= 1
                    eventsDiff_puzzle[user_puzzle_key] = []
                    eventsDiff_puzzle[user_puzzle_key].append(event['type'])
                    timePuzzle[user_puzzle_key] = 0
                    globalTypesEvents[user_puzzle_key] = dict()
                    for ev in typeEvents:
                        globalTypesEvents[user_puzzle_key][ev]= 0
            
            # the event is not final event
            if(event['type'] not in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-create_user', 'ws-login_user']):
                    
                    
                    puzzleEvents[user_puzzle_key] += 1
                    #add the event type
                    eventsDiff_puzzle[user_puzzle_key].append(event['type'])
                    
                    #calculate the duration of the event
                    delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                    if((delta_seconds < theresHoldActivity)):
                        timePuzzle[user_puzzle_key] += delta_seconds

                    previousEvent = event
                    
                    #update event counters by type
                    if(event['type'] in typeEvents):
                        globalTypesEvents[user_puzzle_key][event['type']] +=1
                    
                        
            # the puzzle ends
            if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete']):
                    
                    
                    # Total time
                    if(totalTime[user_puzzle_key] == 0):
                        totalTime[user_puzzle_key] = (event['time'] - eventInitial[user_puzzle_key]).total_seconds()
                    else:
                        totalTime[user_puzzle_key] += (event['time'] - eventInitial[user_puzzle_key]).total_seconds()
                
                    puzzleEvents[user_puzzle_key] += 1
                    
                    if(event['type'] in ['ws-puzzle_complete']): completados[user_puzzle_key] = 1
                    
                    #add the event type
                    eventsDiff_puzzle[user_puzzle_key].append(event['type'])
                    
                    #calculate the duration of the event
                    delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                    if((delta_seconds < theresHoldActivity)):
                        timePuzzle[user_puzzle_key] += delta_seconds
                        

                    previousEvent = event
                    
                    
            if(event['type'] == 'ws-check_solution'):
                if(completados[user_puzzle_key] != 1):
                    n_attempts[user_puzzle_key] +=1
    


                
########################################################################################


    # Diccionarios para guardar para cada usuario los valores de cada puzzle
    userTime = dict()
    userAtt = dict()

    # add the data by group_user_task_id
    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[1] not in userTime.keys()):
            userTime[key_split[1]] = dict()
            userAtt[key_split[1]] = dict()

        if(key_split[2] != ''):
            
            ######Cambiar con cada clase#######
            if(key_split[2] in allPuzzles): userAtt[key_split[1]][key_split[2]] = n_attempts[i]
                                                
            if(key_split[2] in allPuzzles): userTime[key_split[1]][key_split[2]] = timePuzzle[i]
                
                
    puzzleTime = dict()
    puzzleAtt = dict()
    
    percentileTimeMax = dict()
    percentileTimeMin = dict()
    percentileAttMax = dict()
    percentileAttMin = dict()
    
    checkPersistantAttMax = dict()
    checkPersistantAttMin = dict()
    checkPersistantTimeMax = dict()
    checkPersistantTimeMin = dict()
    
    for user in userTime.keys():
        for puzzle in userTime[user]:
            if(puzzle not in puzzleTime.keys()):
                puzzleTime[puzzle] = []
                puzzleAtt[puzzle] = []
                
            puzzleTime[puzzle].append(userTime[user][puzzle])
            puzzleAtt[puzzle].append(userAtt[user][puzzle])
            
            
    for puzzle in puzzleTime.keys():
        if(puzzle not in percentileAttMax.keys()):
            #medianTime[puzzle] = statistics.median(puzzleTime[puzzle])
            #medianAtt[puzzle] = statistics.median(puzzleAtt[puzzle])
            percentileTimeMax[puzzle] = np.percentile(puzzleTime[puzzle], 85)
            percentileTimeMin[puzzle] = np.percentile(puzzleTime[puzzle], 15)
            percentileAttMax[puzzle] = np.percentile(puzzleAtt[puzzle], 85)
            percentileAttMin[puzzle] = np.percentile(puzzleAtt[puzzle], 15)
                 
    persistent = dict()
    boolPersistent = dict()
    totalAtt = dict()
    persistentCase = dict()
    persistentPercent = dict()
    
    percentileActiveTime = dict()
    percentileAtt = dict()
    
    
    for user in userTime.keys():
        if(user not in persistent.keys()):
            persistent[user] = dict()

    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[2] != ''):
            
            
            percentileActiveTime[i] = stats.percentileofscore(puzzleTime[key_split[2]], userTime[key_split[1]][key_split[2]])
            percentileAtt[i] = stats.percentileofscore(puzzleAtt[key_split[2]], userAtt[key_split[1]][key_split[2]])
            
            
            
            if(key_split[1] not in persistentCase.keys()):
                totalAtt[key_split[1]] = 0
                persistentCase[key_split[1]] = 0
                
            if(i not in checkPersistantAttMax.keys()):
                checkPersistantAttMax[i] = False
                checkPersistantAttMin[i] = False
                checkPersistantTimeMax[i] = False
                checkPersistantTimeMin[i] = False
                boolPersistent[i] = 0
                persistentPercent[i] = 0

                
            if(userAtt[key_split[1]][key_split[2]] >= percentileAttMax[key_split[2]]):
                checkPersistantAttMax[i] = True
            if(userAtt[key_split[1]][key_split[2]] <= percentileAttMin[key_split[2]]):
                checkPersistantAttMin[i] = True
                
            if(userTime[key_split[1]][key_split[2]] >= percentileTimeMax[key_split[2]]):
                checkPersistantTimeMax[i] = True
                
            if(userTime[key_split[1]][key_split[2]] <= percentileTimeMin[key_split[2]]):
                checkPersistantTimeMin[i] = True
            
#            if(userAtt[key_split[1]][key_split[2]] >= medianAtt[key_split[2]]):
#                checkPersistantAtt[i]=1
                
            if(checkPersistantTimeMax[i] == True):
                if(checkPersistantAttMax[i]== True):
                    boolPersistent[i] = 1
            else: boolPersistent[i] = 0
                

    for key in boolPersistent.keys():
        key_split = key.split('~')
        if(boolPersistent[key] == 1):
            persistentCase[key_split[1]] +=1
            totalAtt[key_split[1]] +=1
            
        else: totalAtt[key_split[1]] +=1
    
    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[2] != ''):
        
            #print("casos persistentes: ",persistentCase[key_split[1]], "casos totales: ", totalAtt[key_split[1]], "Usuario: ", key )
            persistentPercent[i] = round(( (persistentCase[key_split[1]] / totalAtt[key_split[1]]) * 100),2)
        
            

########################################################################################
                
    # add the data by group_user_task_id
    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[2] != ''):
            activity_by_user.at[i, 'event'] = puzzleEvents[i]
            activity_by_user.at[i, 'different_events'] = len(set(eventsDiff_puzzle[i]))
            activity_by_user.at[i, 'active_time'] = timePuzzle[i]
            activity_by_user.at[i, 'percentileAtt'] = percentileAtt[i]
            activity_by_user.at[i, 'percentileActiveTime'] = percentileActiveTime[i]
            activity_by_user.at[i, 'completed'] = completados[i]
            activity_by_user.at[i, 'persistence'] = boolPersistent[i]
            activity_by_user.at[i, 'percTimeAbove85'] = checkPersistantTimeMax[i]
            activity_by_user.at[i, 'percTimeAbove15'] = checkPersistantTimeMin[i]
            activity_by_user.at[i, 'percAttAbove85'] = checkPersistantAttMax[i]
            activity_by_user.at[i, 'percAttAbove15'] = checkPersistantAttMin[i]
            activity_by_user.at[i, 'totalUserPersistence'] = persistentPercent[i]
            
            
            
            for event in typeEvents:
                activity_by_user.at[i, event] = globalTypesEvents[i][event]
    
    
    #delete row with NaN
    activity_by_user.dropna(inplace=True)
    #delete group_user_task_id column
    activity_by_user.drop(columns=['group_user_task_id'], inplace=True)
    
    #data output preparation
    activity_by_user = pd.DataFrame(activity_by_user, columns=['group', 'user','task_id', 'completed', 'active_time','percentileActiveTime','percTimeAbove85', 'percTimeAbove15', 'ws-check_solution','percentileAtt','percAttAbove85', 'percAttAbove15', 'totalUserPersistence'])
        
    return activity_by_user




