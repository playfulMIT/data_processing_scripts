from datacollection.models import Event, URL, CustomSession
from django_pandas.io import read_frame
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
from scipy import stats

# mapping to positions

difficultyMapping = ['Sandbox~0.000001','1. One Box~0.000002', '2. Separated Boxes~0.111127', '3. Rotate a Pyramid~0.083447', '4. Match Silhouettes~0.061887', '5. Removing Objects~0.106021', '6. Stretch a Ramp~0.107035', '7. Max 2 Boxes~0.078039', '8. Combine 2 Ramps~0.068608', '9. Scaling Round Objects~0.128647', 
               'Square Cross-Sections~0.199714', 'Bird Fez~0.156674', 'Pi Henge~0.067346', '45-Degree Rotations~0.096715',  'Pyramids are Strange~0.179600', 'Boxes Obscure Spheres~0.266198', 'Object Limits~0.257177', 'Not Bird~0.260197', 'Angled Silhouette~0.147673',
               'Warm Up~0.183971','Tetromino~0.226869', 'Stranger Shapes~0.283971', 'Sugar Cones~0.085909', 'Tall and Small~0.266869', 'Ramp Up and Can It~0.206271', 'More Than Meets Your Eye~0.192319', 'Unnecessary~0.76', 'Zzz~0.234035', 'Bull Market~0.358579', 'Few Clues~0.324041', 'Orange Dance~0.647731', 'Bear Market~1.000000']


typeMapping = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial', 
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

difficultyPuzzles = dict()

for puzzle in difficultyMapping:
    desc = puzzle.split("~")
    difficultyPuzzles[desc[0]] = float(desc[1])


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

all_data_collection_urls = ['ginnymason', 'chadsalyer', 'kristinknowlton', 'lori day', 'leja', 'leja2', 'debbiepoull', 'juliamorgan']

def computePersistenceByPuzzle(group = 'all'):

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


    # the data is grouped by the necessary variables      
    activity_by_user = dataEvents.groupby(['group_user_id']).agg({'id':'count',
                                                 'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                                  'type':'different_events'}) 

    dataEvents = dataEvents.sort_values('time')

    # Events type structures
    typeEvents = ['ws-snapshot','ws-paint', 'ws-rotate_view','ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape','ws-undo_action','ws-redo_action', 'ws-check_solution']
    manipulationTypeEvents = ['ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape']

    # Initialize type events structure
    for event in typeEvents:
        activity_by_user[event] = 0
        
        
    # Initialize the data structures 
    puzzleEvents = dict()
    timePuzzle = dict()
    globalTypesEvents = dict()
    n_attempts = dict()
    completed = dict()
    timestamp = dict() 

    breaksPuzzle = dict()
    contCheckSol = dict()

    manipulationEvents = dict()
    timeFirstCheck = dict()
    timeSubExit = dict()
    timeCheckActual = dict()
    timeBetweenSub = dict()
    
    avgTime_start_check = dict()
    differentDays = dict()
    lastDay = dict()
    
    checkSolProd = dict()
    timePuzzleProd = dict()
    puzzleEventsProd = dict()
    
    categoryPuzz = dict()
    userDissconect = dict()
    
    for user in dataEvents['group_user_id'].unique():

        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []

        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None
        #userParc = None
        task_id = None
        initialTime = None
        #prev_id = 1  
        
        for enum, event in user_events.iterrows():

            # If it is the first event
            if(previousEvent is None):
                previousEvent = event
                continue
                    
            if( event['type'] in ['ws-start_level', 'ws-puzzle_started'] ):

                task_id = json.loads(event['data'])['task_id']
                if(task_id == "Sandbox"): continue

                # ID
                user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id                
                key_split = user_puzzle_key.split('~')  

                # Continue if the user has completed the puzzle
                if(user_puzzle_key not in completed.keys()): completed[user_puzzle_key] = 0
                if(completed[user_puzzle_key] == 1): continue

                # Initialize data structures
                if(event['type'] == 'ws-puzzle_started'):
                    if(user_puzzle_key not in n_attempts.keys()): 
                        n_attempts[user_puzzle_key] = 0
                    #else: n_attempts[user_puzzle_key] +=1    
                if(user_puzzle_key not in timeSubExit.keys()):
                    timeSubExit[user_puzzle_key] = str(0)
                    timeBetweenSub[user_puzzle_key] = str(0)  


                # initialize if the id is new                                                                              
                if(user_puzzle_key not in puzzleEvents.keys()):

                    breaksPuzzle[user_puzzle_key] = 0
                    categoryPuzz[user_puzzle_key] = ''
                    puzzleEvents[user_puzzle_key]= 1
                    timePuzzle[user_puzzle_key] = 0
                    contCheckSol[user_puzzle_key] = 0
                    manipulationEvents[user_puzzle_key] = 0
                    timeFirstCheck[user_puzzle_key] = 0
                    userDissconect[user_puzzle_key] = 0

                    globalTypesEvents[user_puzzle_key] = dict()
                    for ev in typeEvents:
                        globalTypesEvents[user_puzzle_key][ev]= 0

                # Category puzzle
                if(task_id in tutorialPuzzles):
                    categoryPuzz[user_puzzle_key] = 'Tutorial'
                elif(task_id in advancedPuzzles): 
                    categoryPuzz[user_puzzle_key] = 'Advanced'
                else: categoryPuzz[user_puzzle_key] = 'Intermediate'   

                #timestamp
                if(event['type'] in 'ws-puzzle_started'): 
                    timestamp[user_puzzle_key] = event['time']
                    initialTime = timestamp[user_puzzle_key]    


            # the event is not final event
            if(event['type'] not in ['ws-exit_to_menu' , 'ws-puzzle_complete', 'ws-disconnect', 'ws-create_user', 'ws-login_user']): 
                if(user_puzzle_key in completed.keys() and completed[user_puzzle_key] != 1):

                    
                    #Different days
                    date = str(event['time']).split('-')
                    day = date[2].split(" ")
                    if(event['type'] in ['ws-check_solution', 'w-snapshot']):
                        if(user not in lastDay.keys()): lastDay[user] = str(0)
                        if(lastDay[user] != date[1]+'~'+day[0]):
                            lastDay[user] = date[1]+'~'+day[0]
                            if(user not in differentDays.keys()): differentDays[user] = 1
                            else: differentDays[user] += 1

                    # Cont the event
                    puzzleEvents[user_puzzle_key] += 1                                                                         

                    #Calculate the duration of the event                                                                          
                    delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                    if((delta_seconds < theresHoldActivity)):
                        timePuzzle[user_puzzle_key] += delta_seconds

                    #Breaks
                    if((delta_seconds > 15)):
                        breaksPuzzle[user_puzzle_key] += 1

                    previousEvent = event 

                    #Update event counters by type
                    if(event['type'] in typeEvents):
                        globalTypesEvents[user_puzzle_key][event['type']] +=1

                    # Time the first check
                    if(globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 1): timeFirstCheck[user_puzzle_key] = event['time']

                    # Update the manipulation events counter
                    if(event['type'] in manipulationTypeEvents):
                        manipulationEvents[user_puzzle_key] +=1

                    # Time check solution
                    if(event['type'] == 'ws-check_solution'):
                        timeCheckActual[user_puzzle_key] = event['time']
                        contCheckSol[user_puzzle_key] +=1
                        if(user_puzzle_key not in avgTime_start_check.keys()): avgTime_start_check[user_puzzle_key] = 0
                        else: avgTime_start_check[user_puzzle_key] += (timeCheckActual[user_puzzle_key] - timestamp[user_puzzle_key]).total_seconds()


            # the puzzle ends        
            if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-disconnect'] ):
                if(user_puzzle_key in completed.keys() and completed[user_puzzle_key] != 1):
                    n_attempts[user_puzzle_key] +=1
                    puzzleEvents[user_puzzle_key] += 1
                    userDissconect[user_puzzle_key] = 1

                    # To complete events, time and attempts    
                    if(event['type'] in ['ws-puzzle_complete']):     
                        sep = user_puzzle_key.split('~')
                        if(sep[2] not in 'Sandbox'):
                            checkSolProd[user_puzzle_key] = contCheckSol[user_puzzle_key]
                            timePuzzleProd[user_puzzle_key] = timePuzzle[user_puzzle_key]  
                            puzzleEventsProd[user_puzzle_key] = puzzleEvents[user_puzzle_key] 

                        completed[user_puzzle_key] = 1        

                    # Calculate average time between submits and time between submit and exit
                    if(completed[user_puzzle_key] == 0 and globalTypesEvents[user_puzzle_key]['ws-check_solution'] > 0):
                        timeSubExit[user_puzzle_key] = str(round((event['time'] - timeFirstCheck[user_puzzle_key]).total_seconds(), 2))
                    else: timeSubExit[user_puzzle_key] = 'NA'  

                    if((globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 0) or (avgTime_start_check[user_puzzle_key]==0)): timeBetweenSub[user_puzzle_key] = 'NA'      
                    else: timeBetweenSub[user_puzzle_key] = str(round(avgTime_start_check[user_puzzle_key] /globalTypesEvents[user_puzzle_key]['ws-check_solution'], 2))


                    #Calculate the duration of the event                                                                          
                    delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                    if((delta_seconds < theresHoldActivity)):
                        timePuzzle[user_puzzle_key] += delta_seconds

                    #Breaks
                    if((delta_seconds > 15)):
                        breaksPuzzle[user_puzzle_key] += 1

                    previousEvent = event


    userTime = dict()
    userAtt = dict()
    userEvent = dict()
    
    userTimeProd = dict()
    userAttProd = dict()
    userEventProd = dict()
    
    # Save attempts, time and events productive and inproductive
    for i in puzzleEvents.keys():

        if(userDissconect[i] != 1): 
            n_attempts[i] += 1
        
        key_split = i.split('~')
        if(key_split[1] not in userTime.keys()):
            userTime[i] = 0
            userAtt[i] = 0
            userEvent[i] = 0
                
        if(key_split[2] != ''):

            if(key_split[2] in allPuzzles): 
                userAtt[i] = contCheckSol[i]
                
                if(i in checkSolProd.keys()):
                    userAttProd[i] = checkSolProd[i]

            if(key_split[2] in allPuzzles): 
                
                userTime[i] = timePuzzle[i]
                if(i in timePuzzleProd.keys()):
                    userTimeProd[i] = timePuzzleProd[i]

            if(key_split[2] in allPuzzles): 
                
                userEvent[i] = puzzleEvents[i] 
                if(i in puzzleEventsProd.keys()):
                    userEventProd[i] = puzzleEventsProd[i]


    puzzleTime = dict()
    puzzleAtt = dict()  
    puzzleEvent = dict()
    
    puzzleTimeProd = dict()
    puzzleAttProd = dict()  
    puzzleEventProd = dict()
    
    # Save the attempts, events and time per puzzle for the distribution (productive and unproductive)
    for i in userTime.keys():
        
        key_split = i.split('~')
        if(key_split[2] not in allPuzzles): continue
        if(key_split[2] not in puzzleTime.keys()):
            puzzleTime[key_split[2]] = []
            puzzleAtt[key_split[2]] = []
            puzzleEvent[key_split[2]] = []
            
        if(key_split[2] not in puzzleTimeProd.keys()):
            puzzleTimeProd[key_split[2]] = []
            puzzleAttProd[key_split[2]] = []
            puzzleEventProd[key_split[2]] = []    

        puzzleTime[key_split[2]].append(userTime[i])
        puzzleAtt[key_split[2]].append(userAtt[i])
        puzzleEvent[key_split[2]].append(userEvent[i])
        
        if(i in userTimeProd.keys()):
            puzzleTimeProd[key_split[2]].append(userTimeProd[i])
            puzzleAttProd[key_split[2]].append(userAttProd[i])
            puzzleEventProd[key_split[2]].append(userEventProd[i])


    difficultyNumber = dict()
    persistent = dict()
    
    percentileActiveTime = dict()
    percentileAtt = dict()
    percentileEvent = dict()
    
    percentileActiveTimeProd = dict()
    percentileAttProd = dict()
    percentileEventProd = dict()
    
    percentileCompositeProd = dict()
    averagePercentileComposite = dict()
    averagePercentilePartial = dict()

    compositeUserProd = dict()


    for i in puzzleEvents.keys():
        
        key_split = i.split('~')
        if(key_split[2] not in ['', 'Sandbox']):

            # Difficulty puzzle
            difficultyNumber[i] = difficultyPuzzles[key_split[2]]

            if(key_split[1] not in compositeUserProd.keys()):
                compositeUserProd[key_split[1]]= []
            
            # General percentile
            percentileActiveTime[i] = stats.percentileofscore(puzzleTime[key_split[2]], userTime[i])
            percentileAtt[i] = stats.percentileofscore(puzzleAtt[key_split[2]], userAtt[i], kind='weak')
            percentileEvent[i] = stats.percentileofscore(puzzleEvent[key_split[2]], userEvent[i], kind='weak')
            
            # Productive percentile
            percentileActiveTimeProd[i] = stats.percentileofscore(puzzleTimeProd[key_split[2]], userTime[i])
            percentileAttProd[i] = stats.percentileofscore(puzzleAttProd[key_split[2]], userAtt[i], kind='weak')
            percentileEventProd[i] = stats.percentileofscore(puzzleEventProd[key_split[2]], userEvent[i], kind='weak')
            #percentileCompositeProd[i] = (stats.percentileofscore(puzzleTimeProd[key_split[2]], userTime[i], kind='weak') + stats.percentileofscore(puzzleEventProd[key_split[2]], userEvent[i], kind='weak')) / 2
            percentileCompositeProd[i] = (percentileActiveTimeProd[i] + percentileEventProd[i]) / 2
            #percentileCompositeProd[i] = (stats.percentileofscore(puzzleTimeProd[key_split[2]], userTime[i], kind='weak') + stats.percentileofscore(puzzleAttProd[key_split[2]], userAtt[i], kind='weak') + stats.percentileofscore(puzzleEventProd[key_split[2]], userEvent[i], kind='weak')) / 3
            compositeUserProd[key_split[1]].append(percentileCompositeProd[i])

            # Initialize persistent structure
            if(i not in persistent.keys()):
                persistent[i] = ''

            # Persistent labels
            if(percentileCompositeProd[i] < 5 and completed[i] == 0):
                persistent[i] = 'NON_PERSISTANT'
                
            
            if(percentileCompositeProd[i] < 25 and completed[i] == 1):
                persistent[i] = 'RAPID_SOLVER'
                

            if(percentileCompositeProd[i] > 75 and completed[i] == 1):
                persistent[i] = 'PRODUCTIVE_PERSISTANCE'
                

            if(percentileCompositeProd[i] > 90 and completed[i] == 0):
                persistent[i] = 'UNPRODUCTIVE_PERSISTANCE'   
                

            if(persistent[i] == ''):
                persistent[i] = 'NO_BEHAVIOR'  
 
    resultPart = 0
    for i in puzzleEvents.keys():

        key_split = i.split('~')
        if(key_split[2] not in ['', 'Sandbox'] and key_split[1] != '' and i != ''):
            activity_by_user.at[i, 'user'] = key_split[1]
            activity_by_user.at[i, 'group'] = key_split[0]
            activity_by_user.at[i, 'task_id'] = key_split[2]            
            activity_by_user.at[i, 'n_events'] = puzzleEvents[i]
            activity_by_user.at[i, 'active_time'] = round(timePuzzle[i],2)
            activity_by_user.at[i, 'percentileAtt'] = round(percentileAttProd[i],2)
            activity_by_user.at[i, 'percentileActiveTime'] = round(percentileActiveTimeProd[i],2)
            activity_by_user.at[i, 'percentileEvents'] = round(percentileEventProd[i],2)
            activity_by_user.at[i, 'percentileComposite'] = round(percentileCompositeProd[i],2)
            activity_by_user.at[i, 'completed'] = completed[i]
            activity_by_user.at[i, 'puzzle_difficulty'] = difficultyNumber[i]
            activity_by_user.at[i, 'puzzle_category'] = categoryPuzz[i]
            activity_by_user.at[i, 'n_attempts'] = n_attempts[i]
            activity_by_user.at[i, 'timestamp'] = timestamp[i]
            activity_by_user.at[i, 'persistence'] = persistent[i]
            activity_by_user.at[i, 'n_breaks'] = breaksPuzzle[i]
            activity_by_user.at[i, 'n_manipulation_events'] = manipulationEvents[i]           
            activity_by_user.at[i, 'time_failed_submission_exit'] = timeSubExit[i]
            activity_by_user.at[i, 'avg_time_between_submissions'] = timeBetweenSub[i]
            activity_by_user.at[i, 'n_check_solution'] = globalTypesEvents[i]['ws-check_solution']
            activity_by_user.at[i, 'n_snapshot'] = globalTypesEvents[i]['ws-snapshot']
            activity_by_user.at[i, 'n_rotate_view'] = globalTypesEvents[i]['ws-rotate_view']

    #delete row with NaN
    activity_by_user.dropna(subset = ['user'], inplace=True)


    #data output preparation                                                                                          
    activity_by_user = pd.DataFrame(activity_by_user, columns=['group', 'user','task_id','puzzle_difficulty' ,'puzzle_category','n_attempts','completed','timestamp', 'active_time','percentileActiveTime','n_events','percentileEvents', 'n_check_solution','percentileAtt','percentileComposite' ,'persistence','n_breaks','n_snapshot','n_rotate_view','n_manipulation_events','time_failed_submission_exit','avg_time_between_submissions',])

    return activity_by_user




