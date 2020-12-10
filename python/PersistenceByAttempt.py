
import pandas as pd
from collections import OrderedDict
from datetime import datetime
import numpy as np
import json
import statistics
from scipy import stats
from datetime import timedelta

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/jruipere/Dropbox (MIT)/Game-based Assessment/data_processing_scripts/data/anonymized_dataset.csv', sep=";")
# dataEvents = pd.read_csv('/Users/pedroantonio/Desktop/data/anonymized_dataset.csv', sep=";")
# persistence = computePersistenceByAttempt(dataEvents)

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


def computePersistenceByAttempt(dataEvents, group = 'all'):

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
    #if(group != 'all'):
    #    dataEvents = dataEvents[dataEvents['group'].isin(group)]

        # the data is grouped by the necessary variables
    activity_by_user = dataEvents.groupby(['group_user_id']).agg({'id':'count',
                                                 'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                                  'type':'different_events'})


           # Data Cleaning
        #dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')

    typeEvents = ['ws-snapshot','ws-paint', 'ws-rotate_view','ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape','ws-undo_action','ws-redo_action', 'ws-check_solution']
    manipulationTypeEvents = ['ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape']



        #initialize the metrics
    activity_by_user['completed'] = np.nan
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_events'] = np.nan
    activity_by_user['timestamp'] = np.nan


    for event in typeEvents:
        activity_by_user[event] = 0

    #initialize the data structures
    puzzleEvents = dict()
    timePuzzle = dict()
    globalTypesEvents = dict()
    n_attempts = dict()
    completados = dict()
    timestamp = dict()

    percentilAtt = dict()
    percentilTime = dict()

    percentilAttValue = 90
    percentilTimeValue = 90

    breaksPuzzle = dict()
    cumAttempts = OrderedDict()
    puzzleAttempts = dict()
    userCumAttempts = OrderedDict()
    puzzleCumAttempts = dict()
    prevReg = dict()
    actualAtt = 0
    prevAtt = 0
    idComplete = dict()
    contParc = dict()
    orden = []
    ids = []
    attemptsAux = dict()

    contCheckSol = dict()

    manipulationEvents = dict()
    userManipulationEvents = dict()
    contManipulation = 0
    timeFirstCheck = dict()
    timeSubExit = dict()
    timeCheckActual = dict()
    timeBetweenSub = dict()


    for user in dataEvents['group_user_id'].unique():

            # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []

        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None
        userParc = None
        task_id = None
        initialTime = None
        prev_id = 1

        for enum, event in user_events.iterrows():

                # If it is the first event
                if(previousEvent is None):
                    previousEvent = event
                    continue

                if( event['type'] in ['ws-start_level'] ):

                    #create id: group+user+task_id
                    task_id = json.loads(event['data'])['task_id']

                    if(user_puzzle_key not in timeSubExit.keys()):
                        timeSubExit[user_puzzle_key] = str(0)
                        timeBetweenSub[user_puzzle_key] = str(0)


                    if(event['user'] not in userCumAttempts.keys()):
                        userCumAttempts[event['user']] = 0
                        actualAtt = 0
                        attemptsAux[event['user']] = dict()
                        timeCheckActual[event['user']] = 0

                    if(event['user'] not in userManipulationEvents.keys()):
                        #print("Se inicializa con ", event['user'])
                        userManipulationEvents[event['user']] = 0


                    #if(user_puzzle_key not in manipulationEvents.keys()):
                    #    manipulationEvents[user_puzzle_key] = 0
                    #    contManipulation = 0

                    if(task_id not in attemptsAux[event['user']].keys()): attemptsAux[event['user']][task_id]=0

                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id# + '~' + str(n_attempts[prev_id])
                    if(user_puzzle_key not in prevReg.keys()):

                        prevReg[user_puzzle_key] = 1
                        user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id + '~' + '1'
                        n_attempts[user_puzzle_key] = 1
                        attemptsAux[event['user']][task_id] = n_attempts[user_puzzle_key]

                    else:

                        user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id + '~' + str(attemptsAux[event['user']][task_id])
                        n_attempts[user_puzzle_key] = attemptsAux[event['user']][task_id]


                    key_split = user_puzzle_key.split('~')
                    userParc = key_split[1]

                    if(user_puzzle_key not in idComplete.keys()): idComplete[user_puzzle_key] = 0


                    if(task_id not in attemptsAux[userParc].keys()): attemptsAux[userParc][task_id]=0
                    if(user_puzzle_key not in cumAttempts.keys()):
                        cumAttempts[user_puzzle_key] = 1


                    # initialize if the id is new
                    if(user_puzzle_key not in puzzleEvents.keys()):

                        breaksPuzzle[user_puzzle_key] = 0
                        timestamp[user_puzzle_key] = 0
                        percentilAtt[user_puzzle_key] = percentilAttValue
                        percentilTime[user_puzzle_key] = percentilTimeValue
                        completados[user_puzzle_key] = 0
                        puzzleEvents[user_puzzle_key]= 1
                        timePuzzle[user_puzzle_key] = 0
                        contCheckSol[user_puzzle_key] = 0
                        manipulationEvents[user_puzzle_key] = 0
                        timeFirstCheck[user_puzzle_key] = 0

                        globalTypesEvents[user_puzzle_key] = dict()
                        for ev in typeEvents:
                            globalTypesEvents[user_puzzle_key][ev]= 0




                    #timestamp
                    if(event['type'] in 'ws-start_level'):

                        timestamp[user_puzzle_key] = event['time']
                        initialTime = timestamp[user_puzzle_key]

                # the event is not final event
                if(event['type'] not in ['ws-exit_to_menu' , 'ws-disconnect', 'ws-create_user', 'ws-login_user']):



                        if(event['type'] in ['ws-puzzle_complete']): completados[user_puzzle_key] = 1

                        puzzleEvents[user_puzzle_key] += 1

                        #calculate the duration of the event
                        delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                        if((delta_seconds < theresHoldActivity)):
                            timePuzzle[user_puzzle_key] += delta_seconds

                        #breaks
                        if((delta_seconds > 15)):
                            breaksPuzzle[user_puzzle_key] += 1

                        previousEvent = event

                        #update event counters by type
                        if(event['type'] in typeEvents):
                            globalTypesEvents[user_puzzle_key][event['type']] +=1

                        if(globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 1): timeFirstCheck[user_puzzle_key] = event['time']


                        if(event['type'] in manipulationTypeEvents):
                            manipulationEvents[user_puzzle_key] +=1

                        if(event['type'] == 'ws-check_solution'):
                            timeCheckActual[event['user']] = event['time']
                            contCheckSol[user_puzzle_key] +=1


                # the puzzle ends
                if(event['type'] in ['ws-exit_to_menu', 'ws-disconnect']):

                        idComplete[user_puzzle_key] = 1
                        puzzleEvents[user_puzzle_key] += 1


                        if(completados[user_puzzle_key] == 0 and globalTypesEvents[user_puzzle_key]['ws-check_solution'] > 0):
                            timeSubExit[user_puzzle_key] = str(round((event['time'] - timeFirstCheck[user_puzzle_key]).total_seconds(), 2))
                        else: timeSubExit[user_puzzle_key] = 'NA'

                        if(globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 0): timeBetweenSub[user_puzzle_key] = 'NA'
                        else: timeBetweenSub[user_puzzle_key] = str(round(((timeCheckActual[event['user']] - timestamp[user_puzzle_key]) /globalTypesEvents[user_puzzle_key]['ws-check_solution']).total_seconds(), 2))


                        #calculate the duration of the event
                        delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                        if((delta_seconds < theresHoldActivity)):
                            timePuzzle[user_puzzle_key] += delta_seconds

                        #breaks
                        if((delta_seconds > 15)):
                            breaksPuzzle[user_puzzle_key] += 1


                        previousEvent = event

                        userCumAttempts[userParc] +=1
                        n_attempts[user_puzzle_key] +=1
                        actualAtt+=1
                        cumAttempts[user_puzzle_key] = actualAtt
                        attemptsAux[userParc][task_id] = n_attempts[user_puzzle_key]

                        #manipulationEvents[user_puzzle_key] = userManipulationEvents[event['user']]


                        ###########


    userTime = dict()
    userAtt = dict()
    userEvent = dict()


    for i in puzzleEvents.keys():
        if(idComplete[i]==0):
            continue
        key_split = i.split('~')
        if(key_split[1] not in userTime.keys()):
            userTime[i] = 0
            userAtt[i] = 0
            userEvent[i] = 0

        if(key_split[2] != ''):

            if(key_split[2] in allPuzzles): userAtt[i] = contCheckSol[i]

            if(key_split[2] in allPuzzles): userTime[i] = timePuzzle[i]

            if(key_split[2] in allPuzzles): userEvent[i] = puzzleEvents[i]


    puzzleTime = dict()
    puzzleAtt = dict()
    puzzleEvent = dict()

    for i in userTime.keys():
        #for puzzle in userTime[user]:
        key_split = i.split('~')
        if(key_split[2] not in puzzleTime.keys()):
            puzzleTime[key_split[2]] = []
            puzzleAtt[key_split[2]] = []
            puzzleEvent[key_split[2]] = []

        puzzleTime[key_split[2]].append(userTime[i])
        puzzleAtt[key_split[2]].append(userAtt[i])
        puzzleEvent[key_split[2]].append(userEvent[i])


    persistent = dict()
    percentileActiveTime = dict()
    percentileAtt = dict()
    percentileEvent = dict()
    percentileComposite = dict()
    averagePercentileComposite = dict()
    averagePercentilePartial = dict()

    difficultyNumber = dict()

    contNonPer = dict()
    totalNonPer = dict()
    contRapid = dict()
    totalRapid = dict()
    contUnpro = dict()
    totalUnpro = dict()
    contProduct = dict()
    totalProduct=dict()
    contNoBehavior = dict()
    totalNoBehavior = dict()
    persistantCumPerc = dict()

    cumDifficulty = dict()
    cumUserPercentage = dict()

    diffPercentage = dict()

    compositeUser = dict()


    for i in puzzleEvents.keys():
        if(idComplete[i]==0):
            continue
        key_split = i.split('~')
        if(key_split[2] != ''):

            #difficulty
            difficultyNumber[i] = difficultyPuzzles[key_split[2]]
            if(i not in diffPercentage.keys()): diffPercentage[i] = 0

            if(key_split[1] not in contNonPer.keys()):
                contNonPer[key_split[1]] = 0
                totalNonPer[key_split[1]] =0
                contRapid[key_split[1]]=0
                totalRapid[key_split[1]]=0
                contUnpro[key_split[1]]=0
                totalUnpro[key_split[1]]=0
                contProduct[key_split[1]]=0
                totalProduct[key_split[1]]=0
                contNoBehavior[key_split[1]]=0
                totalNoBehavior[key_split[1]]=0
                cumDifficulty[key_split[1]]=0
                compositeUser[key_split[1]]= []
                cumUserPercentage[key_split[1]]=0

            percentileActiveTime[i] = stats.percentileofscore(puzzleTime[key_split[2]], userTime[i])
            percentileAtt[i] = stats.percentileofscore(puzzleAtt[key_split[2]], userAtt[i], kind='weak')
            percentileEvent[i] = stats.percentileofscore(puzzleEvent[key_split[2]], userEvent[i], kind='weak')
            percentileComposite[i] = (stats.percentileofscore(puzzleTime[key_split[2]], userTime[i], kind='weak') + stats.percentileofscore(puzzleAtt[key_split[2]], userAtt[i], kind='weak') + stats.percentileofscore(puzzleEvent[key_split[2]], userEvent[i], kind='weak')) / 3
            compositeUser[key_split[1]].append(percentileComposite[i])

            cumDifficulty[key_split[1]] = cumDifficulty[key_split[1]] + difficultyPuzzles[key_split[2]]
            cumUserPercentage[key_split[1]] = cumUserPercentage[key_split[1]] + (difficultyPuzzles[key_split[2]] * percentileComposite[i])
            diffPercentage[i] = cumUserPercentage[key_split[1]] / cumDifficulty[key_split[1]]

            if(key_split[1] not in averagePercentilePartial.keys()): averagePercentilePartial[key_split[1]]=0
            if(i not in averagePercentileComposite.keys()): averagePercentileComposite[i]=0

            if(cumAttempts[i] == 0): averagePercentileComposite[i] = averagePercentileComposite[i]
            else:

                averagePercentilePartial[key_split[1]] = averagePercentilePartial[key_split[1]] + percentileComposite[i]
                averagePercentileComposite[i] = averagePercentilePartial[key_split[1]] / cumAttempts[i]

            if(i not in persistent.keys()):
                persistent[i] = ''


            if(percentileComposite[i] < 25 and completados[i] == 0):
                persistent[i] = 'NON_PERSISTANT'
                contNonPer[key_split[1]] +=1

            if(percentileComposite[i] < 25 and completados[i] == 1):
                persistent[i] = 'RAPID_SOLVER'
                contRapid[key_split[1]]+=1

            if(percentileComposite[i] > 75 and completados[i] == 1):
                persistent[i] = 'PRODUCTIVE_PERSISTANCE'
                contProduct[key_split[1]]+=1

            if(percentileComposite[i] > 75 and completados[i] == 0):
                persistent[i] = 'UNPRODUCTIVE_PERSISTANCE'
                contUnpro[key_split[1]]+=1

            if(percentileComposite[i] >= 25 and percentileComposite[i] <= 75):
                persistent[i] = 'NO_BEHAVIOR'
                contNoBehavior[key_split[1]]+=1

            if(contNonPer[key_split[1]] == 0 or cumAttempts[i]==0):
                totalNonPer[key_split[1]] =0
            else:
                totalNonPer[key_split[1]] = 100 * (contNonPer[key_split[1]] / cumAttempts[i])

            if(contRapid[key_split[1]] == 0 or cumAttempts[i]==0): totalRapid[key_split[1]] =0
            else: totalRapid[key_split[1]] = 100 * (contRapid[key_split[1]] / cumAttempts[i])

            if(contProduct[key_split[1]] == 0 or cumAttempts[i]==0): contProduct[key_split[1]] =0
            else: totalProduct[key_split[1]] = 100 * (contProduct[key_split[1]] / cumAttempts[i])

            if(contUnpro[key_split[1]] == 0 or cumAttempts[i]==0): contUnpro[key_split[1]] =0
            else: totalUnpro[key_split[1]] = 100 * (contUnpro[key_split[1]] / cumAttempts[i])

            if(contNoBehavior[key_split[1]] == 0 or cumAttempts[i]==0): contNoBehavior[key_split[1]] =0
            else: totalNoBehavior[key_split[1]] = 100 * (contNoBehavior[key_split[1]] / cumAttempts[i])

            persistantCumPerc[i] = json.dumps({"NON_PERSISTANT ": round(totalNonPer[key_split[1]],2), "RAPID_SOLVER": round(totalRapid[key_split[1]],2), "PRODUCTIVE_PERSISTANCE": round(totalProduct[key_split[1]],2), "UNPRODUCTIVE_PERSISTANCE": round(totalUnpro[key_split[1]],2), "NO_BEHAVIOR": round(totalNoBehavior[key_split[1]],2) })




    resultPart = 0
    for i in puzzleEvents.keys():
        if(idComplete[i]==0):
            continue
        key_split = i.split('~')
        if(key_split[2] != '' and key_split[1] != '' and i != ''):
            activity_by_user.at[i, 'user'] = key_split[1]
            activity_by_user.at[i, 'group'] = key_split[0]
            activity_by_user.at[i, 'task_id'] = key_split[2]
            activity_by_user.at[i, 'n_events'] = puzzleEvents[i]
            activity_by_user.at[i, 'active_time'] = round(timePuzzle[i],2)
            activity_by_user.at[i, 'percentileAtt'] = round(percentileAtt[i],2)
            activity_by_user.at[i, 'percentileActiveTime'] = round(percentileActiveTime[i],2)
            activity_by_user.at[i, 'percentileEvents'] = round(percentileEvent[i],2)
            activity_by_user.at[i, 'percentileComposite'] = round(percentileComposite[i],2)
            activity_by_user.at[i, 'completed'] = completados[i]
            activity_by_user.at[i, 'puzzle_difficulty'] = difficultyNumber[i]
            activity_by_user.at[i, 'cum_global_puzzle_attempts'] = cumAttempts[i]
            activity_by_user.at[i, 'cum_this_puzzle_attempt'] = key_split[3]
            activity_by_user.at[i, 'cum_avg_perc_composite'] = round(averagePercentileComposite[i],2)
            activity_by_user.at[i, 'cum_avg_persistence'] = persistantCumPerc[i]
            activity_by_user.at[i, 'timestamp'] = timestamp[i]
            activity_by_user.at[i, 'persistence'] = persistent[i]
            activity_by_user.at[i, 'n_breaks'] = breaksPuzzle[i]
            activity_by_user.at[i, 'n_manipulation_events'] = manipulationEvents[i]
            activity_by_user.at[i, 'cum_weighted_difficulty_perc_composite'] = round(diffPercentage[i],2)
            resultPart = stats.percentileofscore(compositeUser[key_split[1]], percentileComposite[i])
            activity_by_user.at[i, 'percentileCompositeAcrossAttempts'] = round(resultPart,2)
            if(resultPart >= 75): activity_by_user.at[i, 'persistenceAcrossAttempts'] = 'MORE_PERSISTANCE_THAN_NORMAL'
            if(resultPart < 75 and resultPart>25): activity_by_user.at[i, 'persistenceAcrossAttempts'] = 'NORMAL_PERSISTANCE'
            if(resultPart <= 25): activity_by_user.at[i, 'persistenceAcrossAttempts'] = 'LESS_PERSISTANCE_THAN_NORMAL'
            activity_by_user.at[i, 'time_failed_submission_exit'] = timeSubExit[i]
            activity_by_user.at[i, 'avg_time_between_submissions'] = timeBetweenSub[i]
            activity_by_user.at[i, 'n_check_solution'] = globalTypesEvents[i]['ws-check_solution']
            activity_by_user.at[i, 'n_snapshot'] = globalTypesEvents[i]['ws-snapshot']
            activity_by_user.at[i, 'n_rotate_view'] = globalTypesEvents[i]['ws-rotate_view']

    #delete row with NaN
    activity_by_user.dropna(subset = ['user'], inplace=True)


    #data output preparation
    activity_by_user = pd.DataFrame(activity_by_user, columns=['group', 'user','task_id','puzzle_difficulty' ,'completed','timestamp', 'active_time','percentileActiveTime','n_events','percentileEvents', 'n_check_solution','percentileAtt','percentileComposite' ,'persistence','n_breaks','n_snapshot','n_rotate_view','n_manipulation_events','time_failed_submission_exit','avg_time_between_submissions','cum_weighted_difficulty_perc_composite','percentileCompositeAcrossAttempts','persistenceAcrossAttempts','cum_global_puzzle_attempts','cum_this_puzzle_attempt','cum_avg_perc_composite', 'cum_avg_persistence'])

    return activity_by_user
