#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import datetime
import numpy as np
from scipy import stats
import json

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/manuelgomezmoratilla/Desktop/TFG/data_processing_scripts/data/anonamyze_all_data_collection_v2.csv', sep=";")
# df2 = computePlayStyles(dataEvents)

def computePlayStyles(dataEvents,  puzzles = 'all', group = 'all'):
    if puzzles == 'all':
        puzzles = ['Square Cross-Sections', 'Bird Fez', 'Pi Henge', '45-Degree Rotations',  'Pyramids are Strange', 'Boxes Obscure Spheres', 'Object Limits', 'Angled Silhouette',
                  'Sugar Cones','Stranger Shapes', 'Tall and Small', 'Ramp Up and Can It', 'More Than Meets Your Eye', 'Not Bird', 'Zzz', 'Bull Market', 'Orange Dance', 'Bear Market']

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
    
    newEvents = []
    for user in dataEvents['group_user_id'].unique():
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_events_na_dropped = user_events.dropna()
        activePuzzle = None
        nAttempt = 1
        for enum, event in user_events_na_dropped.iterrows():
            if(event['type'] == 'ws-start_level'):
                activePuzzle = json.loads(event['data'])['task_id']
                if activePuzzle == None :
                    continue
                event['task_id'] = activePuzzle
                event['n_attempt'] = nAttempt
                event['group_user_task_id'] = event['group'] + '~' + event['user'] + '~' + event['task_id'] + '~' + str(nAttempt)
                if (event['task_id'] == 'Sandbox'):
                    continue
                newEvents.append(event)
            elif(event['type'] in ['ws-exit_to_menu', 'ws-disconnect', 'ws-login_user']):
                if (activePuzzle == 'Sandbox'):
                    continue
                event['task_id'] = activePuzzle
                if activePuzzle == None :
                    continue
                event['n_attempt'] = nAttempt
                event['group_user_task_id'] = event['group'] + '~' + event['user'] + '~' + event['task_id'] + '~' + str(nAttempt)
                nAttempt +=1
                newEvents.append(event)
            else:
                if (activePuzzle == 'Sandbox'):
                    continue
                event['task_id'] = activePuzzle
                if activePuzzle == None :
                    continue
                event['n_attempt'] = nAttempt
                event['group_user_task_id'] = event['group'] + '~' + event['user'] + '~' + event['task_id'] + '~' + str(nAttempt)
                newEvents.append(event)
                
    initialDf = pd.DataFrame(newEvents, columns=['id', 'time', 'group_user_id', 'group', 'user', 'group_user_task_id', 'n_attempt', 'task_id', 'type', 'data'])
    #Recalculate n_attempt
    mod = []
    for user in initialDf['user'].unique():
            previousAttempt = 1
            n_attempt = 1
            individualDf = initialDf[initialDf['user'] == user]
            for enum, event in individualDf.iterrows():
                if (event['n_attempt'] != previousAttempt):
                    n_attempt += 1
                previousAttempt = event['n_attempt']
                event['n_attempt'] = n_attempt
                mod.append(event)
    modDf = pd.DataFrame(mod, columns=['id', 'time', 'group_user_id', 'group', 'user', 'group_user_task_id', 'n_attempt', 'task_id', 'type', 'data'])
    
    initialDf = modDf
    
    data_car = []
    for puzzle in puzzles:
        initialDfP = initialDf[initialDf['task_id'] == puzzle]
        # the data is grouped by the necessary variables      
        activity_by_user = initialDfP.groupby(['group_user_id','group', 'user','group_user_task_id','task_id', 'n_attempt']).agg({'id':'count',
                                                 'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                                  'type':'different_events'}) 

        #indicate the index variable                                                                                                                                                               
        activity_by_user.index = activity_by_user['group_user_task_id'].values

        #initialize the metrics                                                                                          
        activity_by_user['event'] = np.nan
        activity_by_user['different_events'] = np.nan
        activity_by_user['active_time'] = np.nan
        activity_by_user['snapshot'] = 0.0
        activity_by_user['paint'] = 0
        activity_by_user['rotate_view'] = 0.0
        activity_by_user['move_shape'] = 0
        activity_by_user['scale_shape'] = 0
        activity_by_user['create_shape'] = 0
        activity_by_user['delete_shape'] = 0
        activity_by_user['undo_action'] = 0
        activity_by_user['redo_action'] = 0
        activity_by_user['restart_puzzle'] = 0
        activity_by_user['submit_action'] = 0.0
        activity_by_user['manipulation_events'] = 0.0
        activity_by_user['firstShape_time'] = 0.0

        #initialize the data structures
        userFunnelDict = dict()  
        puzzleEvents = dict()
        eventsDiff = []
        eventsDiff_puzzle = dict()
        timePuzzle = dict()
        globalTypesEvents = dict()
        typesEvents = dict()

        for user in initialDfP['group_user_id'].unique():

            # Computing active time
            previousEvent = None
            theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
            activeTime = []

            user_events = initialDfP[initialDfP['group_user_id'] == user]
            user_puzzle_key = None

            for enum, event in user_events.iterrows():

                # If it is the first event
                if(previousEvent is None):
                    previousEvent = event
                    continue

                if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):

                    #create id: group+user+task_id                                                                              
                    user_puzzle_key =  event['group_user_task_id']
                    firstEvent = event
                    shapeCreated = False

                    # initialize if the id is new                                                                              
                    if(user_puzzle_key not in puzzleEvents.keys()):
                        puzzleEvents[user_puzzle_key]= 1
                        eventsDiff_puzzle[user_puzzle_key] = []
                        timePuzzle[user_puzzle_key] = 0
                        
                        globalTypesEvents[user_puzzle_key] = dict()
                        globalTypesEvents[user_puzzle_key]['ws-snapshot'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-paint'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-rotate_view'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-move_shape'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-scale_shape'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-create_shape'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-delete_shape'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-undo_action'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-redo_action'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-check_solution'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-man_events'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-restart_puzzle'] = 0
                        globalTypesEvents[user_puzzle_key]['ws-firstShape_time'] = 0.0
                        globalTypesEvents[user_puzzle_key]['ws-puzzle_complete'] = False


                        eventsDiff_puzzle[user_puzzle_key].append(event['type'])

                # the event is not final event
                if(event['type'] not in ['ws-exit_to_menu', 'ws-create_user', 'ws-login_user']): 

                        puzzleEvents[user_puzzle_key] += 1

                        #add the event type                                                                          
                        eventsDiff_puzzle[user_puzzle_key].append(event['type'])

                        #calculate the duration of the event                                                                          
                        delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                        if((delta_seconds < theresHoldActivity)):
                            timePuzzle[user_puzzle_key] += delta_seconds

                        previousEvent = event 

                        #update event counters by type                                                                          
                        if(event['type'] == 'ws-snapshot'):
                            globalTypesEvents[user_puzzle_key]['ws-snapshot'] +=1
                            globalTypesEvents[user_puzzle_key]['ws-man_events'] +=1
                        elif(event['type'] == 'ws-rotate_view'):
                            globalTypesEvents[user_puzzle_key]['ws-rotate_view'] +=1 
                        elif(event['type'] == 'ws-paint'):
                            globalTypesEvents[user_puzzle_key]['ws-paint'] +=1 
                        elif(event['type'] == 'ws-move_shape'):
                            globalTypesEvents[user_puzzle_key]['ws-move_shape'] +=1 
                            globalTypesEvents[user_puzzle_key]['ws-man_events'] +=1
                        elif(event['type'] == 'ws-scale_shape'):
                            globalTypesEvents[user_puzzle_key]['ws-scale_shape'] +=1
                            globalTypesEvents[user_puzzle_key]['ws-man_events'] +=1
                        elif(event['type'] == 'ws-create_shape'):
                            globalTypesEvents[user_puzzle_key]['ws-create_shape'] +=1
                            globalTypesEvents[user_puzzle_key]['ws-man_events'] +=1
                            if shapeCreated == False:
                                shapeCreated = True
                                delta_shape = (event['time'] - firstEvent['time']).total_seconds()
                                globalTypesEvents[user_puzzle_key]['ws-firstShape_time'] = delta_shape
                        elif(event['type'] == 'ws-delete_shape'):
                            globalTypesEvents[user_puzzle_key]['ws-delete_shape'] +=1
                            globalTypesEvents[user_puzzle_key]['ws-man_events'] +=1
                        elif(event['type'] == 'ws-undo_action'):
                            globalTypesEvents[user_puzzle_key]['ws-undo_action'] +=1
                        elif(event['type'] == 'ws-redo_action'):
                            globalTypesEvents[user_puzzle_key]['ws-redo_action'] +=1 
                        elif(event['type'] == 'ws-check_solution'):
                            globalTypesEvents[user_puzzle_key]['ws-check_solution'] +=1 
                        elif(event['type'] == 'ws-restart_puzzle'):
                            globalTypesEvents[user_puzzle_key]['ws-restart_puzzle'] +=1 
                        elif(event['type'] == 'ws-puzzle_complete'):
                            globalTypesEvents[user_puzzle_key]['ws-puzzle_complete'] = True

                # the puzzle ends        
                if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete']):
                        

                        #add the event type                                                                         
                        eventsDiff_puzzle[user_puzzle_key].append(event['type'])

                        #calculate the duration of the event                                                                          
                        delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                        if((delta_seconds < theresHoldActivity)):
                            timePuzzle[user_puzzle_key] += delta_seconds

                        previousEvent = event
        
             
            
        # add the data by group_user_task_id 
        for i in activity_by_user['group_user_task_id'].unique():
            key_split = i.split('~')
            if(key_split[2] != ''):
                if puzzleEvents.get(i) != None:
                    activity_by_user.at[i, 'event'] = puzzleEvents[i]
                    activity_by_user.at[i, 'different_events'] = len(set(eventsDiff_puzzle[i]))
                    activity_by_user.at[i, 'active_time'] = timePuzzle[i]
                    if (globalTypesEvents[i]['ws-man_events'] > 0):
                        activity_by_user.at[i, 'snapshot'] = globalTypesEvents[i]['ws-snapshot'] / globalTypesEvents[i]['ws-man_events']
                    else:
                        activity_by_user.at[i, 'snapshot'] = 0
                    activity_by_user.at[i, 'paint'] = globalTypesEvents[i]['ws-paint']
                    if (globalTypesEvents[i]['ws-man_events'] + globalTypesEvents[i]['ws-rotate_view']) > 0 :
                        activity_by_user.at[i, 'rotate_view'] = globalTypesEvents[i]['ws-rotate_view'] / (globalTypesEvents[i]['ws-man_events'] + globalTypesEvents[i]['ws-rotate_view'])
                    else:
                        activity_by_user.at[i, 'rotate_view'] = 0
                    activity_by_user.at[i, 'move_shape'] = globalTypesEvents[i]['ws-move_shape']
                    activity_by_user.at[i, 'scale_shape'] = globalTypesEvents[i]['ws-scale_shape']
                    activity_by_user.at[i, 'create_shape'] = globalTypesEvents[i]['ws-create_shape']
                    activity_by_user.at[i, 'delete_shape'] = globalTypesEvents[i]['ws-delete_shape']
                    activity_by_user.at[i, 'undo_action'] = globalTypesEvents[i]['ws-undo_action']
                    activity_by_user.at[i, 'redo_action'] = globalTypesEvents[i]['ws-redo_action']
                    activity_by_user.at[i, 'restart_puzzle'] = globalTypesEvents[i]['ws-restart_puzzle']
                    
                    if (globalTypesEvents[i]['ws-man_events'] + globalTypesEvents[i]['ws-check_solution']) > 0:
                        activity_by_user.at[i, 'submit_events'] = globalTypesEvents[i]['ws-check_solution'] / (globalTypesEvents[i]['ws-man_events'] + globalTypesEvents[i]['ws-check_solution'])
                    else:
                        activity_by_user.at[i, 'submit_events'] = 0
                    activity_by_user.at[i, 'completed'] = globalTypesEvents[i]['ws-puzzle_complete']
                    if activity_by_user.at[i, 'active_time'] > 0 :
                        manByTime = globalTypesEvents[i]['ws-man_events'] / activity_by_user.at[i, 'active_time']
                    else:
                        manByTime = globalTypesEvents[i]['ws-man_events']
                    activity_by_user.at[i, 'manipulation_events'] = manByTime
                    activity_by_user.at[i, 'firstShape_time'] = globalTypesEvents[i]['ws-firstShape_time']
                    if activity_by_user.at[i, 'submit_events'] > 0 :
                        activity_by_user.at[i, 'eventsBySubmit'] =  activity_by_user.at[i, 'event'] / globalTypesEvents[i]['ws-check_solution']
                    else:
                        activity_by_user.at[i, 'eventsBySubmit'] =  0.0

    
        #delete row with NaN
        activity_by_user.dropna(inplace=True)
        #delete group_user_task_id column
        activity_by_user.drop(columns=['group_user_task_id'], inplace=True)

        #data output preparation                                                                                          
        activity_by_user = pd.melt(activity_by_user, id_vars=['group', 'user','task_id', 'n_attempt'], 
            value_vars=['event','different_events', 'active_time','snapshot','paint','rotate_view','move_shape','scale_shape','create_shape','delete_shape','undo_action','redo_action', 'submit_events', 'firstShape_time', 'manipulation_events', 'eventsBySubmit', 'completed', 'restart_puzzle'], 
            var_name='metric', value_name='value')

        df2 = activity_by_user
        for user in df2['user'].unique():
            data_user = df2[df2['user'] == user]
            for attempt in data_user['n_attempt'].unique():
                attempt_data = data_user[data_user['n_attempt'] == attempt]
                for metric in attempt_data['metric'].unique():
                    metric_data = attempt_data[attempt_data['metric'] == metric]
                    for enum, event in metric_data.iterrows():
                        event['unique_id'] = event['group'] + '~' + event['user'] + '~' +  event['task_id'] + '~' + str(event['n_attempt'])
                        data_car.append(event)
                        
    modDf = pd.DataFrame(data_car, columns=['group', 'unique_id', 'metric' ,'value'])
        
        #Define all dimensions
    spatialDimension = ['snapshot', 'rotate_view']
    experimentDimension = ['submit_events', 'manipulation_events']
    reflexDimension = ['firstShape_time', 'eventsBySubmit']
    exploreDimension = ['different_events', 'paint']
    engageDimension = ['active_time', 'event']
    errorRectDimension = ['delete_shape', 'restart_puzzle']
    persistenceDimension = ['active_time', 'submit_events']
    dimensions = [spatialDimension, experimentDimension, reflexDimension, exploreDimension, engageDimension, errorRectDimension, persistenceDimension]

    df3 = modDf
    df = df3.pivot(index='unique_id', columns='metric', values='value')
    newEvents = []
    for enum, event in df.iterrows():
        ev = event.copy()
        for dim in dimensions:
            ev[dim] = 0.0
            total = 0.0
            for feature in dim:
                total += event[feature]
            if dim == spatialDimension :
                ev['spatialDimension'] = total
            elif dim == experimentDimension:
                ev['experimentDimension'] = total
            elif dim == reflexDimension:
                ev['reflexDimension'] = total
            elif dim == exploreDimension:
                ev['exploreDimension'] = total
            elif dim == errorRectDimension:
                ev['errorRectDimension'] = total
            elif dim == persistenceDimension:
                ev['persistenceDimension'] = total
            else:
                ev['engageDimension'] = total
        newEvents.append(ev)

    new = pd.DataFrame(newEvents, columns = ['spatialDimension', 'experimentDimension', 'exploreDimension', 'reflexDimension', 'engageDimension', 'errorRectDimension', 'persistenceDimension', 'completed'])
  
    that = []
    for ind in new.index.values:
        ob = new.loc[ind,:].copy()
        key_split = ind.split('~')
        ob['group_id'] = key_split[0]
        ob['user'] = key_split[1]
        ob['task_id'] = key_split[2]
        ob['n_attempt'] = key_split[3]
        that.append(ob)
    thatDf = pd.DataFrame(that, columns=['group_id', 'user', 'task_id', 'n_attempt','spatialDimension', 'experimentDimension', 'exploreDimension', 'reflexDimension', 'engageDimension', 'errorRectDimension', 'persistenceDimension','completed']) 
    thatDf.reset_index(drop=True, inplace=True)
    
    finalList = []
    for puzzle in thatDf['task_id'].unique():
        newPuzzle = thatDf[thatDf['task_id'] == puzzle]
        percSpatialUp = np.percentile(newPuzzle['spatialDimension'], 85)
        percSpatialDown = np.percentile(newPuzzle['spatialDimension'], 15)

        percExploreUp = np.percentile(newPuzzle['exploreDimension'], 85)
        percExploreDown = np.percentile (newPuzzle['exploreDimension'], 15)

        percEngageUp = np.percentile(newPuzzle['engageDimension'], 85)
        percEngageDown = np.percentile(newPuzzle['engageDimension'], 15)

        percExperimentUp = np.percentile(newPuzzle['experimentDimension'], 85)
        percExperimentDown = np.percentile(newPuzzle['experimentDimension'], 15)

        percReflexUp = np.percentile(newPuzzle['reflexDimension'], 85)
        percReflexDown = np.percentile(newPuzzle['reflexDimension'], 15)

        percErrorRectUp = np.percentile(newPuzzle['errorRectDimension'], 85)
        percErrorRectDown = np.percentile(newPuzzle['errorRectDimension'], 15)
        
        percPersistenceUp = np.percentile(newPuzzle['persistenceDimension'], 85)
        percPersistenceDown = np.percentile(newPuzzle['persistenceDimension'], 15)

        for enum, event in newPuzzle.iterrows():
            event['percSpatialValue'] = stats.percentileofscore(newPuzzle['spatialDimension'], event['spatialDimension'])
            event['percEngageValue'] = stats.percentileofscore(newPuzzle['engageDimension'], event['engageDimension'])
            event['percExperimentValue'] = stats.percentileofscore(newPuzzle['experimentDimension'], event['experimentDimension'])
            event['percExploreValue'] = stats.percentileofscore(newPuzzle['exploreDimension'], event['exploreDimension'])
            event['percReflexValue'] = stats.percentileofscore(newPuzzle['reflexDimension'], event['reflexDimension'])
            event['percErrorRectValue'] = stats.percentileofscore(newPuzzle['errorRectDimension'], event['errorRectDimension'])
            event['percPersistenceValue'] = stats.percentileofscore(newPuzzle['persistenceDimension'], event['persistenceDimension'])
            
            if event['spatialDimension'] > percSpatialUp:
                event['PercentileSpatialAbove85'] = True
                event['PercentileSpatialBelow15'] = False
            elif event['spatialDimension'] < percSpatialDown:
                event['PercentileSpatialAbove85'] = False
                event['PercentileSpatialBelow15'] = True
            else:
                event['PercentileSpatialAbove85'] = False
                event['PercentileSpatialBelow15'] = False

            if event['exploreDimension'] > percExploreUp:
                event['PercentileExploreAbove85'] =  True
                event['PercentileExploreBelow15'] = False
            elif event['exploreDimension'] < percExploreDown:
                event['PercentileExploreBelow15'] = True
                event['PercentileExploreAbove85'] = False
            else:
                event['PercentileExploreBelow15'] = False
                event['PercentileExploreAbove85'] = False

            if event['experimentDimension'] > percExperimentUp:
                event['PercentileExperimentAbove85'] = True
                event['PercentileExperimentBelow15'] = False
            elif event['experimentDimension'] < percExperimentDown:
                event['PercentileExperimentAbove85'] = False
                event['PercentileExperimentBelow15'] = True
            else:
                event['PercentileExperimentAbove85'] = False
                event['PercentileExperimentBelow15'] = False

            if event['engageDimension'] > percEngageUp:
                event['PercentileEngageAbove85'] = True
                event['PercentileEngageBelow15'] = False
            elif event['engageDimension'] < percEngageDown and event['completed'] == True:
                event['PercentileEngageAbove85'] = False
                event['PercentileEngageBelow15'] = True
            else:
                event['PercentileEngageAbove85'] = False
                event['PercentileEngageBelow15'] = False

            if event['reflexDimension'] > percReflexUp:
                event['PercentileReflexAbove85'] = True
                event['PercentileReflexBelow15'] = False
            elif event['reflexDimension'] < percReflexDown:
                event['PercentileReflexAbove85'] = False
                event['PercentileReflexBelow15'] = True
            else:
                event['PercentileReflexAbove85'] = False
                event['PercentileReflexBelow15'] = False

            if event['errorRectDimension'] > percErrorRectUp:
                event['PercentileErrorRectAbove85'] = True
                event['PercentileErrorRectBelow15'] = False
            elif event['errorRectDimension'] < percErrorRectDown and event['completed'] == True:
                event['PercentileErrorRectAbove85'] = False
                event['PercentileErrorRectBelow15'] = True
            else:
                event['PercentileErrorRectAbove85'] = False
                event['PercentileErrorRectBelow15'] = False
                
            if event['persistenceDimension'] > percPersistenceUp:
                event['PercentilePersistenceAbove85'] = True
                event['PercentilePersistenceBelow15'] = False
            elif event['persistenceDimension'] < percPersistenceDown:
                event['PercentilePersistenceAbove85'] = False
                event['PercentilePersistenceBelow15'] = True
            else:
                event['PercentilePersistenceAbove85'] = False
                event['PercentilePersistenceBelow15'] = False

            finalList.append(event)

    finalDf = pd.DataFrame(finalList, columns=['group_id', 'user', 'task_id', 'n_attempt', 'spatialDimension', 'percSpatialValue', 'PercentileSpatialAbove85', 'PercentileSpatialBelow15', 'engageDimension', 'percEngageValue', 'PercentileEngageAbove85', 'PercentileEngageBelow15', 'exploreDimension', 'percExploreValue', 'PercentileExploreAbove85', 'PercentileExploreBelow15', 'experimentDimension', 'percExperimentValue', 'PercentileExperimentAbove85', 'PercentileExperimentBelow15', 'reflexDimension', 'percReflexValue', 'PercentileReflexAbove85', 'PercentileReflexBelow15', 'errorRectDimension', 'percErrorRectValue', 'PercentileErrorRectAbove85', 'PercentileErrorRectBelow15', 'persistenceDimension', 'percPersistenceValue', 'PercentilePersistenceAbove85', 'PercentilePersistenceBelow15', 'completed'])
    return finalDf




