import pandas as pd
from datetime import datetime
import numpy as np
import json

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/jruipere/Dropbox (MIT)/Game-based Assessment/data_processing_scripts/data/anonymized_dataset.csv', sep=";")
# dataEvents = pd.read_csv('/Users/pedroantonio/Desktop/data/anonymized_dataset.csv', sep=";")
# lelevelsOfActivity = computeLevelsOfActivity(dataEvents)

def computeLevelsOfActivity(dataEvents, group = 'all'):
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
    
    typeEvents = ['ws-snapshot','ws-paint', 'ws-rotate_view','ws-move_shape','ws-rotate_shape' ,'ws-scale_shape','ws-create_shape','ws-delete_shape','ws-undo_action','ws-redo_action']
    
   
        
    #initialize the metrics                                                                                          
    activity_by_user['event'] = np.nan
    activity_by_user['different_events'] = np.nan
    activity_by_user['active_time'] = np.nan
    for event in typeEvents:
        activity_by_user[event] = 0
    
    #initialize the data structures
    userFunnelDict = dict()  
    puzzleEvents = dict()
    eventsDiff = []
    eventsDiff_puzzle = dict()
    timePuzzle = dict()
    globalTypesEvents = dict()
    #typesEvents = dict()
    
      
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
                        
                # initialize if the id is new                                                                              
                if(user_puzzle_key not in puzzleEvents.keys()):
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
                    
                    puzzleEvents[user_puzzle_key] += 1
                    
                    #add the event type                                                                         
                    eventsDiff_puzzle[user_puzzle_key].append(event['type'])
                    
                    #calculate the duration of the event                                                                          
                    delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                    if((delta_seconds < theresHoldActivity)):
                        timePuzzle[user_puzzle_key] += delta_seconds

                    previousEvent = event
    
    # add the data by group_user_task_id            
    for i in dataEvents['group_user_task_id'].unique():
        key_split = i.split('~')
        if(key_split[2] != ''):
            activity_by_user.at[i, 'event'] = puzzleEvents[i]
            activity_by_user.at[i, 'different_events'] = len(set(eventsDiff_puzzle[i]))
            activity_by_user.at[i, 'active_time'] = timePuzzle[i]
            for event in typeEvents:
                activity_by_user.at[i, event] = globalTypesEvents[i][event]

    #delete row with NaN
    activity_by_user.dropna(inplace=True)
    #delete group_user_task_id column
    activity_by_user.drop(columns=['group_user_task_id'], inplace=True)
    
    #data output preparation                                                                                          
    activity_by_user = pd.melt(activity_by_user, id_vars=['group', 'user','task_id'], 
        value_vars=['event','different_events', 'active_time','ws-snapshot','ws-paint','ws-rotate_view','ws-rotate_shape','ws-move_shape','ws-scale_shape','ws-create_shape','ws-delete_shape','ws-undo_action','ws-redo_action'], 
        var_name='metric', value_name='value')
        
    return activity_by_user



