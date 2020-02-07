#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/manuelgomezmoratilla/Desktop/data_processing_scripts/data/anonymized_dataset.csv', sep=";")
# metrics = levelsOfDifficulty(dataEvents)
    
listActionEvents = ['ws-move_shape', 'ws-rotate_shape', 'ws-scale_shape', 
                    'ws-check_solution', 'ws-undo_action', 'ws-redo_action',
                    'ws-rotate_view', 'ws-snapshot', 'ws-mode_change',
                    'ws-create_shape', 'ws-select_shape', 'ws-delete_shape', 'ws-select_shape_add']

orderMapping = {'Sandbox': np.nan, '1. One Box': 1, '2. Separated Boxes': 2, '3. Rotate a Pyramid': 3, '4. Match Silhouettes': 4, '5. Removing Objects': 5, '6. Stretch a Ramp': 6, '7. Max 2 Boxes': 7, '8. Combine 2 Ramps': 8, '9. Scaling Round Objects': 9, 
                'Square Cross-Sections': 10, 'Bird Fez': 11, 'Pi Henge': 12, '45-Degree Rotations': 13,  'Pyramids are Strange': 14, 'Boxes Obscure Spheres': 15, 'Object Limits': 16, 'Not Bird': 17, 'Angled Silhouette': 18,
                'Warm Up': 19, 'Stranger Shapes': 20, 'Sugar Cones': 21, 'Tall and Small': 22, 'Ramp Up and Can It': 23, 'More Than Meets Your Eye': 24, 'Unnecessary': 25, 'Zzz': 26, 'Bull Market': 27, 'Few Clues': 28, 'Orange Dance': 29, 'Bear Market': 30}

# mapping to positions
typeMapping = {'Sandbox': 'SAND', '1. One Box': 'Tutorial', '2. Separated Boxes': 'Tutorial', '3. Rotate a Pyramid': 'Tutorial', '4. Match Silhouettes': 'Tutorial', '5. Removing Objects': 'Tutorial', '6. Stretch a Ramp': 'Tutorial', '7. Max 2 Boxes': 'Tutorial', '8. Combine 2 Ramps': 'Tutorial', '9. Scaling Round Objects': 'Tutorial', 
               'Square Cross-Sections': 'Easy Puzzles', 'Bird Fez': 'Easy Puzzles', 'Pi Henge': 'Easy Puzzles', '45-Degree Rotations': 'Easy Puzzles',  'Pyramids are Strange': 'Easy Puzzles', 'Boxes Obscure Spheres': 'Easy Puzzles', 'Object Limits': 'Easy Puzzles', 'Not Bird': 'Easy Puzzles', 'Angled Silhouette': 'Easy Puzzles',
               'Warm Up': 'Hard Puzzles', 'Stranger Shapes': 'Hard Puzzles', 'Sugar Cones': 'Hard Puzzles', 'Tall and Small': 'Hard Puzzles', 'Ramp Up and Can It': 'Hard Puzzles', 'More Than Meets Your Eye': 'Hard Puzzles', 'Unnecessary': 'Hard Puzzles', 'Zzz': 'Hard Puzzles', 'Bull Market': 'Hard Puzzles', 'Few Clues': 'Hard Puzzles', 'Orange Dance': 'Hard Puzzles', 'Bear Market': 'Hard Puzzles'}

    
def levelsOfDifficulty(dataEvents):    
    
    all_data_by_user = dataEvents.groupby(['session_id']).agg({'id':'count',
                                                         'type':'nunique'}).reset_index().rename(columns={'id':'n_events',
                                                                                                          'type':'n_different_events'})

    # Data Cleaning
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents['time'] = dataEvents['time'] - timedelta(hours=4)
    dataEvents['date'] = dataEvents['time'].dt.date
    dataEvents.groupby('date').size()
    dataEvents = dataEvents.sort_values('time')
    
    
    # puzzleDict[user][puzzle_id] = {'tutorial':value, 'completed':value, 'n_actions':value, 'n_attempts':value, 'sum_time':value}
    userPuzzleDict = {}
    theresHoldActivity = 60
    
    for user in all_data_by_user['session_id'].unique():
        #Select rows
        user_events = dataEvents[dataEvents['session_id'] == user]
        userPuzzleDict[user] = {}
        # Analyze when a puzzle has been started
        activePuzzle = None
        previousEvent = None
        numberActions = 0
        numberAttempts = 0
        activeTime = []
        
        for enum, event in user_events.iterrows():
            #print(('{} - {}').format(event['time'], event['type']))
            if(event['type'] == 'ws-start_level'):
                #print('\\start level\\')
                #print(json.loads(event['data']))
                activePuzzle = json.loads(event['data'])['task_id']
                if(activePuzzle not in userPuzzleDict[user].keys()):

                    userPuzzleDict[user][activePuzzle] = {'completed':0, 
                                                          'n_actions':0, 'n_attempts':0, 'active_time':0}

            # If event is puzzle complete we always add it
            if(event['type'] == 'ws-puzzle_complete'):
                puzzleName = json.loads(event['data'])['task_id']
                if(puzzleName in userPuzzleDict[user].keys()):
                    userPuzzleDict[user][puzzleName]['completed'] = 1
                    
            # If they are not playing a puzzle we do not do anything and continue
            if(activePuzzle is None):
                continue       
                
            # If it is the first event we store the current event and continue
            if(previousEvent is None):
                previousEvent = event
                continue
            
            #Add new active time
            delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
            if((delta_seconds < theresHoldActivity)):
                activeTime.append(delta_seconds)
                
            if(event['type'] in listActionEvents):
                numberActions += 1

            if(event['type'] == 'ws-check_solution'):
                numberAttempts += 1
            
          # Analyze when puzzle is finished or user left
            # Measure time, attempts, completion and actions
            if(event['type'] in ['ws-puzzle_complete', 'ws-exit_to_menu', 'ws-disconnect']):

                #print('\\finish\\')
                # time spent
                #print('{} minutes, {} actions, {} attempts'.format(round(np.sum(activeTime)/60,2), numberActions, numberAttempts))
                # adding counters
                userPuzzleDict[user][activePuzzle]['n_attempts'] += numberAttempts
                userPuzzleDict[user][activePuzzle]['n_actions'] += numberActions
                userPuzzleDict[user][activePuzzle]['active_time'] += round(np.sum(activeTime)/60,2)

                # reset counters
                previousEvent = None
                activeTime = []
                activePuzzle = None
                numberActions = 0
                numberAttempts = 0

            previousEvent = event 
            
    
    stats_by_level_player = []
    for user in userPuzzleDict.keys():
        userDf = pd.DataFrame.from_dict(userPuzzleDict[user], orient = 'index')
        userDf['session_id'] = user
        stats_by_level_player.append(userDf)
       
    stats_by_level_player = pd.concat(stats_by_level_player, sort=True)
    stats_by_level_player['puzzle'] = stats_by_level_player.index
    
    stats_by_level_player['order'] = stats_by_level_player['puzzle'].map(orderMapping) 
    stats_by_level_player['level_type'] = stats_by_level_player['puzzle'].map(typeMapping) 
    stats_by_level_player['p_incorrect'] = 100-100*(stats_by_level_player['completed']/stats_by_level_player['n_attempts'])
    stats_by_level_player['n_abandoned'] = 1 - stats_by_level_player['completed']

    stats_by_level = round(stats_by_level_player.groupby(['puzzle', 'order', 'level_type']).agg({'active_time': 'mean',
                                                'n_attempts': 'mean',
                                                'n_actions': 'mean',
                                                'p_incorrect': 'mean',
                                                'completed': 'sum',
                                                'session_id':'count',
                                                'n_abandoned': 'sum'}).reset_index(),2).sort_values('order').rename(columns = {'completed': 'n_completed',
                                                                                                                               'session_id': 'n_started'})
    stats_by_level['p_abandoned'] = round(100*stats_by_level['n_abandoned']/stats_by_level['n_started'],2)
    #Amount of time / #puzzles completed
    stats_by_level['completed_time'] = round(stats_by_level['active_time']/stats_by_level['n_completed'],2)
     #Amount of actions / #puzzles completed
    stats_by_level['actions_completed'] = round(stats_by_level['n_actions']/stats_by_level['n_completed'],2)
    #Replace NaNs values with 100%
    stats_by_level['p_incorrect'].replace(np.nan, 100, inplace=True)
    #Older metrics
    #stats_by_level['z_active_time'] = (stats_by_level['active_time'] - stats_by_level['active_time'].mean())/stats_by_level['active_time'].std()
    #stats_by_level['z_n_actions'] = (stats_by_level['n_actions'] - stats_by_level['n_actions'].mean())/stats_by_level['n_actions'].std()
    
    #Standardize parameters
    stats_by_level['z_p_incorrect'] = (stats_by_level['p_incorrect'] - stats_by_level['p_incorrect'].mean())/stats_by_level['p_incorrect'].std()
    stats_by_level['z_p_abandoned'] = (stats_by_level['p_abandoned'] - stats_by_level['p_abandoned'].mean())/stats_by_level['p_abandoned'].std()
    stats_by_level['z_actions_completed'] = (stats_by_level['actions_completed'] - stats_by_level['actions_completed'].mean())/stats_by_level['actions_completed'].std()
    stats_by_level['z_completed_time'] = (stats_by_level['completed_time'] - stats_by_level['completed_time'].mean())/stats_by_level['completed_time'].std()

    stats_by_level['z_all_measures'] = stats_by_level[['z_completed_time', 'z_actions_completed', 'z_p_incorrect', 'z_p_abandoned']].sum(axis = 1)
    
    #Normalize between 0 and 1
    stats_by_level['norm_all_measures'] = (stats_by_level['z_all_measures']-stats_by_level['z_all_measures'].min())/(stats_by_level['z_all_measures'].max()-stats_by_level['z_all_measures'].min())
    
    difficulty_metrics = pd.DataFrame(stats_by_level, columns = ['puzzle', 'completed_time', 'actions_completed', 'p_incorrect', 'p_abandoned', 'norm_all_measures'])
    
    return difficulty_metrics






