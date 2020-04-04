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
# metrics = levelsOfDifficulty(dataEvents, group = 'all')


pd.options.mode.chained_assignment = None  # default='warn'
    
listActionEvents = ['ws-move_shape', 'ws-rotate_shape', 'ws-scale_shape', 
                    'ws-check_solution', 'ws-undo_action', 'ws-redo_action',
                    'ws-rotate_view', 'ws-snapshot', 'ws-mode_change',
                    'ws-create_shape', 'ws-select_shape', 'ws-delete_shape', 'ws-select_shape_add']

orderMapping = {'1. One Box': 1, '2. Separated Boxes': 2, '3. Rotate a Pyramid': 3, '4. Match Silhouettes': 4, '5. Removing Objects': 5, '6. Stretch a Ramp': 6, '7. Max 2 Boxes': 7, '8. Combine 2 Ramps': 8, '9. Scaling Round Objects': 9, 
                'Square Cross-Sections': 10, 'Bird Fez': 11, 'Pi Henge': 12, '45-Degree Rotations': 13,  'Pyramids are Strange': 14, 'Boxes Obscure Spheres': 15, 'Object Limits': 16, 'Warm Up': 17, 'Angled Silhouette': 18,
                'Sugar Cones': 19,'Stranger Shapes': 20, 'Tall and Small': 21, 'Ramp Up and Can It': 22, 'More Than Meets Your Eye': 23, 'Not Bird': 24, 'Unnecesary': 25, 'Zzz': 26, 'Bull Market': 27, 'Few Clues': 28, 'Orange Dance': 29, 'Bear Market': 30}

# mapping to positions
typeMapping = {'1. One Box': 'Basic Puzzles', '2. Separated Boxes': 'Basic Puzzles', '3. Rotate a Pyramid': 'Basic Puzzles', '4. Match Silhouettes': 'Basic Puzzles', '5. Removing Objects': 'Basic Puzzles', '6. Stretch a Ramp': 'Basic Puzzles', '7. Max 2 Boxes': 'Basic Puzzles', '8. Combine 2 Ramps': 'Basic Puzzles', '9. Scaling Round Objects': 'Basic Puzzles', 
               'Square Cross-Sections': 'Intermediate Puzzles', 'Bird Fez': 'Intermediate Puzzles', 'Pi Henge': 'Intermediate Puzzles', '45-Degree Rotations': 'Intermediate Puzzles',  'Pyramids are Strange': 'Intermediate Puzzles', 'Boxes Obscure Spheres': 'Intermediate Puzzles', 'Object Limits': 'Intermediate Puzzles', 'Angled Silhouette': 'Intermediate Puzzles',
               'Sugar Cones': 'Advanced Puzzles', 'Stranger Shapes': 'Advanced Puzzles', 'Tall and Small': 'Advanced Puzzles', 'Ramp Up and Can It': 'Advanced Puzzles', 'More Than Meets Your Eye': 'Advanced Puzzles', 'Not Bird': 'Advanced Puzzles', 'Unnecessary': 'Advanced Puzzles', 'Zzz': 'Advanced Puzzles', 'Bull Market': 'Advanced Puzzles', 'Few Clues': 'Advanced Puzzles', 'Orange Dance': 'Advanced Puzzles', 'Bear Market': 'Advanced Puzzles', 'Warm Up': 'Intermediate Puzzles'}

    
def levelsOfDifficulty(dataEvents, group = 'all'):    
    
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']
    # filtering to only take the group passed as argument
    if(group != 'all'):
        dataEvents = dataEvents[dataEvents['group'].isin(group)]

    # Data Cleaning
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')
    
    
    # puzzleDict[user][puzzle_id] = {'tutorial':value, 'completed':value, 'n_actions':value, 'n_attempts':value, 'sum_time':value}
    userPuzzleDict = {}
    theresHoldActivity = 60
    
    for user in dataEvents['group_user_id'].unique():
        #Select rows
        user_events = dataEvents[dataEvents['group_user_id'] == user]
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

                # print('\\finish\\')
                # time spent
                # print('{} minutes, {} actions, {} attempts'.format(round(np.sum(activeTime)/60,2), numberActions, numberAttempts))
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
        userDf['group_user_id'] = user
        key_split = user.split('~')
        userDf['group'] = key_split[0] 
        if (userDf.shape != 0):
            stats_by_level_player.append(userDf)
        else: 
            continue
    
    try:
        stats_by_level_player = pd.concat(stats_by_level_player, sort=True)
        stats_by_level_player['puzzle'] = stats_by_level_player.index

        stats_by_level_player['order'] = stats_by_level_player['puzzle'].map(orderMapping) 
        stats_by_level_player['level_type'] = stats_by_level_player['puzzle'].map(typeMapping) 
        stats_by_level_player['p_incorrect'] = 100-100*(stats_by_level_player['completed']/stats_by_level_player['n_attempts'])
        stats_by_level_player['n_abandoned'] = 1 - stats_by_level_player['completed']

        stats_by_level = round(stats_by_level_player.groupby(['puzzle', 'order', 'level_type', 'group']).agg({'active_time': 'mean',
                                                    'n_attempts': 'mean',
                                                    'n_actions': 'mean',
                                                    'p_incorrect': 'mean',
                                                    'completed': 'sum',
                                                    'group_user_id':'count',
                                                    'n_abandoned': 'sum'}).reset_index(),2).sort_values('order').rename(columns = {'completed': 'n_completed',
                                                                                                                                   'group_user_id': 'n_started', 'puzzle': 'task_id'})
        difficulty_metrics = []
        for group in stats_by_level['group'].unique():
            new_stats = stats_by_level[stats_by_level['group']== group]
            new_stats['p_abandoned'] = round(100*new_stats['n_abandoned']/new_stats['n_started'],2)
            new_stats['completed_time'] = round(new_stats['active_time']/new_stats['n_completed'],4)
            #Amount of actions / #puzzles completed
            new_stats['actions_completed'] = round(new_stats['n_actions']/new_stats['n_completed'],2)
            #Replace NaNs values with 100%
            new_stats['p_incorrect'].replace(np.nan, 100, inplace=True)
            #Older metrics
            #stats_by_level['z_active_time'] = (stats_by_level['active_time'] - stats_by_level['active_time'].mean())/stats_by_level['active_time'].std()
            #stats_by_level['z_n_actions'] = (stats_by_level['n_actions'] - stats_by_level['n_actions'].mean())/stats_by_level['n_actions'].std()

            #Standardize parameters
            new_stats['z_p_incorrect'] = (new_stats['p_incorrect'] - new_stats['p_incorrect'].mean())/new_stats['p_incorrect'].std()
            new_stats['z_p_abandoned'] = (new_stats['p_abandoned'] - new_stats['p_abandoned'].mean())/new_stats['p_abandoned'].std()
            new_stats['z_actions_completed'] = (new_stats['actions_completed'] - new_stats['actions_completed'].mean())/new_stats['actions_completed'].std()
            new_stats['z_completed_time'] = (new_stats['completed_time'] - new_stats['completed_time'].mean())/new_stats['completed_time'].std()

            new_stats['z_all_measures'] = new_stats[['z_completed_time', 'z_actions_completed', 'z_p_incorrect', 'z_p_abandoned']].sum(axis = 1)
            #Normalize between 0 and 1
            new_stats['norm_all_measures'] = (new_stats['z_all_measures']-new_stats['z_all_measures'].min())/(new_stats['z_all_measures'].max()-new_stats['z_all_measures'].min())

            difficulty_metrics.append(pd.DataFrame(new_stats, columns = ['group','task_id','order','completed_time', 'actions_completed', 'p_incorrect', 'p_abandoned', 'norm_all_measures']))

        difficulty_metrics = pd.concat(difficulty_metrics)
        difficulty_metrics.sort_values(['task_id'])
        return difficulty_metrics
    except ValueError:
        return -1




