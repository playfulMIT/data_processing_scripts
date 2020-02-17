#!/usr/bin/env python
# coding: utf-8

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/manuelgomezmoratilla/Desktop/TFG/data_processing_scripts/data/anonamyze_all_data_collection.csv', sep=";")
# sequence = sequenceOfActions(dataEvents, group = 'all')

import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

pd.options.mode.chained_assignment = None  # default='warn'

def sequenceOfActions(dataEvents, group = 'all'): 
    
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

    userPuzzleDict = {}

    for user in dataEvents['user'].unique():
            #Select rows
            user_events = dataEvents[dataEvents['user'] == user]
            # Drop NaNs
            user_events_na_dropped = user_events.dropna()
            for enum, event in user_events_na_dropped.iterrows():
                user_key = event['user']
                if(user_key not in userPuzzleDict.keys()):
                    userPuzzleDict[user_key] = {}
                    numPuzzles = 0
                if(event['type'] == 'ws-start_level'):
                    #print('\\start level\\')
                    #print(json.loads(event['data']))
                    activePuzzle = json.loads(event['data'])['task_id']
                    numPuzzles += 1
                    #Concatenate sequence with session_id
                    secondKey = str(numPuzzles) + '~' + event['session_id']
                    userPuzzleDict[user_key][secondKey] = {activePuzzle : ''}  
                elif(event['type'] == 'ws-puzzle_started'):
                    userPuzzleDict[user_key][secondKey] = {activePuzzle : 'started'}
                elif(event['type'] == 'ws-create_shape'):
                    userPuzzleDict[user_key][secondKey] = {activePuzzle : 'shape_created'}
                elif(event['type'] == 'ws-check_solution'):
                    userPuzzleDict[user_key][secondKey] = {activePuzzle :'submitted'}
                elif(event['type'] == 'ws-puzzle_complete'):
                    userPuzzleDict[user_key][secondKey] = {activePuzzle :'completed'}

    userSessionList = []
    for key in userPuzzleDict.keys():
        for sequence in userPuzzleDict[key].keys():
            # Getting session_id and sequence number by split.
                key_split = sequence.split('~')
                if ('Sandbox' not in userPuzzleDict[key][sequence].keys()):
                    # user, session_id, sequence number, {task_id : funnel}
                    userSessionList.append([key, key_split[1], key_split[0], userPuzzleDict[key][sequence]])

    userSequence = pd.DataFrame(userSessionList, columns=['user', 'session_id', 'sequence', 'task_id'])
    
    return userSequence





