#!/usr/bin/env python
# coding: utf-8

##Usage : pruebas = sequenceBetweenPuzzles(group=['chadsalyer', 'ginnymason'])
##pruebas

from datacollection.models import Event, URL, CustomSession
from django_pandas.io import read_frame
import pandas as pd
import numpy as np
import json
import hashlib
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

all_data_collection_urls = ['ginnymason', 'chadsalyer', 'kristinknowlton', 'lori day', 'leja', 'leja2', 'debbiepoull', 'juliamorgan']

def sequenceBetweenPuzzles(group = 'all'): 
    
    if group == 'all' : 
        toFilter = all_data_collection_urls
    else:
        toFilter = group
        
    urls = URL.objects.filter(name__in=toFilter)
    sessions = CustomSession.objects.filter(url__in=urls)
    qs = Event.objects.filter(session__in=sessions)
    dataEvents = read_frame(qs)
    
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

    for user in dataEvents['group_user_id'].unique():
            #Select rows
            user_events = dataEvents[dataEvents['group_user_id'] == user]
            user_events_na_dropped = user_events.dropna()
            for enum, event in user_events_na_dropped.iterrows():
                user_key = event['user']
                if(user_key not in userPuzzleDict.keys()):
                    userPuzzleDict[user_key] = {}
                    numPuzzles = 1
                if(event['type'] == 'ws-start_level'):
                    activePuzzle = json.loads(event['data'])['task_id']
                    if (activePuzzle == 'Sandbox'):
                        continue
                    secondKey = str(numPuzzles) + '~' + event['session']
                    if (userPuzzleDict[user_key].get(secondKey) == None):
                        userPuzzleDict[user_key][secondKey] = dict()
                    if (userPuzzleDict[user_key][secondKey].get(activePuzzle) == None):
                        userPuzzleDict[user_key][secondKey] = {activePuzzle : 'started'}
                elif(event['type'] == 'ws-puzzle_started'):
                    if (activePuzzle == 'Sandbox' or userPuzzleDict[user_key][secondKey][activePuzzle] in ['started','shape_created', 'submitted', 'completed']):
                        continue
                    userPuzzleDict[user_key][secondKey] = {activePuzzle : 'started'}
                elif(event['type'] == 'ws-create_shape'):
                    if (activePuzzle == 'Sandbox' or userPuzzleDict[user_key][secondKey][activePuzzle] in ['shape_created', 'submitted', 'completed']):
                        continue
                    userPuzzleDict[user_key][secondKey] = {activePuzzle : 'shape_created'}
                elif(event['type'] == 'ws-check_solution'):
                    if (activePuzzle == 'Sandbox' or userPuzzleDict[user_key][secondKey][activePuzzle] in ['submitted', 'completed']):
                        continue
                    userPuzzleDict[user_key][secondKey] = {activePuzzle :'submitted'}
                elif(event['type'] == 'ws-puzzle_complete'):
                    if (activePuzzle == 'Sandbox'):
                        continue
                    userPuzzleDict[user_key][secondKey] = {activePuzzle :'completed'}
                elif(event['type'] in ['ws-exit_to_menu', 'ws-disconnect', 'ws-login_user']):
                    if (activePuzzle == 'Sandbox'):
                        continue
                    numPuzzles +=1
                    
    userSessionList = []
    for key in userPuzzleDict.keys():
        for sequence in userPuzzleDict[key].keys():
                key_split = sequence.split('~')
                userSessionList.append([key, key_split[1], int(key_split[0]), userPuzzleDict[key][sequence]])

    userSequence = pd.DataFrame(userSessionList, columns=['user', 'session', 'sequence', 'task_id'])
    #Recalculate sequence
    mod = []
    for user in userSequence['user'].unique():
            previousAttempt = 1
            n_attempt = 1
            individualDf = userSequence[userSequence['user'] == user]
            for enum, event in individualDf.iterrows():
                if (event['sequence'] != previousAttempt):
                    n_attempt += 1
                previousAttempt = event['sequence']
                event['sequence'] = n_attempt
                mod.append(event)
    modDf = pd.DataFrame(mod, columns=['user', 'session', 'sequence', 'task_id'])
    return modDf




