#!/usr/bin/env python
# coding: utf-8

from datacollection.models import Event, URL, CustomSession
from django_pandas.io import read_frame
import pandas as pd
import numpy as np
import json
import hashlib
import collections
import re
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

all_data_collection_urls = ['ginnymason', 'chadsalyer', 'kristinknowlton', 'lori day', 'leja', 'leja2', 'debbiepoull', 'juliamorgan']
pd.options.mode.chained_assignment = None  # default='warn'

# USAGE : pruebas = sequenceWithinPuzzles(group=['chadsalyer'])
# pruebas

def sequenceWithinPuzzles(group = 'all'):
    
    if group == 'all' : 
        toFilter = all_data_collection_urls
    else:
        toFilter = group
        
    urls = URL.objects.filter(name__in=toFilter)
    sessions = CustomSession.objects.filter(url__in=urls)
    qs = Event.objects.filter(session__in=sessions)
    dataEvents = read_frame(qs)
    
    
    tutorialList = ['1. One Box', '2. Separated Boxes', '3. Rotate a Pyramid', '4. Match Silhouettes', '5. Removing Objects', '6. Stretch a Ramp', '7. Max 2 Boxes', '8. Combine 2 Ramps', '9. Scaling Round Objects', 'Sandbox']
    #Remove SandBox and tutorial levels.
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
    
    newDataEvents = []
    #Select puzzle and actions
    notSelectedEvents = ['ws-mode_change', 'ws-click_nothing', 'ws-click_disabled', 'ws-select_shape', 'ws-deselect_shape', 'ws-paint', 'ws-palette_change', 'ws-toggle_paint_display', 'ws-toggle_snapshot_display', 'ws-create_user', 'ws-redo_action', 'ws-undo_action', 'ws-restart_puzzle', 'ws-puzzle_started']
    #Selected puzzles
    selectedPuzzles = ['Square Cross-Sections', 'Bird Fez', 'Pi Henge', '45-Degree Rotations',  'Pyramids are Strange', 'Boxes Obscure Spheres', 'Object Limits', 'Warm Up', 'Angled Silhouette',
                    'Sugar Cones','Stranger Shapes', 'Tall and Small', 'Ramp Up and Can It', 'More Than Meets Your Eye', 'Not Bird', 'Unnecesary', 'Zzz', 'Bull Market', 'Few Clues', 'Orange Dance', 'Bear Market']

    eventsWithMetaData = ['ws-create_shape', 'ws-delete_shape', 'ws-rotate_shape', 'ws-scale_shape', 'ws-move_shape']

    for user in dataEvents['group_user_id'].unique():
            #Select rows
            user_events = dataEvents[dataEvents['group_user_id'] == user]
            user_events_na_dropped = user_events.dropna()
            activePuzzle = None
            nAttempt = 1
            prevCheck = False
            prevEvent = None
            figureDict = dict()
            for enum, event in user_events_na_dropped.iterrows():
                #Ignore event
                if (prevCheck == True):
                    if (event['type'] == 'ws-puzzle_complete'):
                        prevEvent['metadata']['correct'] = True
                        newDataEvents.append(prevEvent)
                        prevCheck = False
                        prevEvent = None
                        continue
                    else:
                        prevEvent['metadata']['correct'] = False
                    newDataEvents.append(prevEvent)
                    prevCheck = False
                    prevEvent = None
                        
                if (event['type'] in notSelectedEvents):
                        continue

                elif(event['type'] == 'ws-start_level'):
                    activePuzzle = json.loads(event['data'])['task_id']
                    event['task_id'] = activePuzzle

                elif (event['type'] == 'ws-create_shape'):
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt
                        shape_id = json.loads(event['data'])['objSerialization']
                        shape_type = json.loads(event['data'])['shapeType']
                        figureDict[shape_id] = shape_type
                        event['metadata'] = dict()
                        event['metadata']['shape_id'] = shape_id 
                        event['metadata']['shape_type'] = shape_type 
                        newDataEvents.append(event)

                elif (event['type'] == 'ws-delete_shape' or event['type'] == 'ws-move_shape'):
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt
                        if (event['type'] == 'ws-delete_shape'):
                            idList = json.loads(event['data'])['deletedShapes']
                        elif (event['type'] == 'ws-move_shape'):
                            idList = json.loads(event['data'])['selectedObjects']
                        for shapeId in idList:
                            shape_id = shapeId
                            shape_type = figureDict[shape_id]
                            event['metadata'] = dict()
                            event['metadata']['shape_id'] = shape_id 
                            event['metadata']['shape_type'] = shape_type 
                            newDataEvents.append(event)

                elif (event['type'] == 'ws-rotate_shape' or event['type'] == 'ws-scale_shape'):
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt
                        shape_id = json.loads(event['data'])['selectedObject']
                        shape_type = figureDict[shape_id]
                        event['metadata'] = dict()
                        event['metadata']['shape_id'] = shape_id 
                        event['metadata']['shape_type'] = shape_type 
                        newDataEvents.append(event)

                elif ((event['type'] in ['ws-exit_to_menu', 'ws-login_user']) and (activePuzzle in selectedPuzzles)):
                    figureDict.clear()
                    nAttempt +=1
   
                else :
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt
                        event['metadata'] = dict()
                        if (event['type'] == 'ws-check_solution'):
                            prevCheck = True
                            prevEvent = event
                        else:
                            newDataEvents.append(event)

    taskDf = pd.DataFrame(newDataEvents, columns=['id', 'time', 'group_user_id', 'task_id', 'n_attempt', 'type', 'metadata']) 

    data = taskDf
                        
    listEvent = ['ws-rotate_view', 'ws-rotate_shape', 'ws-undo_action', 'ws-move_shape', 'ws-snapshot', 'ws-scale_shape']
    
    dataConvert2 = []
    for user in data['group_user_id'].unique():
        individualDf = data[data['group_user_id'] == user]
        #Current action set
        currentAction = []
        #String with action types
        actionString = ""
        actualEvent = 'None'
        for enum, event in individualDf.iterrows():
            key = event['group_user_id']
            key_split = key.split('~')
            event['group_id'] = key_split[0]
            event['user'] = key_split[1]
            actualEvent = event['type']
            eq = True
            for a in currentAction:
                if (a['type'] != actualEvent):
                    #Ver si podemos compactar
                    eq = False
                    
            if (eq == False):      
                igual = True
                prev = ""
                for a2 in currentAction:
                    if (a2['type'] != prev):
                        if (prev == "") :
                            igual = True
                        else:
                            igual = False
                    prev = a2['type']
                if ((igual == True) and (prev in listEvent)):
                    add = currentAction[0]
                    #add['type'] = add['type'] + 'x' + str(len(currentAction))
                    add['n_times'] = dict()
                    add['n_times'][add['type']] = len(currentAction)
                    dataConvert2.append(add)
                    currentAction.clear()
                    currentAction.append(event)     
                else: #igual != True 
                    for a in currentAction:
                        a['n_times'] = dict()
                        a['n_times'][a['type']] = 1
                        dataConvert2.append(a)
                    currentAction.clear()
                    currentAction.append(event)
            else: #eq = True
                if (event['type'] not in listEvent):
                    currentAction.append(event)
                    for a in currentAction:
                        a['n_times'] = dict()
                        a['n_times'][a['type']] = 1
                        dataConvert2.append(a)
                    currentAction.clear()
                    
                else:
                    if (len(currentAction) > 0):
                            if (currentAction[0]['type'] in eventsWithMetaData):
                                #Event with metadata, check if it is the same shape_id
                                if (currentAction[0]['metadata']['shape_id'] == event['metadata']['shape_id']):
                                    currentAction.append(event)
                                else:
                                    add = currentAction[0]
                                    #add['type'] = add['type'] + 'x' + str(len(currentAction))
                                    add['n_times'] = dict()
                                    add['n_times'][add['type']] = len(currentAction)
                                    dataConvert2.append(add)
                                    currentAction.clear()
                                    currentAction.append(event)
                            #Event without metaData, just concatenate.
                            else:
                                currentAction.append(event) 

                    elif (len(currentAction) == 0):
                        currentAction.append(event)
                
                    
        #Add last elems
        #We must check if last elems can be also replaced.
        final = ""
        if (len(currentAction) > 0):
            igual2 = True
            prev = ""
            for a2 in currentAction:
                if (a2['type'] != prev):
                    if (prev == "") :
                        igual2 = True
                    else:
                        igual2 = False
                prev = a2['type']
            if ((igual == True) and (prev in listEvent)):
                add = currentAction[0]
                #add['type'] = add['type'] + 'x' + str(len(currentAction))
                add['n_times'] = dict()
                add['n_times'][add['type']] = len(currentAction)
                dataConvert2.append(add)
                currentAction.clear()
                currentAction.append(event)     
            else: #igual != True 
                for a in currentAction:
                    a['n_times'] = dict()
                    a['n_times'][a['type']] = 1
                    dataConvert2.append(a)
                currentAction.clear()
                currentAction.append(event)
               
    #Create dataframe from list
    #consecutiveDf = pd.DataFrame(dataConvert2, columns=['id', 'time', 'group_user_id', 'task_id', 'n_attempt', 'type', 'metadata'])         
    data = pd.DataFrame(dataConvert2, columns=['group_id', 'user', 'task_id', 'n_attempt', 'type', 'n_times', 'metadata']) 
    
    #Recalculate n_attempt
    mod = []
    for user in data['user'].unique():
            previousAttempt = 1
            n_attempt = 1
            individualDf = data[data['user'] == user]
            for enum, event in individualDf.iterrows():
                if (event['n_attempt'] != previousAttempt):
                    n_attempt += 1
                previousAttempt = event['n_attempt']
                event['n_attempt'] = n_attempt
                mod.append(event)
    modDf = pd.DataFrame(mod, columns=['group_id', 'user', 'task_id', 'n_attempt', 'type', 'n_times', 'metadata'])
    return modDf




