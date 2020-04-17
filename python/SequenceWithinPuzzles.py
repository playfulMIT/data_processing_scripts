#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import json
import hashlib
import collections
import re
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/manuelgomezmoratilla/Desktop/data_processing_scripts/data/anonymized_dataset.csv', sep=";")
# metrics = levelsOfDifficulty(dataEvents, group = 'all')


pd.options.mode.chained_assignment = None  # default='warn'
def sequenceWithinPuzzles(dataEvents, group = 'all'):
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
    notSelectedEvents = ['ws-mode_change', 'ws-click_nothing', 'ws-click_disabled', 'ws-select_shape', 'ws-deselect_shape', 'ws-paint', 'ws-palette_change', 'ws-toggle_paint_display']
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
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt
                        event['metadata'] = dict()
                        newDataEvents.append(event)

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

                elif ((event['type'] == 'ws-exit_to_menu') and (activePuzzle in selectedPuzzles)):
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
    listEvent = ['ws-rotate_view', 'ws-rotate_shape', 'ws-undo_action','ws-move_shape','ws-snapshot','ws-scale_shape']
    
    for nameEvent in listEvent:
        dataConvert2 = []
        for user in data['group_user_id'].unique():
            individualDf = data[data['group_user_id'] == user]
            #Current action set
            currentAction = []
            #String with action types
            actionString = ""
            for enum, event in individualDf.iterrows():
                #Add new action to the string
                prov = actionString + event['type'] + " "
                regex = "(" + nameEvent + ")+"
                #Sustituimos y si solo hay eventos del tipo que buscamos debera quedar "".
                #Replace event, should be "" if all event types are nameEvent.
                string = re.sub(regex ,"", prov)
                srem = re.sub(" ", "", string)
                if (srem != ""):
                    coinc = re.findall(regex, prov)
                    #No coincidences.
                    if (len(coinc) == 0):                    
                        prov = ""
                        actionString= ""
                        if (len(currentAction) > 0):
                            for a in currentAction:
                                dataConvert2.append(a)
                            dataConvert2.append(event)
                        else:
                            dataConvert2.append(event)
                        currentAction.clear()
                    #One coincidence
                    elif(len(coinc) == 1):
                        #Se añade la accion anterior al DF, se añade la actual al buffer y seguimos buscando.
                        if (len(currentAction) > 0) :
                            dataConvert2.append(currentAction[0])
                            currentAction.clear()
                            currentAction.append(event)
                            actionString = ""
                            actionString = actionString + event['type'] + " "
                            prov = ""
                    #Hay más de una coincidencia. Se coge el numero total, se añade al dataFrame quitando el ultimo
                    #evento que es el que es diferente al resto y se vuelve a añadir al buffer para seguir buscando a 
                    #partir de el.
                    #Two or more coincidences. Add the action with # of repetitions and continue.
                    else:
                        add = currentAction[0]
                        add['type'] = nameEvent + 'x' + str(len(coinc))
                        dataConvert2.append(add)
                        currentAction.clear()
                        currentAction.append(event)
                        actionString = ""
                        actionString = actionString + event['type'] + " "
                        prov = ""  
                #Check if it is the same shape_id or not
                else:
                    if (len(currentAction) > 0):
                        if (currentAction[0]['type'] in eventsWithMetaData):
                            #Event with metadata, check if it is the same shape_id
                            if (currentAction[0]['metadata']['shape_id'] == event['metadata']['shape_id']):
                                actionString = actionString + event['type'] + " "
                                currentAction.append(event)
                            else:
                                add = currentAction[0]
                                coinc3 = re.findall(regex, actionString)
                                add['type'] = nameEvent + 'x' + str(len(coinc3))
                                dataConvert2.append(add)
                                actionString = ""
                                actionString = actionString + event['type'] + " "
                                currentAction.clear()
                                currentAction.append(event)
                                prov = ""
                        #Event without metaData, just concatenate.
                        else:
                            actionString = actionString + event['type'] + " "
                            currentAction.append(event) 
                            
                    elif (len(currentAction) == 0):
                        actionString = actionString + event['type'] + " "
                        currentAction.append(event)
                                   
            #Add last elems
            #We must check if last elems can be also replaced.
            final = ""
            if (len(currentAction) > 0):
                for e in currentAction:
                    final = final + e['type'] + " "
                string2 = re.sub(regex ,"", final)
                srem2 = re.sub(" ", "", string2)
                if (srem2 != ""):
                #Add each action to the list
                    for e in currentAction :
                        dataConvert2.append(e)
                else:
                    coinc2 = re.findall(regex, final)
                    add = currentAction[0]
                    add['type'] = nameEvent + 'x' + str(len(coinc2))
                    dataConvert2.append(add)
        
        #Create dataframe from list
        consecutiveDf = pd.DataFrame(dataConvert2, columns=['id', 'time', 'group_user_id', 'task_id', 'n_attempt', 'type', 'metadata']) 
        data = consecutiveDf
    
    newData = []
    regexNum = '[0-9]+$'
    regexNumX = '(x[0-9]+$)'
    for enum, event in data.iterrows():
        key = event['group_user_id']
        key_split = key.split('~')
        event['group'] = key_split[0]
        event['user'] = key_split[1]
        string = event['type']
        match = re.findall(regexNum, string)
        if (len(match) == 1) :
            event['n_times'] = (int)(match[0])
            matchX = re.findall(regexNumX, string)
            event['type'] = string.split(matchX[0])[0] 
            newData.append(event)
        else :
            event['n_times'] = 1
            newData.append(event)
            
    
    data = pd.DataFrame(newData, columns=['group', 'user', 'task_id', 'n_attempt', 'type', 'n_times', 'metadata']) 
    return data

