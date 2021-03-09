#!/usr/bin/env python
# coding: utf-8

# Usage: pruebas = misconceptionsTagged(dataEvents)
# pruebas


import pandas as pd
import numpy as np
import json
import hashlib
import collections
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
from math import nan
import copy

dataEvents = pd.read_csv('/Users/manuelgomezmoratilla/Desktop/TFG/data_processing_scripts/data/anonamyze_all_data_collection_v2.csv', sep=";")


pd.options.mode.chained_assignment = None  # default='warn'
def sequenceWithinPuzzlesForMisconceptions(dataEvents, group = 'all'):
    tutorialList = ['1. One Box', '2. Separated Boxes', '3. Rotate a Pyramid', '4. Match Silhouettes', '5. Removing Objects', '6. Stretch a Ramp', '7. Max 2 Boxes', '8. Combine 2 Ramps', '9. Scaling Round Objects']
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
    notSelectedEvents = ['ws-click_nothing', 'ws-click_disabled', 'ws-toggle_paint_display', 'ws-toggle_snapshot_display', 'ws-create_user', 'ws-login_user']
    #Selected puzzles
    selectedPuzzles = ['Square Cross-Sections','Angled Silhouette', 'Not Bird']
    #selectedPuzzles = ['Square Cross-Sections', 'Bird Fez', 'Pi Henge', '45-Degree Rotations',  'Pyramids are Strange', 'Boxes Obscure Spheres', 'Object Limits', 'Angled Silhouette',
    #                'Sugar Cones','Stranger Shapes', 'Tall and Small', 'Ramp Up and Can It', 'More Than Meets Your Eye', 'Not Bird', 'Zzz', 'Bull Market', 'Orange Dance', 'Bear Market']

    #selectedPuzzles = selectedPuzzles + tutorialList

    eventsWithMetaData = ['ws-create_shape', 'ws-delete_shape', 'ws-rotate_shape', 'ws-scale_shape', 'ws-move_shape']

    for user in dataEvents['group_user_id'].unique():
            #Select rows
            user_events = dataEvents[dataEvents['group_user_id'] == user]
            user_events_na_dropped = user_events.dropna()
            activePuzzle = None
            nAttempt = dict()
            prevCheck = False
            prevEvent = None
            figureDict = dict()
            for enum, event in user_events_na_dropped.iterrows():
                key = event['group_user_id']
                key_split = key.split('~')
                event['group_id'] = key_split[0]
                event['user'] = key_split[1]
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
                    if event['task_id'] not in nAttempt.keys():
                        nAttempt[event['task_id']] = 1
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt[event['task_id']]
                        event['metadata'] = dict()
                        newDataEvents.append(event)

                elif (event['type'] == 'ws-create_shape'):
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt[event['task_id']]
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
                        event['n_attempt'] = nAttempt[event['task_id']]
                        if (event['type'] == 'ws-delete_shape'):
                            idList = json.loads(event['data'])['deletedShapes']
                        elif (event['type'] == 'ws-move_shape'):
                            idList = json.loads(event['data'])['selectedObjects']
                        for shapeId in idList:
                            shape_id = shapeId
                            try:
                                shape_type = figureDict[shape_id]
                                event['metadata'] = dict()
                                event['metadata']['shape_id'] = shape_id 
                                event['metadata']['shape_type'] = shape_type 
                                newDataEvents.append(event)
                            except KeyError:
                                pass      

                elif (event['type'] == 'ws-rotate_shape' or event['type'] == 'ws-scale_shape'):
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt[event['task_id']]
                        shape_id = json.loads(event['data'])['selectedObject']
                        try:
                            shape_type = figureDict[shape_id]
                            event['metadata'] = dict()
                            event['metadata']['shape_id'] = shape_id 
                            event['metadata']['shape_type'] = shape_type 
                            newDataEvents.append(event)
                        except KeyError:
                            pass

                elif ((event['type'] == 'ws-exit_to_menu') and (activePuzzle in selectedPuzzles)):
                    figureDict.clear()
                    nAttempt[activePuzzle] = nAttempt[activePuzzle] + 1
   
                else :
                    event['task_id'] = activePuzzle
                    if (event['task_id'] in selectedPuzzles):
                        event['n_attempt'] = nAttempt[event['task_id']]
                        event['metadata'] = dict()
                        if (event['type'] == 'ws-check_solution'):
                            dict_views = json.loads(event['data'])['correct']
                            corr = 0
                            for key in dict_views:
                                if key == True:
                                    corr += 1
                            event['metadata']['p_pictures_matched'] = (corr / len(dict_views))*100
                            event['pictures_matched'] = dict_views
                            prevCheck = True
                            prevEvent = event
                        else:
                            newDataEvents.append(event)

    taskDf = pd.DataFrame(newDataEvents, columns=['id', 'time', 'group_id', 'user', 'task_id', 'n_attempt', 'type', 'metadata', 'pictures_matched']) 

    
    shapes_map = {1 : 'cube', 2 : 'pyramid', 3 : 'ramp', 4 : 'cylinder', 5 : 'cone', 6: 'sphere'} 
    data = taskDf
    misconceptionsList = []
    for user in data['user'].unique():
                #Select rows
                user_events = data[data['user'] == user]
                user_events_na_dropped = user_events
                for puzzle in user_events_na_dropped['task_id'].unique():
                    puzzle_events = user_events_na_dropped[user_events_na_dropped['task_id'] == puzzle]
                    for attempt in puzzle_events['n_attempt'].unique():
                        attempt_events = puzzle_events[puzzle_events['n_attempt'] == attempt]
                        completed = False
                        attemptFigureDict = dict()
                        # Para los deletes, por si hay un undo despues tenemos aqui un diccionario sin borrar ninguna 
                        # figura, por si hay que recuperarla.
                        backupFigureDict = dict()
                        attemptList = []
                        totalList = []
                        undoEvents = []
                        prevEvent = None
                        for enum, event in attempt_events.iterrows():

                            if (event['type'] == 'ws-redo_action'):
                                lastUndo = undoEvents[-1]
                                if lastUndo['type'] in ['ws-create_shape', 'ws-delete_shape', 'ws-rotate_shape', 'ws-scale_shape']:
                                    shapeType = shapes_map[lastUndo['metadata']['shape_type']]
                                    identifier = lastUndo['metadata']['shape_id']
                                    key = shapeType + '-' + str(identifier)

                                    if lastUndo['type'] == 'ws-create_shape':
                                        attemptFigureDict[key] = dict()
                                        attemptFigureDict[key]['rotate'] = 0
                                        attemptFigureDict[key]['scale'] = 0

                                    elif lastUndo['type'] == 'ws-delete_shape':
                                        del attemptFigureDict[key]

                                    elif lastUndo['type'] == 'ws-rotate_shape':
                                        attemptFigureDict[key]['rotate'] += 1

                                    elif lastUndo['type'] == 'ws-scale_shape':
                                        attemptFigureDict[key]['scale'] += 1

                                #La volvemos a añadir por si hay que volver a deshacerla o algo así
                                totalList.append(lastUndo)
                                #Y la eliminamos de las acciones a rehacer, si no se reharía siempre la misma
                                undoEvents.pop(-1)

                            if (event['type'] == 'ws-undo_action'):
                                #Obtenemos el último evento
                                contador = -1
                                lastEv = totalList[contador]
                                while lastEv['type'] in ['ws-snapshot', 'ws-check_solution']:
                                    contador -= 1
                                    lastEv = totalList[contador]
                                #Vamos a ver qué tipo de evento era:
                                if lastEv['type'] in ['ws-create_shape', 'ws-delete_shape', 'ws-rotate_shape', 'ws-scale_shape']:
                                    shapeType = shapes_map[lastEv['metadata']['shape_type']]
                                    identifier = lastEv['metadata']['shape_id']
                                    key = shapeType + '-' + str(identifier)

                                    if lastEv['type'] == 'ws-create_shape':
                                        del attemptFigureDict[key]


                                    elif lastEv['type'] == 'ws-delete_shape':
                                        attemptFigureDict[key] = dict()
                                        attemptFigureDict[key]['rotate'] = backupFigureDict[key]['rotate']
                                        attemptFigureDict[key]['scale'] = backupFigureDict[key]['scale']

                                    elif lastEv['type'] == 'ws-rotate_shape':
                                        attemptFigureDict[key]['rotate'] -= 1

                                    elif lastEv['type'] == 'ws-scale_shape':
                                        attemptFigureDict[key]['scale'] -= 1

                                #añadimos el evento deshecho
                                undoEvents.append(lastEv)
                                totalList.pop(contador)


                            if (event['type'] in ['ws-restart_puzzle', 'ws-start_level', 'ws-puzzle_started']):
                                attemptFigureDict.clear()

                            if event['type'] == 'ws-create_shape':
                                shapeType = shapes_map[event['metadata']['shape_type']]
                                identifier = event['metadata']['shape_id']
                                key = shapeType + '-' + str(identifier)
                                attemptFigureDict[key] = dict()
                                attemptFigureDict[key]['rotate'] = 0
                                attemptFigureDict[key]['scale'] = 0

                            if (event['type'] == 'ws-delete_shape'):
                                shapeType = shapes_map[event['metadata']['shape_type']]
                                identifier = event['metadata']['shape_id']
                                key = shapeType + '-' + str(identifier)
                                try:
                                    #Para el de backup
                                    backupFigureDict[key] = dict()
                                    backupFigureDict[key]['rotate'] = attemptFigureDict[key]['rotate']
                                    backupFigureDict[key]['scale'] = attemptFigureDict[key]['scale']
                                    del attemptFigureDict[key]
                                except KeyError:
                                    pass

                            if (event['type'] == 'ws-rotate_shape'):
                                shapeType = shapes_map[event['metadata']['shape_type']]
                                identifier = event['metadata']['shape_id']
                                key = shapeType + '-' + str(identifier)
                                try:
                                    attemptFigureDict[key]['rotate'] += 1
                                except KeyError:
                                    attemptFigureDict[key] = dict()
                                    attemptFigureDict[key]['rotate'] = 0
                                    attemptFigureDict[key]['scale'] = 0
                                    attemptFigureDict[key]['rotate'] += 1

                            if (event['type'] == 'ws-scale_shape'):
                                shapeType = shapes_map[event['metadata']['shape_type']]
                                identifier = event['metadata']['shape_id']
                                key = shapeType + '-' + str(identifier)
                                try:
                                    attemptFigureDict[key]['scale'] += 1
                                except KeyError:
                                    attemptFigureDict[key] = dict()
                                    attemptFigureDict[key]['rotate'] = 0
                                    attemptFigureDict[key]['scale'] = 0
                                    attemptFigureDict[key]['scale'] += 1


                            if (event['type'] in ['ws-snapshot', 'ws-check_solution']):
                                if event['type'] == 'ws-check_solution':
                                    event['p_pictures_matched'] = event['metadata']['p_pictures_matched']
                                    if (event['metadata']['correct'] == True):
                                        completed = True
                                else:
                                    event['p_pictures_matched'] = nan
                                event['shapes_used'] = copy.deepcopy(attemptFigureDict)
                                attemptList.append(event)

                            prevEvent = event
                            if (event['type'] not in ['ws-undo_action', 'ws-redo_action']):
                                totalList.append(event)

                        if completed == True:
                            for elem in attemptList:
                                elem['complete'] = True
                        else:
                            for elem in attemptList:
                                elem['complete'] = False
                        misconceptionsList.extend(attemptList)

    errorDf = pd.DataFrame(misconceptionsList, columns = ['time', 'group_id', 'user', 'task_id', 'n_attempt', 'type', 'complete', 'pictures_matched', 'p_pictures_matched', 'shapes_used']) 
    return errorDf


def tagMisconceptions(misconDf):
    listTags = []
    # Los de Angled Silhouette
    angled = misconDf[misconDf['task_id'] == 'Angled Silhouette']
    for enum, event in angled.iterrows():
        #Cualquier solucion
        event['labels'] = set()
        if event['type'] == 'ws-snapshot':
            event['labels'] = list(event['labels']) 
            listTags.append(event)
            continue 
        keyShapes = event['shapes_used'].keys()
        newList = []
        for shape in keyShapes:
            newList.append(shape.split("-")[0])
            
        if ("ramp" in newList):
            event['labels'].add("Mis1")
            event['labels'].add("Mis1_2b")
            event['labels'].add("s4a")

        elif ("cone" in newList):
            event['labels'].add("Mis1")
            event['labels'].add("Mis1_2b")
            event['labels'].add("s5a")
            
        ##Los que son soluciones incorrectas
        if (event['p_pictures_matched'] < 100.0):
            if len(newList) == 2:
                if ("pyramid" in newList and "cube" in newList):
                    event['labels'].add("Mis2")
                
            elif len(newList) == 3:
                 if (newList.count("pyramid") == 2 and "cube" in newList):
                        #Cubo escalado?
                        for shape in keyShapes:
                            if "cube" in shape:
                                if event['shapes_used'][shape]['scale'] > 0:
                                    event['labels'].add("Mis4")
                                else:
                                    event['labels'].add("Mis3")
                        
        event['labels'] = list(event['labels']) 
        listTags.append(event)       
        
     # Los de Square Cross-Sections
    square = misconDf[misconDf['task_id'] == 'Square Cross-Sections']
    for enum, event in square.iterrows():
        #Cualquier solucion
        event['labels'] = set()
        keyShapes = event['shapes_used'].keys()
        newList = []
        for shape in keyShapes:
            newList.append(shape.split("-")[0])
        
        ##Los que son soluciones incorrectas
        if (event['p_pictures_matched'] < 100.0):
                
            if ("cube" in newList):
                event['labels'].add("Mis2")
        
            if len(newList) == 3:
                    
                if ("pyramid" in newList and "ramp" in newList and "sphere" in newList):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_1")
                    event['labels'].add("s7a")
                elif ("cone" in newList and "ramp" in newList and "cylinder" in newList):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_1")
                    event['labels'].add("s5a")
                elif ("cone" in newList and "ramp" in newList and "sphere" in newList):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_1")
                    event['labels'].add("s5a")
                    event['labels'].add("s7a")
                elif ("pyramid" in newList and "ramp" in newList and "cone" in newList):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_1")
                    event['labels'].add("s8b")
                elif ("pyramid" in newList and "ramp" in newList and "cylinder" in newList):
                    #Rotados?
                    for shape in keyShapes:
                        rot = 0
                        if ("ramp" in shape):
                            if event['shapes_used'][shape]['rotate'] > 0:
                                rot += 1
                        elif("cylinder" in shape):
                            if event['shapes_used'][shape]['rotate'] > 0:
                                rot += 1
                    if rot == 2:
                        event['labels'].add("Mis5")
                    else:
                        event['labels'].add("Mis4")
        event['labels'] = list(event['labels']) 
        listTags.append(event)
        
    # Los de Not Bird
    bird = misconDf[misconDf['task_id'] == 'Not Bird']
    for enum, event in bird.iterrows():
        #Cualquier solucion
        event['labels'] = set()
        keyShapes = event['shapes_used'].keys()
        newList = []
        for shape in keyShapes:
            newList.append(shape.split("-")[0])
        
        ##Los que son soluciones incorrectas
        if (event['p_pictures_matched'] < 100.0):
                
            if len(newList) == 3:
                if ("cube" in newList and "sphere" in newList and "cone" in newList):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_1")
                    event['labels'].add("Mis1_3")
                    event['labels'].add("s2a")
                    event['labels'].add("s5a")
                    event['labels'].add("s7a")
                    
                elif (("cube" in newList and "pyramid" in newList and "cylinder" in newList) or ("cube" in newList and "pyramid" in newList and "sphere" in newList) or ("cube" in newList and "cone" in newList and "cylinder" in newList)):
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("Mis1_3")
                    event['labels'].add("s2a")
                elif ("cylinder" in newList and "sphere" in newList and "cone" in newList):  
                    event['labels'].add("Mis1")
                    event['labels'].add("Mis2")
                    event['labels'].add("s5a")
                    event['labels'].add("s7a")
                        
                elif (newList.count("cylinder") == 2 and "cone" in newList):
                    #Rotados?
                    #Escalados?
                    for shape in keyShapes:
                        sca = 0
                        rot = 0
                        rotCyl = 0
                        if ("cylinder" in shape):
                            if event['shapes_used'][shape]['scale'] > 0:
                                sca += 1
                            if event['shapes_used'][shape]['rotate'] > 0:
                                rotCyl += 1
                        if ("cone" in shape):
                            if event['shapes_used'][shape]['rotate'] > 0:
                                rot += 1
                    if (sca == 0 or rot == 0):
                        event['labels'].add("Mis4")
                    if (sca == 0 ):
                        event['labels'].add("Mis3")
                    if (sca > 0 and rotCyl > 0 and rot > 0):
                        event['labels'].add("Mis5")
                            
                elif ("pyramid" in newList and "sphere" in newList and "cylinder" in newList):
                    #Rotados?
                    #Escalados?
                    for shape in keyShapes:
                        scaCyl = 0
                        rotPyr = 0
                        if ("cylinder" in shape):
                            if event['shapes_used'][shape]['scale'] > 0:
                                scaCyl += 1
                        
                        if ("pyramid" in shape):
                            if event['shapes_used'][shape]['rotate'] > 0:
                                rotPyr += 1
                                    
                    if scaCyl > 0 and rotPyr > 0:
                        event['labels'].add("Mis5")
                            
                    elif (scaCyl == 0):
                        event['labels'].add("Mis3")
                        
                    elif (rotPyr == 0):
                        event['labels'].add("Mis4")
        event['labels'] = list(event['labels'])               
        listTags.append(event)
        
    labelsDf = pd.DataFrame(listTags, columns = ['time', 'group_id', 'user', 'task_id', 'n_attempt', 'type', 'complete', 'pictures_matched', 'p_pictures_matched', 'shapes_used', 'labels']) 
    return labelsDf


def misconceptionsTagged(dataEvents):
    df1 = sequenceWithinPuzzlesForMisconceptions(dataEvents)
    df2 = tagMisconceptions(df1)
    return df2



