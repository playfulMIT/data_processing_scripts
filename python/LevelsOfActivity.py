import pandas as pd
from datetime import datetime
import numpy as np
import json

# USAGE EXAMPLE
# dataEvents = pd.read_csv('datasets/shadowspect_10-28-2019.csv', sep=";")
# levelsOfActivity = computeLevelsOfActivity(dataEvents)

def computeLevelsOfActivity(dataEvents, group = 'all'):
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')
    
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
    
    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']
    
    activity_by_user = dataEvents.groupby(['group_user_id', 'group', 'user']).agg({'id':'count',
                                             'type':'nunique'}).reset_index().rename(columns={'id':'n_events',
                                                                                              'type':'n_different_events'})
    activity_by_user.index = activity_by_user['group_user_id'].values
    
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_completed_puzzles'] = np.nan
    
    completedPuzzles = dict()   
    for user in activity_by_user['group_user_id'].unique():
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        completedPuzzles[user] = set()

        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 
        activeTime = []

        for enum, event in user_events.iterrows():
            # If it is the first event
            if(previousEvent is None):
                previousEvent = event
                continue

            if(event['type'] == 'ws-puzzle_complete'):
                # adding key of the completed puzzle
                completedPuzzles[user].add(json.loads(event['data'])['task_id']) 

            delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
            if((delta_seconds < theresHoldActivity)):
                activeTime.append(delta_seconds)

            previousEvent = event

        activity_by_user.at[user, 'active_time'] = round(np.sum(activeTime)/60,2)
        activity_by_user.at[user, 'n_completed_puzzles'] = len(completedPuzzles[user])
        
    activity_by_user.drop(columns=['group_user_id'], inplace=True)
    activity_by_user = pd.melt(activity_by_user, id_vars=['group', 'user'], 
        value_vars=['n_events', 'n_different_events', 'active_time', 'n_completed_puzzles'], 
        var_name='metric', value_name='value')
    
    return activity_by_user
