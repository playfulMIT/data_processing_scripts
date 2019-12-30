import pandas as pd
import numpy as np
import json

# USAGE EXAMPLE
# dataEvents = pd.read_csv('/Users/jruipere/Dropbox (MIT)/Game-based Assessment/datasets/shadowspect_10-28-2019.csv', sep=";")
# userFunnelByPuzzle = computeFunnelByPuzzle(dataEvents)

def computeFunnelByPuzzle(dataEvents, group='all'):
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in
                           dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]

    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[
        ((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']

    # filtering to process only the list of groups passed as argument
    if(group != 'all'):
        dataEvents = dataEvents[dataEvents['group'].isin(group)]

    # userFunnelDict key: (group~user~puzzle), json values: started, create_shape, submitted, completed
    userFunnelDict = dict()

    for user in dataEvents['group_user_id'].unique():

        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None

        for enum, event in user_events.iterrows():

            if (event['type'] in ['ws-start_level', 'ws-puzzle_started']):
                user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']
                if (user_puzzle_key not in userFunnelDict.keys()):
                    userFunnelDict[user_puzzle_key] = json.loads(
                        '{"started": 0, "create_shape": 0, "submitted": 0, "completed": 0}')

            if (event['type'] == 'ws-puzzle_started'):
                userFunnelDict[user_puzzle_key]['started'] += 1
            elif (event['type'] == 'ws-create_shape'):
                userFunnelDict[user_puzzle_key]['create_shape'] += 1
            elif (event['type'] == 'ws-check_solution'):
                userFunnelDict[user_puzzle_key]['submitted'] += 1
            elif (event['type'] == 'ws-puzzle_complete'):
                userFunnelDict[user_puzzle_key]['completed'] += 1

    userFunnelList = []
    for key in userFunnelDict.keys():
        key_split = key.split('~')
        userFunnelList.append([key_split[0], key_split[1], key_split[2], userFunnelDict[key]])

    userFunnelByPuzzle = pd.DataFrame(userFunnelList, columns=['group', 'user', 'task_id', 'funnel'])

    return userFunnelByPuzzle