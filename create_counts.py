"""create_counts.py.

Creates a counts matrix for time {players|teams} spend in specific n x n sized
squares on the basketball court.
"""
# from requests import
# from github import Github
from py7zlib import Archive7z
import pandas as pd
import json
from multiprocessing import Pool, cpu_count
import dask.dataframe as dd
import dask.bag as db
import dask.bag as db
from zone_classifier import ZoneClassifier
from github_download import read_write_file_from_github

from github import Github
import getpass
import os
import re

TIME_INTERVAL = 0.5

def _extract_moment_data(m):
    d = {}
    period, _, game_clock, shot_clock, _2, pos = tuple(m)
    # _df = pd.DataFrame(pos, columns=['team_id', 'player_id',
    #                                  'x_loc', 'y_loc', 'radius'])
    # _df['game_clock'] = game_clock
    # _df['shot_clock'] = shot_clock
    # _df['period'] = period

    d['team_id'] = pos[0]
    d['player_id'] = pos[1]
    d['x_loc'] = pos[2]
    d['y_loc'] = pos[3]
    d['radius'] = pos[4]
    d['game_clock'] = game_clock
    d['shot_clock'] = shot_clock

    return _df


def _shape_event(ev):
    print("moments")
    moments = ev['moments']
    # vis = ev['visitor']
    # home = ev['home']
    # event_id = ev['eventId']
    for m in moments:
        print(m)
    # movements = list(map((lambda x: print(x[2])), moments))

    # if len(movements) > 1:
    #     df = pd.concat(movements)
    #     df['event_id'] = event_id
    #     df['home_team_id'] = home['teamid']
    #     df['visitor_team_id'] = vis['teamid']
    #     return df
    # else:
    #     return None


# assumes that moments are pre sorted
def _reshape_raw_json(js):
    keep = []
    evs = js['events']
    for ev in evs:
        moments = ev['moments']
        vis = ev['visitor']
        home = ev['home']
        event_id = ev['eventId']
        prev_time = moments[0][2] if len(moments) > 0 else None
        for m in moments:
            time = m[2]
            if time < prev_time - 0.5:
                period, game_clock, _, shot_clock, _2, pos = tuple(m)
                d = {}
                d['team_id'] = []
                d['player_id'] = []
                d['x_loc'] = []
                d['y_loc'] = []
                d['radius'] = []
                for r in pos:
                    d['team_id'] += [r[0]]
                    d['player_id'] += [r[1]]
                    d['x_loc'] += [r[2]]
                    d['y_loc'] += [r[3]]
                    d['radius'] += [r[4]]
                d['game_clock'] = game_clock
                d['shot_clock'] = shot_clock
                d['home'] = home['teamid']
                d['visitor'] = vis['teamid']
                d['event_id'] = event_id
                d['quarter'] = period
                keep.append(d)
                prev_time = time
    return keep
    # map(_shape_event, evs)

    # p = Pool(cpu_count())
    # processed_evs = p.map(_shape_event, evs)
    # processed_evs = [ev for ev in processed_evs if ev is not None]
    # game = pd.concat(processed_evs)
    # game['date'] = js['gamedate']
    # game['id'] = js['gameid']
    # return game


def _block(row, info):
    clock_time = row['game_clock']
    if clock_time < (info["prev_clock_time"] - info["inc"]):
        info["cur_label"] += 1
    info["prev_clock_time"] = clock_time
    return info['cur_label']


def _block_labels(g, inc=0.5):
    clock = g.sort_values(by='game_clock', ascending=False)
    info = {}
    info['prev_clock_time'] = g.iloc[0]['game_clock']
    info['inc'] = inc
    info['cur_label'] = 0
    clock['time_block'] = clock.apply(lambda x: _block(x, info), axis=1)
    # blocks = [_block(tup.game_clock, info) for tup in clock.itertuples()]
    return clock


def _sample(g, inc=0.5):
    """Short summary.

    Parameters
    ----------
    g : Pandas.DataFrame
        A dataframe of movements corresponding to a single play
        and specific player.

    inc : float
        The increment with which to sample

    Returns
    -------
    type
        A dataframe thinned to inc-second intervals.

    """
    g.sort_values('game_clock', inplace=True, ascending=False)

    start = g['game_clock'].max()
    end = g['game_clock'].min()
    result = []
    while start >= end:
        d = g[g.game_clock <= start].iloc[0].tolist()
        result.append(d)
        start -= inc
    # take a beginning sample if we have an event shorter than half a second
    if len(result) < 2:
        result.append(g.tail(1).iloc[0].tolist())

    return pd.DataFrame(result, columns=g.columns)


def _load_7z_file_json(f):
    print("testsetst")
    arch = Archive7z(f)
    json_names = [n for n in arch.list() if '.json' in n]
    if len(json_names) > 1:
        raise AttributeError
    fname = json_names[0]
    print(fname)
    print("test")
    return json.load(arch.getmember(fname))

def convert(file_name):

    data = pd.read_csv('./data/no_label/'+filename)
    data = full_to_half_full(data)
    data = half_full_to_half(data)
    # save data as converted
    data.to_csv('./data/converted/'+filename,index=False)


def _get_label(df, classifier):
    df = full_to_half_full(data)
    df = half_full_to_half(data)

def _label_offense_defense(df):
    # gets all first quarter points in half of the court
    df['of_def_flag'] = None
    first_q_pts = df[(df['EVENTMSGTYPE'] == 1) & (df.x_loc > 47) \
                     & (df.PERIOD == 1) & (df.PLAYER1_ID == df.player_id)]
    print(first_q_pts.x_loc.std())
    print(first_q_pts.x_loc.mean())
    team_id_count = first_q_pts.team_id.value_counts().idxmax()
    print(team_id_count)
    print(((df.team_id == team_id_count) & ((df.PERIOD == 1) | (df.PERIOD == 2))).sum())
    df.loc[((df.team_id == team_id_count) &
       ((df.PERIOD == 1) | (df.PERIOD == 2))), 'of_def_flag'] = 'of'
    df.loc[((df.team_id != team_id_count) &
       ((df.PERIOD == 1) | (df.PERIOD == 2))), 'of_def_flag'] = 'def'
    df.loc[((df.team_id == team_id_count) &
       ((df.PERIOD == 3) | (df.PERIOD == 4))), 'of_def_flag'] = 'def'
    df.loc[((df.team_id != team_id_count) &
       ((df.PERIOD == 3) | (df.PERIOD == 4))), 'of_def_flag'] = 'of'
    # gets the team that is on offense on this half of the court

    # df[df.team_id == ]
    # data[data.x_loc > 47,'y_loc']
    return df['of_def_flag']


def _process(file, logs, classifier, repo):
    read_write_file_from_github(file, repo)
    f = Archive7z(open('data/temp/'+file.name, 'rb'))
    json_name = f.getnames()[0]
    js = json.load(f.getmember(json_name))
    game_id = re.search('([0-9]*)\.', json_name).group(1)
    for l in logs:
        l_label = re.search('([0-9]*)\.', l.name).group(1)
        if l_label.strip() == game_id.strip():
            read_write_file_from_github(l, repo)
            break

    pbp = pd.read_csv('data/temp/'+l.name)
    reshaped = _reshape_raw_json(js)
    df = pd.concat(list(map(pd.DataFrame, reshaped)))
    #
    df['court_location'] = classifier.label(df[["x_loc", "y_loc"]])
    #
    pbp['EVENTNUM'] = pbp['EVENTNUM'].astype('int')
    df['event_id'] = df['event_id'].astype('int')
    #
    print("============size df.index ================")
    print(len(df.index))
    df = df.merge(pbp, left_on='event_id', right_on='EVENTNUM', how='left')
    print("============size df.index after================")
    print(len(df.index))
    df['of_def_flag'] = _label_offense_defense(df)
    df.to_csv("data/processed_games/"+l.name)
    #
    os.remove('data/temp/'+file.name)
    os.remove('data/temp/'+l.name)
    return l.name



if __name__ == '__main__':
    classifier = ZoneClassifier('data/train.csv')
    # fs = open('data/01.01.2016.CHA.at.TOR (1).7z', 'rb')
    #
    done = {}
    error_log = open("errors.txt", "w+")

    username = 'samghelms'
    password = 'REDACTED'

    github = Github(username, password)
    usr = github.get_user()
    repository = usr.get_repo('nba-movement-data')

    games = repository.get_dir_contents('data')
    logs = repository.get_dir_contents('data/events')

    for g in games:
        try:
            if g.name not in done.keys():
                game_id = _process(g, logs, classifier, repository)
                done[g.name] = str(game_id)
                with open("finished.json", "w+") as done_log:
                    json.dump(done, done_log)
                print ("finished: "+str(g.name))
        except:
            error_log.write(g.name)

    error_log.close()
        # g_label =
        # for l in logs:
        #     l_label = re.search('events/([0-9]*)\.', logs[0].path).group(1)
        #     if
        #         game_log_pairs.append((g, l))

    # _process(fs, classifier)
    #
    # directory_to_download = raw_input("Directory to download: ")
    # files = repository.get_dir_contents(directory_to_download)

    # zoned = _label_zones(reshaped)
    # 50mb size chunk

    # db.read_text('data/01.01.2016.CHA.at.TOR (1).7z').map(_load_7z_file_json)
    # num_partitions = (game.memory_usage().sum()/1000)/50
    # chunksize = len(game.index)/num_partitions
    # print(num_partitions)
    # print(chunksize)
    # game = dd.from_pandas(game, chunksize=chunksize)
    # # game.apply()
    # print("fully loaded")

    # game = dd.from_pandas(_reshape_raw_json(js), npartitions=cpu_count()*3)

    # # Take the player position at the beginning of every second
    # print("game reshaped")
    # game_blocked = game.groupby('player_id').apply(_block_labels)
    # print("blocked")
    # game_subsample = game_blocked.groupby(['player_id',
    #                                        'time_block']).apply(_sample)
    # print(game_subsample.head())
    # game_subsample.to_csv("data/reshaped")
