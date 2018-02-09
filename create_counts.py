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
# github = Github('soofaloofa', 'password')
# user = github.get_user()
#
# repository = user.get_repo('soofaloofa.github.io')
#
# file_content = repository.get_contents('README.md')


def _extract_moment_data(m):
    period, _, game_clock, shot_clock, _2, pos = tuple(m)
    _df = pd.DataFrame(pos, columns=['team_id', 'player_id',
                                     'x_loc', 'y_loc', 'radius'])
    _df['game_clock'] = game_clock
    _df['shot_clock'] = shot_clock
    _df['period'] = period
    return _df


def _shape_event(ev):
    moments = ev['moments']
    vis = ev['visitor']
    home = ev['home']
    event_id = ev['eventId']
    movements = []
    movements = list(map(_extract_moment_data, moments))

    if len(movements) > 1:
        df = pd.concat(movements)
        df['event_id'] = event_id
        df['home_team_id'] = home['teamid']
        df['visitor_team_id'] = vis['teamid']
        return df
    else:
        return None


def _reshape_raw_json(js):
    evs = js['events']

    p = Pool(cpu_count())
    processed_evs = p.map(_shape_event, evs)
    processed_evs = [ev for ev in processed_evs if ev is not None]
    game = pd.concat(processed_evs)
    game['date'] = js['gamedate']
    game['id'] = js['gameid']
    return game


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


if __name__ == '__main__':
    f = Archive7z(open('data/01.01.2016.CHA.at.TOR (1).7z', 'rb'))
    js = json.load(f.getmember('0021500492.json'))

    game = dd.from_pandas(_reshape_raw_json(js), npartitions=cpu_count()*3)

    # Take the player position at the beginning of every second
    print("game reshaped")
    game_blocked = game.groupby('player_id').apply(_block_labels)
    print("blocked")
    game_subsample = game_blocked.groupby(['player_id',
                                           'time_block']).apply(_sample)
    print(game_subsample.head())
    game_subsample.to_csv("data/reshaped")
