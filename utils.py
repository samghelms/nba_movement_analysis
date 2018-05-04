# https://github.com/sealneaward/movement-quadrants/blob/master/convert_full_court_to_half_court.py

########################################################################
# Convert all full court coordinates in data to half court coordinates
########################################################################
def _half_full_to_half(data):

    # convert to half court scale
    # note the x_loc and the y_loc are switched in shot charts from movement data (charts are perpendicular)
    data['x_loc_copy'] = data['x_loc']
    data['y_loc_copy'] = data['y_loc']

    # Range conversion formula
    # http://math.stackexchange.com/questions/43698/range-scaling-problem

    data['x_loc'] = data['y_loc_copy'].apply(lambda y: 250 * (1 - (y - 0)/(50 - 0)) + -250 * ((y - 0)/(50 - 0)))
    data['y_loc'] = data['x_loc_copy'].apply(lambda x: -47.5 * (1 - (x - 0)/(47 - 0)) + 422.5 * ((x - 0)/(47 - 0)))
    data = data.drop('x_loc_copy', axis=1, inplace=False)
    data = data.drop('y_loc_copy', axis=1, inplace=False)

    return data

##############################################################################################################
# Convert all full court coordinates that occur in other half of court to occur in one half of a full court
##############################################################################################################
def _full_to_half_full(data):

    # first force all points above 47 to their half court counterparts
    data.loc[data.x_loc > 47,'y_loc'] = data.loc[data.x_loc > 47, 'y_loc'].apply(lambda y: 50 - y)
    data.loc[data.x_loc > 47,'x_loc'] = data.loc[data.x_loc > 47, 'x_loc'].apply(lambda x: 94 - x)

    return data

def markdown_table(df):
    # https://stackoverflow.com/questions/33181846/programmatically-convert-pandas-dataframe-to-markdown-table
    # You don't need these two lines
    # as you already have your DataFrame in memory

    # Get column names
    cols = df.columns

    # Create a new DataFrame with just the markdown
    # strings
    df2 = pd.DataFrame([['---',]*len(cols)], columns=cols)

    #Create a new concatenated DataFrame
    df3 = pd.concat([df2, df])

    #Save as markdown
    return df3.to_csv(sep="|", index=False)

import pandas as pd
def create_counts_mat(df):
    mi = pd.MultiIndex.from_tuples(tuple([(team, (loc)) 
                                          for team, loc in list(df.index) ]))
    df.index = (mi)
    df.columns = ['counts']
    mat = df.unstack(fill_value = 0.0)
    
    return mat
    
import json

def label_teams(mat):
    team_names_raw = json.load(open('data/teams.json'))
    team_names = {}
    for d in team_names_raw:
        team_names[d['teamId']] = d['teamName']
    mat = mat.rename(mapper=team_names, axis='index')
    return mat


def label_players(mat):
    player_names_raw = json.load(open('data/players.json'))
    player_names = {}
    for d in player_names_raw:
        player_names[d['playerId']] = d['firstName'] + ' ' + d['lastName']
    mat = mat.rename(mapper=player_names, axis='index')
    return mat

def label_zones(iterable):
    zones = ['Right Side(R)', 'Left Side(L)', 'Center(C)', 'Right Side Center(RC)', 'Left Side Center(LC)']
    dists = ['Less Than 8 ft.', '8-16 ft.', '16-24 ft.', '24+ ft.']

    j = 0
    labels = {}
    reverse_labels = {}

    for z in zones:
        for d in dists:
            j += 1
            labels[j] = z + ", " + d
            reverse_labels[labels[j]] = j

    return [labels[z] for z in iterable]

import seaborn as sns
import matplotlib.pyplot as plt

def create_pair_grid(mat, save=False, savename="pairwise.png"):
    g = sns.pairplot(mat)
    xlabels,ylabels = [],[]

    for ax in g.axes[-1,:]:
        xlabel = ax.xaxis.get_label_text()
        xlabels.append(xlabel)
    for ax in g.axes[:,0]:
        ylabel = ax.yaxis.get_label_text()
        ylabels.append(ylabel)

    for i in range(len(xlabels)):
        for j in range(len(ylabels)):
            if i != j:
                g.axes[j,i].xaxis.set_label_text(xlabels[i])
                g.axes[j,i].yaxis.set_label_text(ylabels[j])
    if save:
        g.savefig(savename, dpi=480)

    plt.show()

from draw_court import alpha_shape, plot_polygon
from shapely.geometry.point import Point
from shapely.geometry.multipoint import MultiPoint
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from draw_court import draw_court 
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib as mpl

class CourtLoadingsPlotter:
    def __init__(self, zone_data, zones):
        self.zones = zone_data
        self.hulls = {}
        for zone in zones:
            data_filt = zone_data[zone_data.zone == zone]
            new_points = MultiPoint(data_filt[['LOC_X','LOC_Y']].to_records(index=False))
            concave_hull, edge_points = alpha_shape(new_points, alpha=0.1)
            self.hulls[zone] = concave_hull
    
    def plot_polygon(self, polygon, ax, loading, min_val, max_val):
        margin = .3
        x_min, y_min, x_max, y_max = polygon.bounds
        norm = mpl.colors.Normalize(vmin=min_val,vmax=max_val)
        intensity = norm(loading) if loading is not None else np.random.random_sample()
        patch = PolygonPatch(polygon, color=cm.seismic(intensity), zorder=4)
        ax.add_patch(patch)
            
    def plot_loadings(self, ax, loadings, min_val=-1, max_val=1, title=None, fontsize=20):
        ax.set_xlim(-250, 250)
        ax.set_ylim(422.5, -47.5)
        if title:
            ax.set_title(title, fontsize=fontsize)
        for zone, loading in loadings:
            self.plot_polygon(self.hulls[zone], ax, loading, min_val, max_val)


