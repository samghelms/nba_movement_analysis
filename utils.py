# https://github.com/sealneaward/movement-quadrants/blob/master/convert_full_court_to_half_court.py

########################################################################
# Convert all full court coordinates in data to half court coordinates
########################################################################
def half_full_to_half(data):

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
def full_to_half_full(data):

    # first force all points above 47 to their half court counterparts
    data.loc[data.x_loc > 47,'y_loc'] = data.loc[data.x_loc > 47, 'y_loc'].apply(lambda y: 50 - y)
    data.loc[data.x_loc > 47,'x_loc'] = data.loc[data.x_loc > 47, 'x_loc'].apply(lambda x: 94 - x)

    return data
