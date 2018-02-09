# https://github.com/sealneaward/movement-quadrants/blob/master/knn.py
import pandas pd

def label(file_name):
    ###########################################
    # Load data and map location categories
    ###########################################
    dictionary_1 = {'Right Side(R)':1, 'Left Side(L)':2, 'Center(C)':3, 'Right Side Center(RC)':4, 'Left Side Center(LC)':5}
    dictionary_2 = {'Less Than 8 ft.':1, '8-16 ft.':2, '16-24 ft.':3, '24+ ft.':4}
    dictionary_3 = {'Restricted Area':1, 'In The Paint (Non-RA)':2, 'Mid-Range':3, 'Right Corner 3':4, 'Left Corner 3':5, 'Above the Break 3':6}

    data = pd.read_csv('./data/train.csv')
    data['shot_zone_area'] = data['shot_zone_area'].map(dictionary_1)
    data['shot_zone_range'] = data['shot_zone_range'].map(dictionary_2)
    data['SHOT_ZONE_BASIC'] = data['SHOT_ZONE_BASIC'].map(dictionary_3)

    data = data.dropna(subset = ['LOC_X','LOC_Y','shot_zone_area', 'shot_zone_range'])

    data['SHOT_ZONE_BASIC'] = data['SHOT_ZONE_BASIC'].astype(int)
    data['shot_zone_range'] = data['shot_zone_range'].astype(int)
    data['shot_zone_area'] = data['shot_zone_area'].astype(int)

    ###################################################
    # Combine areas and ranges
    ###################################################
    data.loc[(data.shot_zone_area == 1) & (data.shot_zone_range == 1),'shot_zone_range_area'] = 1
    data.loc[(data.shot_zone_area == 1) & (data.shot_zone_range == 2),'shot_zone_range_area'] = 2
    data.loc[(data.shot_zone_area == 1) & (data.shot_zone_range == 3),'shot_zone_range_area'] = 3
    data.loc[(data.shot_zone_area == 1) & (data.shot_zone_range == 4),'shot_zone_range_area'] = 4

    data.loc[(data.shot_zone_area == 2) & (data.shot_zone_range == 1),'shot_zone_range_area'] = 5
    data.loc[(data.shot_zone_area == 2) & (data.shot_zone_range == 2),'shot_zone_range_area'] = 6
    data.loc[(data.shot_zone_area == 2) & (data.shot_zone_range == 3),'shot_zone_range_area'] = 7
    data.loc[(data.shot_zone_area == 2) & (data.shot_zone_range == 4),'shot_zone_range_area'] = 8

    data.loc[(data.shot_zone_area == 3) & (data.shot_zone_range == 1),'shot_zone_range_area'] = 9
    data.loc[(data.shot_zone_area == 3) & (data.shot_zone_range == 2),'shot_zone_range_area'] = 10
    data.loc[(data.shot_zone_area == 3) & (data.shot_zone_range == 3),'shot_zone_range_area'] = 11
    data.loc[(data.shot_zone_area == 3) & (data.shot_zone_range == 4),'shot_zone_range_area'] = 12

    data.loc[(data.shot_zone_area == 4) & (data.shot_zone_range == 1),'shot_zone_range_area'] = 13
    data.loc[(data.shot_zone_area == 4) & (data.shot_zone_range == 2),'shot_zone_range_area'] = 14
    data.loc[(data.shot_zone_area == 4) & (data.shot_zone_range == 3),'shot_zone_range_area'] = 15
    data.loc[(data.shot_zone_area == 4) & (data.shot_zone_range == 4),'shot_zone_range_area'] = 16

    data.loc[(data.shot_zone_area == 5) & (data.shot_zone_range == 1),'shot_zone_range_area'] = 17
    data.loc[(data.shot_zone_area == 5) & (data.shot_zone_range == 2),'shot_zone_range_area'] = 18
    data.loc[(data.shot_zone_area == 5) & (data.shot_zone_range == 3),'shot_zone_range_area'] = 19
    data.loc[(data.shot_zone_area == 5) & (data.shot_zone_range == 4),'shot_zone_range_area'] = 20

    # plot_shot(data)

    data = data.drop_duplicates(subset=['GAME_EVENT_ID','GAME_ID'], inplace=False)
    # data[['LOC_RHO','LOC_PHI']] = data[['LOC_X','LOC_Y']].apply(cart2pol, axis=1)

    X = data[['LOC_X','LOC_Y']]
    Y = data[['shot_zone_range_area']]
    train_x, test_x, train_y, test_y = train_test_split(X, Y, test_size=0.2, random_state=0123)

    ####################################################################################
    # Train, Predict, Evaluate TODO: Serialize the classifier so code is more efficient
    ####################################################################################
    knn = KNeighborsClassifier(n_neighbors=4)
    knn.fit(train_x, train_y)
    predictions = knn.predict(test_x)

    # KNN is 99.56% accurate with single range, 99.07% accurate with two ranges
    print str(classification_report(test_y, predictions, digits=4))

    ######################################################################
    # Use KNN Model to label full converted movement set
    ######################################################################
    # read data and rename columns
    data = pd.read_csv('./data/converted/'+file_name)
    data[['LOC_X','LOC_Y']] = data[['x_loc','y_loc']]

    # predict and label shot zones
    X = data[['LOC_X','LOC_Y']]
    zones = knn.predict(X)
    data['shot_zone_range_area'] = zones
    plot_shot(data)

    # map real labels
    # data['range_area_basic'] = data['shot_zone_range_area']
    #
    # dictionary_1 = {1:'Right Side(R)', 2:'Left Side(L)', 3:'Center(C)', 4:'Right Side Center(RC)', 5:'Left Side Center(LC)'}
    # dictionary_2 = {1:'Less Than 8 ft.', 2:'8-16 ft.', 3:'16-24 ft.', 4:'24+ ft.'}
    # dictionary_3 = {1:'Restricted Area', 2:'In The Paint (Non-RA)', 3:'Mid-Range', 4:'Right Corner 3', 5:'Left Corner 3', 6:'Above the Break 3'}
    #
    # data['range_basic'] = data['range_basic'].map(dictionary_2)
    #
    # # get rid of excess data
    # data = data.drop('LOC_X', axis=1, inplace=False)
    # data = data.drop('LOC_Y', axis=1, inplace=False)
    # data = data.drop('shot_zone_range', axis=1, inplace=False)
    #
    # # write to labelled folder
    # data.to_csv('./data/label/'+file_name, index=False)

label('0021500139.csv')
