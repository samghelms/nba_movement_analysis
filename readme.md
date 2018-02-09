
# Intro

# Helpful resources

## Explains the schema of our data
https://danvatterott.com/blog/2016/06/16/creating-videos-of-nba-action-with-sportsvu-data/

## How we organize

defense - Grid space xrange : [0,1] yrange: [0,1]


## Pipeline steps:

1. Send request to github, unzip data
2. Reshape data, extract every second:
2.a. result: [player/ball label, x location, y location, time, away/home, quarter, ..., event code?]
2.a.i. get rid of duplicate positions
2.a.ii filter so only sampling position every second of the play [also the ending of the play]
2.b. add "defense/offense" label to result 1 = offense, 2 = defense
3. Use KNN to label x and y location zone.
4. data['location'] = results of KNN
5. data[data.player/ball label == 'ball' and data.event code == 'score'].location = 'BASKET'
