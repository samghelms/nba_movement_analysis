import glob
import pandas as pd

def make_team_counts():
	g = glob.glob('data/processed_games/[0-9]*.csv')

	total_counts = None
	for n in g:
	    print(n)
	    df = pd.read_csv(n, low_memory=False)
	    df = df[df.team_id != -1]
	    df = df.groupby(['team_id', 'court_location', 'of_def_flag'])['event_id'].count()
	    if total_counts is None:
	        total_counts = df  
	    else:
	        total_counts = total_counts.add(df, fill_value=0.0)
	        print(len(total_counts.index))  

	total_counts.to_csv("data/time_in_location_team.csv")

def make_player_counts():
	g = glob.glob('data/processed_games/[0-9]*.csv')
	total_counts = None
	for n in g:
	    print(n)
	    df = pd.read_csv(n, low_memory=False)
	    df = df[df.team_id != -1]
	    df = df.groupby(['player_id', 'court_location', 'of_def_flag'])['event_id'].count()
	    if total_counts is None:
	        total_counts = df  
	    else:
	        total_counts = total_counts.add(df, fill_value=0.0)
	        print(len(total_counts.index))  

	total_counts.to_csv("data/time_in_location_player.csv")

def make_manova():
	g = glob.glob('data/processed_games/[0-9]*.csv')

	total_counts = None
	for n in g:
	    print(n)
	    df = pd.read_csv(n, low_memory=False)
	    df = df[df.team_id != -1]
	    df = df.groupby(['team_id', 'player_id', 'court_location', 'of_def_flag'])['event_id'].count()
	    if total_counts is None:
	        total_counts = df  
	    else:
	        total_counts = total_counts.add(df, fill_value=0.0)
	        print(len(total_counts.index))  
	total_counts = pd.DataFrame(total_counts)
	total_counts.columns = ['count']
	total_counts.to_csv("data/player_team_counts.csv")
	    

if __name__ == '__main__':
	print("making team counts")
	make_team_counts()

	print("making player counts")
	make_player_counts()

	print("making manova data")
	make_manova()


