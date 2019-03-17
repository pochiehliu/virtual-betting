import pandas as pd

##### read csv files
player_df = pd.read_csv('player_db_2018_june.csv')
game_df = pd.read_csv('game_db_2018_june.csv')

##### build player
count = 1
player_dict = {}

# TODO: NEED TO CHANGE THE FIRST LAST; SOME PEOPLE HAVE > 2 NAMES
for name in player_df.name.unique():
	player_dict[name] = count
	first, last = name.split(' ')
	%%sql
	INSERT INTO player VALUES (count, first, last)
	count += 1

##### build team
count = 1
team_dict = {}

# TODO: NEED TO UPDATE TEAM INSERTS TO NEW SCHEMA
# need a city list
for team in player_df.team.unique():
	team_dict[team] =  count
	%%sql
	INSERT INTO team VALUES (count, name, city)
	count += 1

##### game table
for row, el in game_df.iterrows():
	%%sql
	INSERT INTO game(
		el[1],
		el[2],
		team_dict[el[5]],
		team_dict[el[6]],
		el[20], el[21], el[22], el[23],
		el[12], el[13], el[14], el[15],
		el[24],el[16]
		)

##### player_game_stats
for row, el in player_df.iterrows():
	print(el)
	break
	%%sql
	INSERT IN player_game_stats VALUES(
		el[1], # g_id
		player_dict[el[7]], # p_id
		team_dict[el[3]], # t_id
		el[8], # mp
		el[9], el[10], #fg, fga
		el[12], el[13], # tp, tpa
		el[15], el[16], # ft, fta
		el[18], el[19], # orb, drb
		el[21], # ast
		el[22], # stl
		el[23], # blk
		el[24], # tov
		el[25], # pf
		el[26], # pts?
		el[27], # pm
		el[32], # orbp
		el[33], # drbp
		el[34], # trbp
		el[35], # astp
		el[36], # stlp
		el[37], # blkp
		el[38], # tovp
		el[39], # usgp
		el[40], # ortg
		el[41]  # drtg
		)


#### users, sportbook, bet_type tables are skipped for now
