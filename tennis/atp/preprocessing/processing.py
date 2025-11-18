import pandas as pd
import sys
import time
import math

class Processor:
    def __init__(self, data):
        print('initializing processor...')
        print(f'data: {len(data)}')
        # self.data = data
        self.data = data[data['Opponent_Name'].isin(data['Player_Name'])]
        print(f'self.data: {len(self.data)}')
        self.surface_map = self.createSurfaceMap(data)
        self.round_map = self.createRoundMap(data)
        self.best_of_map = self.createBestOfMap(data)
        self.json_data = self.convert_to_json()
        self.hand_map = self.createHandMap(data)

    def createSurfaceMap(self, data):
        surface_options = data['Surface'].unique()
        surface_map = {}
        for index, option in enumerate(surface_options):
            surface_map[option] = index
        return surface_map
    
    def createRoundMap(self, data):
        round_options = data['Round'].unique()
        round_map = {}
        for index, option in enumerate(round_options):
            round_map[option] = index
        return round_map
    
    def createBestOfMap(self, data):
        best_of_options = data['Best_of'].unique()
        best_of_map = {}
        for index, option in enumerate(best_of_options):
            best_of_map[option] = index
        return best_of_map

    def createHandMap(self, data):
        hand_options = data['Dominant_Hand'].unique()
        hand_map = {}
        for index, option in enumerate(hand_options):
            hand_map[option] = index
        return hand_map
    
    def calculateAge(self, dob, match_date):
        # convert the dob from 20010816.0 to 2001-08-16
        dob = pd.to_datetime(dob, format='%Y%m%d')

        # calculate the age of the player based on the dob
        dob = pd.to_datetime(dob)
        match_date = pd.to_datetime(match_date)

        age = match_date.year - dob.year

        return age

    def convert_to_json(self):
        data = pd.read_csv('data/atp_player_match_data.csv')

        # remove rows where the Opponent_Name isn't in the data as a player
        data = data[data['Opponent_Name'].isin(data['Player_Name'])]

        print('converting to json...')
        json_conversion_start_time = time.time()

        json_data = {}
        for _, row in data.iterrows():
            if row['Player_Name'] in json_data.keys():
                continue

            # get all the games for a player
            player_data = data[(data['Player_Name'] == row['Player_Name'])]
            # sort the player data by date descending
            player_data = player_data.sort_values(by='Date', ascending=False)
            # add the player data to the json
            json_data[row['Player_Name']] = player_data.to_dict('records')
        
        # print the time it took to convert to json
        elapsed_time = time.time() - json_conversion_start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f'done converting to json, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
        
        return json_data
    
    def getGamesInLastXYears(self, player_name, date, x):
        """
        Get all the games in the last x years for a player
        x: int[] number of games to get
        """
        last_x_game_data = {
            'wins': 0,
            'losses': 0,
            'hard_count': 0,
            'clay_count': 0,
            'grass_count': 0,
            'carpet_count': 0,
            'avg_rank_step': 0,
            'player_1_games': 0,
            'player_2_games': 0,
            'player_3_games': 0,
            'opponent_1_games': 0,
            'opponent_2_games': 0,
            'opponent_3_games': 0,
            'player_sets': 0,
            'opponent_sets': 0,
            'avg_ace_rate': 0,
            'avg_double_fault_rate': 0,
            'avg_first_serve_rate': 0,
            'avg_first_serve_points_won': 0,
            'avg_second_serve_points_won': 0,
            'avg_break_points_saved': 0,
            'avg_dominance_ratio': 0,
            'avg_points_won_percent': 0,
            'avg_return_points_won_percent': 0,
            'avg_break_point_opprotunities_converted': 0
        }
        prefixes = [f'last_{i}_yr_' for i in x]
        full_stats = {}
        # adding the prefixes to the columns in last_x_game_data and adding them to full_stats
        for prefix in prefixes:
            for key in last_x_game_data:
                full_stats[prefix + key] = last_x_game_data[key]
        
        rank_steps = {}
        for prefix in prefixes:
            rank_steps[prefix + 'steps'] = []
        # convert the date to a pandas datetime object
        date = pd.to_datetime(date)

        if player_name not in self.json_data.keys():
            return full_stats

        player_data = self.json_data[player_name]
        # slice player data to only be games that happened before the current game
        player_data = [game for game in player_data if pd.to_datetime(game['Date']) < date]
        x_game_counts = x.copy()
        for i in range(len(x_game_counts)):
            x_game_counts[i] = 0
        for game in player_data:
            game_date = pd.to_datetime(game['Date'])
            # if the game is before the date of the current game
            if game_date < date:
                # if the game is within the last x[0] years
                if game_date >= date - pd.DateOffset(years=x[0]):
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[0]}_yr_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[0]}_yr_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[0]}_yr_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[0]}_yr_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[0]}_yr_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_yr_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                    x_game_counts[0] += 1
                elif len(x) == 1:
                    break
                # if the game is within the last x[1] years
                if len(x) > 1 and game_date >= date - pd.DateOffset(years=x[1]):
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[1]}_yr_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[1]}_yr_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[1]}_yr_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[1]}_yr_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[1]}_yr_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_yr_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                    x_game_counts[1] += 1
                elif len(x) == 2:
                    break
                # if the game is within the last x[2] years
                if len(x) > 2 and game_date >= date - pd.DateOffset(years=x[2]):
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[2]}_yr_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[2]}_yr_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[2]}_yr_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[2]}_yr_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[2]}_yr_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_yr_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                    x_game_counts[2] += 1
                elif len(x) > 2:
                    break

        # calculating the average rank step
        for key in rank_steps:
            if len(rank_steps[key]) > 0:
                # calculate the change in rank from the previous game to the current game
                rank_changes = [float(rank_steps[key][i]) - float(rank_steps[key][i + 1]) for i in range(len(rank_steps[key]) - 1)]
                # calculate the average rank change (positive is bad... means the player's rank is decreasing --> their actual rank number is getting larger)
                avg_rank_change = sum(rank_changes) / len(rank_changes) if len(rank_changes) > 0 else 0
                full_stats[key.replace('steps', 'avg_rank_step')] = avg_rank_change
        
        for i in range(len(x)):
            full_stats[f'last_{x[i]}_yr_avg_ace_rate'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_double_fault_rate'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_first_serve_rate'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_first_serve_points_won'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_second_serve_points_won'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_break_points_saved'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_points_won_percent'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_return_points_won_percent'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1
            full_stats[f'last_{x[i]}_yr_avg_break_point_opprotunities_converted'] /= x_game_counts[i] if x_game_counts[i] > 0 else 1

        return full_stats
    
    def getLastXGames(self, player, date, x):
        """
        Get the last x games for a player
        x: int[] number of games to get
        """
        last_x_game_data = {
            'wins': 0,
            'losses': 0,
            'hard_count': 0,
            'clay_count': 0,
            'grass_count': 0,
            'carpet_count': 0,
            'avg_rank_step': 0,
            'player_1_games': 0,
            'player_2_games': 0,
            'player_3_games': 0,
            'opponent_1_games': 0,
            'opponent_2_games': 0,
            'opponent_3_games': 0,
            'player_sets': 0,
            'opponent_sets': 0,
            'avg_ace_rate': 0,
            'avg_double_fault_rate': 0,
            'avg_first_serve_rate': 0,
            'avg_first_serve_points_won': 0,
            'avg_second_serve_points_won': 0,
            'avg_break_points_saved': 0,
            'avg_dominance_ratio': 0,
            'avg_points_won_percent': 0,
            'avg_return_points_won_percent': 0,
            'avg_break_point_opprotunities_converted': 0
        }
        prefixes = [f'last_{i}_' for i in x]
        full_stats = {}
        # adding the prefixes to the columns in last_x_game_data and adding them to full_stats
        for prefix in prefixes:
            for key in last_x_game_data:
                full_stats[prefix + key] = last_x_game_data[key]

        rank_steps = {}
        for prefix in prefixes:
            rank_steps[prefix + 'steps'] = []
        # convert the date to a pandas datetime object
        date = pd.to_datetime(date)

        if player not in self.json_data.keys():
            return full_stats
        
        player_data = self.json_data[player]
        # slice player data to only be games that happened before the current game
        player_data = [game for game in player_data if pd.to_datetime(game['Date']) < date]
        counter = 0
        for game in player_data:
            game_date = pd.to_datetime(game['Date'])
            # if the game is before the date of the current game
            if game_date < date and counter < x[-1]:

                # if the game is within the last x[0] years
                if counter < x[0]:
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[0]}_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[0]}_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[0]}_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[0]}_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[0]}_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[0]}_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[0]}_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[0]}_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[0]}_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[0]}_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                elif len(x) == 1:
                    break
                # if the game is within the last x[1] years
                if len(x) > 1 and counter < x[1]:
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[1]}_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[1]}_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[1]}_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[1]}_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[1]}_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[1]}_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[1]}_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[1]}_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[1]}_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[1]}_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                elif len(x) == 2:
                    break
                # if the game is within the last x[2] years
                if len(x) > 2 and counter < x[2]:
                    # updating full stats with the appropriate prefix
                    full_stats[f'last_{x[2]}_wins'] += 1 if game['Outcome'] == 'W' else 0
                    full_stats[f'last_{x[2]}_losses'] += 1 if game['Outcome'] == 'L' else 0
                    full_stats[f'last_{x[2]}_{game["Surface"].lower()}_count'] += 1 if game['Outcome'] == 'W' else 0
                    rank_steps[f'last_{x[2]}_steps'].append(game['ATP_Rank'])
                    full_stats[f'last_{x[2]}_player_1_games'] += float(game['w1']) if not game['w1'] == ' ' and not game['w1'] == '' else 0
                    full_stats[f'last_{x[2]}_player_2_games'] += float(game['w2']) if not game['w2'] == ' ' and not game['w2'] == '' else 0
                    full_stats[f'last_{x[2]}_player_3_games'] += float(game['w3']) if not game['w3'] == ' ' and not game['w3'] == '' else 0
                    full_stats[f'last_{x[2]}_player_sets'] += float(game['player_sets']) if not game['player_sets'] == ' ' and not game['player_sets'] == '' else 0
                    full_stats[f'last_{x[2]}_opponent_sets'] += float(game['opponent_sets']) if not game['opponent_sets'] == ' ' and not game['opponent_sets'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_ace_rate'] += float(game['Ace_Percent']) if not game['Ace_Percent'] == ' ' and not game['Ace_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_double_fault_rate'] += float(game['Double_Fault_Percent']) if not game['Double_Fault_Percent'] == ' ' and not game['Double_Fault_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_first_serve_rate'] += float(game['1st_In_Play_Rate']) if not game['1st_In_Play_Rate'] == ' ' and not game['1st_In_Play_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_first_serve_points_won'] += float(game['1st_Serve_Points_Won_Rate']) if not game['1st_Serve_Points_Won_Rate'] == ' ' and not game['1st_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_second_serve_points_won'] += float(game['2nd_Serve_Points_Won_Rate']) if not game['2nd_Serve_Points_Won_Rate'] == ' ' and not game['2nd_Serve_Points_Won_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_break_points_saved'] += float(game['Break_Point_Save_Rate']) if not game['Break_Point_Save_Rate'] == ' ' and not game['Break_Point_Save_Rate'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_points_won_percent'] += float(game['Total_Points_Won_Percent']) if not game['Total_Points_Won_Percent'] == ' ' and not game['Total_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_return_points_won_percent'] += float(game['Return_Points_Won_Percent']) if not game['Return_Points_Won_Percent'] == ' ' and not game['Return_Points_Won_Percent'] == '' else 0
                    full_stats[f'last_{x[2]}_avg_break_point_opprotunities_converted'] += float(game['Break_Point_Opportunities_Converted']) if not game['Break_Point_Opportunities_Converted'] == ' ' and not game['Break_Point_Opportunities_Converted'] == '' else 0
                elif len(x) > 2:
                    break
                counter += 1

        # calculating the average rank step
        for key in rank_steps:
            if len(rank_steps[key]) > 0:
                # calculate the change in rank from the previous game to the current game
                rank_changes = [float(rank_steps[key][i]) - float(rank_steps[key][i + 1]) for i in range(len(rank_steps[key]) - 1)]
                # calculate the average rank change (positive is bad... means the player's rank is decreasing --> their actual rank number is getting larger)
                avg_rank_change = sum(rank_changes) / len(rank_changes) if len(rank_changes) > 0 else 0
                full_stats[key.replace('steps', 'avg_rank_step')] = avg_rank_change
        
        for i in x:
            full_stats[f'last_{i}_avg_ace_rate'] /= i
            full_stats[f'last_{i}_avg_double_fault_rate'] /= i
            full_stats[f'last_{i}_avg_first_serve_rate'] /= i
            full_stats[f'last_{i}_avg_first_serve_points_won'] /= i
            full_stats[f'last_{i}_avg_second_serve_points_won'] /= i
            full_stats[f'last_{i}_avg_break_points_saved'] /= i
            full_stats[f'last_{i}_avg_points_won_percent'] /= i
            full_stats[f'last_{i}_avg_return_points_won_percent'] /= i
            full_stats[f'last_{i}_avg_break_point_opprotunities_converted'] /= i

        return full_stats

    def process_data(self):
        print(f'processing data from {self.data.shape[0]} matches...')

        counter = 0
        messed_up_counter = 0
        processed_data = []
        for _, row in self.data.iterrows():
            if row['Player_Name'] not in self.json_data.keys() or row['Opponent_Name'] not in self.json_data.keys():
                continue

            # if the match between the two players has already been processed by checking the processed_data list
            # Use a set for efficient O(1) average time complexity lookups.
            # The set is attached to the class instance to persist across iterations.
            if not hasattr(self, '_processed_matches_set'):
                self._processed_matches_set = set()

            player1 = row['Player_Name']
            player2 = row['Opponent_Name']
            date = row['Date']

            # Create a canonical representation for the match by sorting player names.
            # This ensures that (player1, player2) and (player2, player1) are treated as the same match.
            match_key = tuple(sorted((player1, player2))) + (date,)

            if match_key in self._processed_matches_set:
                continue
            
            self._processed_matches_set.add(match_key)

            tournament = row['Tournament']
            winner = row['Player_Name'] if row['Outcome'] == 'W' else row['Opponent_Name']
            loser = row['Opponent_Name'] if row['Outcome'] == 'W' else row['Player_Name']
            date = row['Date']
            surface = self.surface_map[row['Surface']]
            round_count = self.round_map[row['Round']]
            best_of = self.best_of_map[row['Best_of']]
            winner_rank = row['ATP_Rank'] if row['Outcome'] == 'W' else row['Opponent_Rank']
            loser_rank = row['Opponent_Rank'] if row['Outcome'] == 'W' else row['ATP_Rank']
            if row['Dominant_Hand'] in self.hand_map.keys() and row['Opponent_Hand'] in self.hand_map.keys():
                winner_hand = self.hand_map[row['Dominant_Hand']] if row['Outcome'] == 'W' else self.hand_map[row['Opponent_Hand']]
                loser_hand = self.hand_map[row['Opponent_Hand']] if row['Outcome'] == 'W' else self.hand_map[row['Dominant_Hand']]
            else:
                winner_hand = None
                loser_hand = None
            winner_dob = row['DOB'] if row['Outcome'] == 'W' else row['Opponent_DOB']
            loser_dob = row['Opponent_DOB'] if row['Outcome'] == 'W' else row['DOB']
            winner_height = row['Height'] if row['Outcome'] == 'W' else row['Opponent_Height']
            loser_height = row['Opponent_Height'] if row['Outcome'] == 'W' else row['Height']
            winner_country = row['Country'] if row['Outcome'] == 'W' else row['Opponent_Nationality']
            loser_country = row['Opponent_Nationality'] if row['Outcome'] == 'W' else row['Country']

            winner_total_games, loser_total_games = 0, 0
            if row['Outcome'] == 'W':
                winner_total_games += float(row['w1']) if not row['w1'] == ' ' and not row['w1'] == '' else 0
                winner_total_games += float(row['w2']) if not row['w2'] == ' ' and not row['w2'] == '' else 0
                winner_total_games += float(row['w3']) if not row['w3'] == ' ' and not row['w3'] == '' else 0
                winner_total_games += float(row['w4']) if not row['w4'] == ' ' and not row['w4'] == '' else 0
                winner_total_games += float(row['w5']) if not row['w5'] == ' ' and not row['w5'] == '' else 0
                loser_total_games += float(row['l1']) if not row['l1'] == ' ' and not row['l1'] == '' else 0
                loser_total_games += float(row['l2']) if not row['l2'] == ' ' and not row['l2'] == '' else 0
                loser_total_games += float(row['l3']) if not row['l3'] == ' ' and not row['l3'] == '' else 0
                loser_total_games += float(row['l4']) if not row['l4'] == ' ' and not row['l4'] == '' else 0
                loser_total_games += float(row['l5']) if not row['l5'] == ' ' and not row['l5'] == '' else 0
            else:
                winner_total_games += float(row['l1']) if not row['l1'] == ' ' and not row['l1'] == '' else 0
                winner_total_games += float(row['l2']) if not row['l2'] == ' ' and not row['l2'] == '' else 0
                winner_total_games += float(row['l3']) if not row['l3'] == ' ' and not row['l3'] == '' else 0
                winner_total_games += float(row['l4']) if not row['l4'] == ' ' and not row['l4'] == '' else 0
                winner_total_games += float(row['l5']) if not row['l5'] == ' ' and not row['l5'] == '' else 0
                loser_total_games += float(row['w1']) if not row['w1'] == ' ' and not row['w1'] == '' else 0
                loser_total_games += float(row['w2']) if not row['w2'] == ' ' and not row['w2'] == '' else 0
                loser_total_games += float(row['w3']) if not row['w3'] == ' ' and not row['w3'] == '' else 0
                loser_total_games += float(row['w4']) if not row['w4'] == ' ' and not row['w4'] == '' else 0
                loser_total_games += float(row['w5']) if not row['w5'] == ' ' and not row['w5'] == '' else 0

            winner_age = self.calculateAge(winner_dob, row['Date'])
            loser_age = self.calculateAge(loser_dob, row['Date'])

            # get the game stats in the last 1 year
            winner_time_data = self.getGamesInLastXYears(winner, row['Date'], [1])
            loser_time_data = self.getGamesInLastXYears(loser, row['Date'], [1])
            # get the last 10, 5, 1 game stats
            winner_game_data = self.getLastXGames(winner, row['Date'], [1, 5, 25])
            loser_game_data = self.getLastXGames(loser, row['Date'], [1, 5, 25])

            if counter % 2 == 0:
                # adding the prefix p_ to all the keys in winner_time_data, winner_game_data, and winner_composite_stats
                player_time_data = {f'p_{key}': value for key, value in winner_time_data.items()}
                player_game_data = {f'p_{key}': value for key, value in winner_game_data.items()}
                # adding the prefix o_ to all the keys in loser_time_data, loser_game_data, and loser_composite_stats
                opponent_time_data = {f'o_{key}': value for key, value in loser_time_data.items()}
                opponent_game_data = {f'o_{key}': value for key, value in loser_game_data.items()}
                game_stats = {
                    'tournament': tournament,
                    'date': date,
                    'player': winner,
                    'opponent': loser,
                    'result': 1,
                    'player_total_games_won': winner_total_games,
                    'opponent_total_games_won': loser_total_games,
                    'surface': surface,
                    'round_count': round_count,
                    'best_of': best_of,
                    'player_rank': winner_rank,
                    'opponent_rank': loser_rank,
                    'player_hand': winner_hand,
                    'opponent_hand': loser_hand,
                    'player_age': winner_age,
                    'opponent_age': loser_age,
                    'player_height': winner_height,
                    'opponent_height': loser_height,
                    'player_country': winner_country,
                    'opponent_country': loser_country,
                    **player_time_data,
                    **opponent_time_data,
                    **player_game_data,
                    **opponent_game_data
                }
            else:
                # adding the prefix p_ to all the keys in winner_time_data, winner_game_data, and winner_composite_stats
                player_time_data = {f'p_{key}': value for key, value in loser_time_data.items()}
                player_game_data = {f'p_{key}': value for key, value in loser_game_data.items()}
                # adding the prefix o_ to all the keys in loser_time_data, loser_game_data, and loser_composite_stats
                opponent_time_data = {f'o_{key}': value for key, value in winner_time_data.items()}
                opponent_game_data = {f'o_{key}': value for key, value in winner_game_data.items()}
                game_stats = {
                    'tournament': tournament,
                    'date': date,
                    'player': loser,
                    'opponent': winner,
                    'result': 0,
                    'player_total_games_won': loser_total_games,
                    'opponent_total_games_won': winner_total_games,
                    'surface': surface,
                    'round_count': round_count,
                    'best_of': best_of,
                    'player_rank': loser_rank,
                    'opponent_rank': winner_rank,
                    'player_hand': loser_hand,
                    'opponent_hand': winner_hand,
                    'player_age': loser_age,
                    'opponent_age': winner_age,
                    'player_height': loser_height,
                    'opponent_height': winner_height,
                    'player_country': loser_country,
                    'opponent_country': winner_country,
                    **player_time_data,
                    **opponent_time_data,
                    **player_game_data,
                    **opponent_game_data
                }
            processed_data.append(game_stats)
            counter += 1

            if game_stats['p_last_1_wins'] == 0 and game_stats['p_last_1_losses'] == 0 and game_stats['o_last_1_wins'] == 0 and game_stats['o_last_1_losses'] == 0:
                print(game_stats['player'], game_stats['opponent'], game_stats['date'])
                messed_up_counter += 1

            if counter % 1000 == 0 and counter > 999:
                print(f'processed {counter / self.data.shape[0] * 100:.2f}% games...')
        
        print(f'processed {counter} games...')
        print(f'messed up {messed_up_counter} times...')
        return processed_data

def convert_odds_to_american(odds):
    if odds >= 2:
        return math.floor((odds - 1) * 100)
    else:
        return math.floor(-100 / (odds - 1))
    
def add_moneyline_odds_to_processed_data(processed_data, upcoming=True):
    # adding the odds to the processed data
    if upcoming:
        odds_data = pd.read_csv('data/upcoming_tennis_odds.csv')
    else:
        odds_data = pd.read_csv('data/tennis_odds.csv')
    
    # remove any rows where the market_name is not Moneyline
    odds_data = odds_data[odds_data['market_name'] == 'Moneyline']
    
    count = 0
    for index, row in processed_data.iterrows():
        # find the row in odds_data where the player and opponent are the same as the row in processed_data
        player_match = (odds_data['player1_name'] == row['player']) & (odds_data['player2_name'] == row['opponent'])
        opponent_match = (odds_data['player1_name'] == row['opponent']) & (odds_data['player2_name'] == row['player'])
        odds_row = odds_data[player_match | opponent_match]

        if odds_row.empty:
            # remove the row from processed_data
            processed_data = processed_data.drop(index)
            continue

        # get the year of the tournament from the processed_data
        date = pd.to_datetime(row['date'], utc=True)
        year = date.year

        # filter out odds_row where the year is not the same as the year in the processed_data
        odds_row = odds_row[odds_row['start_date'].str.split('-').str[0] == str(year)]

        if odds_row.empty:
            # remove the row from processed_data
            processed_data = processed_data.drop(index)
            continue

        # get the name of the tournament from the processed_data
        tournament = row['tournament']

        # filter out player_odds_row and opponent_odds_row where the tournament is not the same as the tournament in the processed_data
        odds_row = odds_row[
            (odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in odds_row['tournament_name'])
        ]

        if odds_row.empty:
            # remove the row from processed_data
            processed_data = processed_data.drop(index)
            continue

        count += 1
        # convert the odds and add them to the row in processed_data
        if odds_row['player1_name'].values[0] == row['player']:
            processed_data.loc[index, 'player_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
            processed_data.loc[index, 'opponent_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
        else:
            processed_data.loc[index, 'player_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
            processed_data.loc[index, 'opponent_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
        
        # replace the match date in the processed_data with the match date in the odds_data
        processed_data.loc[index, 'date'] = standardize_date(odds_row['start_date'].values[0])

    print(f'Found moneyline odds: {count}')
    return processed_data

def standardize_date(date):
    """
    Takes in dates in any format and returns them as a pd.Timestamp object with UTC timezone.
    Tries common date formats and falls back to pandas' parser.
    Returns None if parsing fails.
    """
    import pandas as pd

    if pd.isna(date):
        return None

    # Try common date formats
    date_formats = ['%m-%d-%Y', '%Y-%m-%d', '%d-%Y-%m']
    for fmt in date_formats:
        try:
            # create timezone aware timestamp in UTC
            return pd.to_datetime(date, format=fmt, utc=True)
        except Exception:
            continue
    # Try pandas default parser as a last resort
    try:
        dt = pd.to_datetime(date, errors='raise')
        if dt.tzinfo is None:
            # if naive, assume UTC and convert
            return dt.tz_localize('UTC')
        else:
            # if aware, just convert
            return dt.tz_convert('UTC')
    except Exception:
        return None

def add_total_games_odds_to_processed_data(processed_data, upcoming=True):
    # adding the total games odds to the processed data
    if upcoming:
        tennis_odds = pd.read_csv('data/upcoming_tennis_odds.csv')
    else:
        tennis_odds = pd.read_csv('data/tennis_odds.csv')

    # remove any rows where the market_name is not Total Games
    tennis_odds = tennis_odds[tennis_odds['market_name'].str.contains('Player Total Games Won', na=False)]
    
    count = 0
    for index, row in processed_data.iterrows():
        # find the row in odds_data where the player and opponent are the same as the row in processed_data
        player_match = tennis_odds['event_name'].apply(
            lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0] == row['player'] and x.split(' vs ')[1] == row['opponent']
        )
        opponent_match = tennis_odds['event_name'].apply(
            lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0] == row['opponent'] and x.split(' vs ')[1] == row['player']
        )
        odds_row = tennis_odds[player_match | opponent_match]

        if odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue

        # find the odds for the player total games won market
        player_odds_row = odds_row[odds_row['market_name'].str.contains(row['player'])]
        opponent_odds_row = odds_row[odds_row['market_name'].str.contains(row['opponent'])]

        if player_odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue
        
        if opponent_odds_row.empty:
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            continue

        # if there is more than one line for the player or opponent, then we need to find the line with the smallest difference between the player1_odds and player2_odds
        player_odds_row = player_odds_row.loc[
            [(player_odds_row['player1_odds'] - player_odds_row['player2_odds']).abs().idxmin()]
        ].reset_index(drop=True)

        opponent_odds_row = opponent_odds_row.loc[
            [(opponent_odds_row['player1_odds'] - opponent_odds_row['player2_odds']).abs().idxmin()]
        ].reset_index(drop=True)

        # get the year of the tournament from the processed_data
        date = pd.to_datetime(row['date'], utc=True)
        year = date.year

        # filter out player_odds_row and opponent_odds_row where the year is not the same as the year in the processed_data
        player_odds_row = player_odds_row[player_odds_row['start_date'].str.split('-').str[0] == str(year)]
        opponent_odds_row = opponent_odds_row[opponent_odds_row['start_date'].str.split('-').str[0] == str(year)]

        if player_odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue
        
        if opponent_odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue

        # get the name of the tournament from the processed_data
        tournament = row['tournament']

        print('--------------------------------')
        print(f'{row["player"]} vs {row["opponent"]} {row["date"]} {tournament} | {player_odds_row["tournament_name"].values[0].split(" - ")[-1]} | {opponent_odds_row["tournament_name"].values[0].split(" - ")[-1]}')

        # filter out player_odds_row and opponent_odds_row where the tournament is not the same as the tournament in the processed_data
        player_odds_row = player_odds_row[
            (player_odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in player_odds_row['tournament_name'])
        ]
        opponent_odds_row = opponent_odds_row[
            (opponent_odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in opponent_odds_row['tournament_name'])
        ]

        # TODO: and the year and tournament are the same --> for now just matching on year and month
        if player_odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue

        if opponent_odds_row.empty:
            processed_data.loc[index, 'player_total_games_odds_over'] = 0
            processed_data.loc[index, 'player_total_games_odds_under'] = 0
            processed_data.loc[index, 'player_total_games_odds_line'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_over'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_under'] = 0
            processed_data.loc[index, 'opponent_total_games_odds_line'] = 0
            continue
        
        print(f'{row["player"]} Total Games: {player_odds_row["player1_name"].values[0]} ({convert_odds_to_american(player_odds_row["player1_odds"].values[0])}) vs {player_odds_row["player2_name"].values[0]} ({convert_odds_to_american(player_odds_row["player2_odds"].values[0])}) - {player_odds_row["player1_points"].values[0]}')
        print(f'{row["opponent"]} Total Games: {opponent_odds_row["player1_name"].values[0]} ({convert_odds_to_american(opponent_odds_row["player1_odds"].values[0])}) vs {opponent_odds_row["player2_name"].values[0]} ({convert_odds_to_american(opponent_odds_row["player2_odds"].values[0])}) - {opponent_odds_row["player1_points"].values[0]}')

        # convert the odds and add them to the row in processed_data
        processed_data.loc[index, 'player_total_games_odds_over'] = convert_odds_to_american(player_odds_row['player1_odds'].values[0])
        processed_data.loc[index, 'player_total_games_odds_under'] = convert_odds_to_american(player_odds_row['player2_odds'].values[0])
        processed_data.loc[index, 'player_total_games_odds_line'] = player_odds_row['player1_points'].values[0]
        processed_data.loc[index, 'opponent_total_games_odds_over'] = convert_odds_to_american(opponent_odds_row['player1_odds'].values[0])
        processed_data.loc[index, 'opponent_total_games_odds_under'] = convert_odds_to_american(opponent_odds_row['player2_odds'].values[0])
        processed_data.loc[index, 'opponent_total_games_odds_line'] = opponent_odds_row['player1_points'].values[0]
        
        # replace the match date in the processed_data with the match date in the odds_data
        # processed_data.loc[index, 'date'] = odds_row['start_date'].values[0]

        count += 1
    
    print(f'Found rounds odds: {count}')

    return processed_data

if __name__ == "__main__":
    # load the data
    if len(sys.argv) > 1 and sys.argv[1] == 'new':
        data = pd.read_csv('data/atp_new_matches.csv')
    else:
        data = pd.read_csv('data/atp_player_match_data.csv')
    processor = Processor(data)
    start_time = time.time()
    processed_data = pd.DataFrame(processor.process_data())
    # order by date
    processed_data = processed_data.sort_values(by='date', ascending=True)
    # drop duplicates
    processed_data = processed_data.drop_duplicates()
    # drop rows where the player is the same as the opponent
    processed_data = processed_data[processed_data['player'] != processed_data['opponent']]

    # save the processed data to a csv file
    if len(sys.argv) > 1 and sys.argv[1] == 'new':
        processed_data.to_csv('data/processed_data_upcoming.csv', index=False)

        # adding the odds to the processed data
        processed_data = add_moneyline_odds_to_processed_data(processed_data)
        # processed_data = add_total_games_odds_to_processed_data(processed_data)
        
        # save the processed data to a csv file
        processed_data.to_csv('data/processed_data_upcoming.csv', index=False)
    else:
        processed_data.to_csv('data/processed_data.csv', index=False)

        processed_data = add_moneyline_odds_to_processed_data(processed_data, upcoming=False)
        processed_data = add_total_games_odds_to_processed_data(processed_data, upcoming=False)
        
        processed_data.to_csv('data/processed_data.csv', index=False)

        # get all the rows where the player_total_games_odds_line isnt 0 and the opponent_total_games_odds_line isnt 0
        total_games_processed = processed_data[
            (processed_data['player_total_games_odds_line'] != 0) & 
            (processed_data['opponent_total_games_odds_line'] != 0)
        ]
        
        # write the total_games_processed to a csv file
        total_games_processed.to_csv('data/total_games_processed.csv', index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(f'done processing data, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')