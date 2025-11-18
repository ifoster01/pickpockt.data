import pandas as pd
import time
from datetime import timedelta
from typing import Dict, Any
from pathlib import Path
from functions.general import *
import pytz
et_tz = pytz.timezone('US/Eastern')
utc_tz = pytz.UTC

class Preprocessor:
    def __init__(self, game_data) -> None:
        print('initializing preprocessor...')
        self.game_data = self.clean_data(game_data)
        self.processed_data = None
        # Process and convert dates to timezone-aware UTC format
        self._process_dates()
        self.json_data = self.convert_to_json()
        # Pre-compute stat keys for faster lookups
        self._init_stat_keys()
        
        self.team_dict = {
            'crd': ['Arizona Cardinals'],
            'atl': ['Atlanta Falcons'],
            'rav': ['Baltimore Ravens'],
            'buf': ['Buffalo Bills'],
            'car': ['Carolina Panthers'],
            'chi': ['Chicago Bears'],
            'cin': ['Cincinnati Bengals'],
            'cle': ['Cleveland Browns'],
            'dal': ['Dallas Cowboys'],
            'den': ['Denver Broncos'],
            'det': ['Detroit Lions'],
            'gnb': ['Green Bay Packers'],
            'htx': ['Houston Texans'],
            'clt': ['Indianapolis Colts'],
            'jax': ['Jacksonville Jaguars'],
            'kan': ['Kansas City Chiefs'],
            'rai': ['Las Vegas Raiders', 'Oakland Raiders'],
            'sdg': ['Los Angeles Chargers', 'San Diego Chargers'],
            'ram': ['Los Angeles Rams', 'St. Louis Rams'],
            'mia': ['Miami Dolphins'],
            'min': ['Minnesota Vikings'],
            'nwe': ['New England Patriots'],
            'nor': ['New Orleans Saints'],
            'nyg': ['New York Giants'],
            'nyj': ['New York Jets'],
            'phi': ['Philadelphia Eagles'],
            'pit': ['Pittsburgh Steelers'],
            'sea': ['Seattle Seahawks'],
            'sfo': ['San Francisco 49ers'],
            'tam': ['Tampa Bay Buccaneers'],
            'oti': ['Tennessee Titans', 'Tennessee Oilers', 'Houston Oilers'],
            'was': ['Washington Commanders', 'Washington Football Team', 'Washington Redskins']
        }
    
    def clean_data(self, data):
        """Clean and prepare the data for processing"""
        data = data.copy()

        # Remove rows where opponent is same as team (but allow opponents that don't appear as a team in this slice)
        if 'opponent' in data.columns and 'team' in data.columns:
            data = data[data['opponent'] != data['team']]

        # Robustly convert home to 0/1
        if 'home' in data.columns:
            with pd.option_context('mode.chained_assignment', None):
                data['home'] = data['home'].apply(
                    lambda v: 1 if (isinstance(v, bool) and v) or str(v).strip().lower() in {'1', 'true', 't', 'yes', 'y'} else 0
                )

        # Robustly convert win to 0/1
        if 'win' in data.columns:
            def _to_result(value: Any) -> int:
                if isinstance(value, (int, float)) and not pd.isna(value):
                    return 1 if float(value) > 0 else 0
                s = str(value).strip().lower()
                if s in {'1', 'w', 'win', 'true', 't', 'yes', 'y'}:
                    return 1
                if s in {'0', 'l', 'loss', 'false', 'f', 'no', 'n'}:
                    return 0
                return 1 if s.startswith('w') else 0
            with pd.option_context('mode.chained_assignment', None):
                data['win'] = data['win'].apply(_to_result)

        # Ensure year column exists (derive from date when possible)
        if 'year' not in data.columns:
            with pd.option_context('mode.chained_assignment', None):
                data['year'] = pd.to_datetime(data['date'], errors='coerce').dt.year.fillna(0).astype(int)
        
        return data
    
    def _process_dates(self):
        """
        Process and convert game dates to timezone-aware UTC format.
        Dates after August 1, 2025 are converted from ET to UTC,
        while earlier dates are directly localized to UTC.
        """
        # Pre-compute date conversions and convert ET to UTC
        self.game_data['formatted_date'] = pd.to_datetime(self.game_data.apply(
            lambda row: self.formatDate(row['date'], row['time']), axis=1
        ))

        # Convert from ET to UTC only for dates after August 1, 2025
        cutoff_date = pd.Timestamp('2025-08-01')  # Keep timezone-naive for comparison

        # Create a new Series to hold timezone-aware datetimes
        timezone_aware_dates = pd.Series(index=self.game_data.index, dtype='datetime64[ns, UTC]')
        
        # First, identify dates that need ET->UTC conversion (after cutoff)
        et_conversion_mask = self.game_data['formatted_date'] > cutoff_date
        
        # Convert dates after cutoff from ET to UTC
        timezone_aware_dates.loc[et_conversion_mask] = (
            self.game_data.loc[et_conversion_mask, 'formatted_date']
            .dt.tz_localize(et_tz)
            .dt.tz_convert(utc_tz)
        )
        
        # Convert dates before/equal to cutoff directly to UTC
        pre_cutoff_mask = ~et_conversion_mask
        timezone_aware_dates.loc[pre_cutoff_mask] = (
            self.game_data.loc[pre_cutoff_mask, 'formatted_date'].dt.tz_localize(utc_tz)
        )
        
        # Replace the original column with the timezone-aware version
        self.game_data['formatted_date'] = timezone_aware_dates
    
    def _init_stat_keys(self):
        """Pre-compute stat keys used in processing - mapped to actual NFL CSV columns"""
        self.base_stats = {
            'wins': 0,
            'losses': 0, 
            'points': 0,
            'opponent_points': 0,
            'first_downs_off': 0,
            'total_yards_off': 0,
            'pass_yards_off': 0,
            'rush_yards_off': 0,
            'turnovers_off': 0,
            'first_downs_def': 0,
            'total_yards_def': 0,
            'pass_yards_def': 0,
            'rush_yards_def': 0,
            'turnovers_def': 0,
            'team_strong_drives': 0,
            'opp_strong_drives': 0,
        }
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float, return 0 if invalid"""
        if isinstance(value, (int, float)):
            return float(value)
        if not value or value == ' ' or pd.isna(value):
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def formatDate(self, date: str, time: str):
        """
        Converts the date to a pandas datetime object
        date: str in the format 1995-09-03
        time: str in the format 4:00PM ET
        """
        try:
            # Handle date format from CSV (YYYY-MM-DD)
            if '-' in date and len(date.split('-')) == 3:
                # Extract time info
                if time and time.strip():
                    time_part = time.split(' ')[0]
                    if len(time_part) > 2:
                        clock_time = time_part[:-2]
                        ampm = time_part[-2:]
                        return pd.to_datetime(f'{date} {clock_time} {ampm}')
                    else:
                        return pd.to_datetime(f'{date} 12:00 PM')
                else:
                    return pd.to_datetime(date)
        except Exception as e:
            print(f"Error parsing date {date} {time}: {e}")
            return pd.to_datetime('1995-01-01')  # Fallback date

    def convert_to_json(self):
        """Convert game data to JSON format for efficient lookups - NBA style"""
        print('converting to json...')
        json_conversion_start_time = time.time()
        
        # Group by team and sort by date in one operation
        json_data = {}
        for team in self.game_data['team'].unique():
            team_data = self.game_data[self.game_data['team'] == team].sort_values('formatted_date', ascending=False)
            json_data[team] = team_data.to_dict('records')
        
        elapsed_time = time.time() - json_conversion_start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f'done converting to json, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
        
        return json_data

    def _process_game_stats(self, game: Dict, stats: Dict, prefix: str):
        """Process a single game's statistics with safe key access - NBA style"""
        for key in self.base_stats.keys():
            if key == 'wins':
                stats[f'{prefix}{key}'] += 1 if game.get('win') == 1 else 0
            elif key == 'losses':
                stats[f'{prefix}{key}'] += 1 if game.get('win') == 0 else 0
            else:
                stats[f'{prefix}{key}'] += self._safe_float(game.get(key, 0))
        return stats

    def getLastXGameStats(self, team: str, date, x: int, prefix: str):
        """
        Optimized version - similar to NBA getLastXGames
        Returns the stats of the last x games played by a team
        """
        full_stats = {f'{prefix}{k}': v for k, v in self.base_stats.items()}

        if team not in self.json_data:
            return full_stats

        team_data = self.json_data[team]
        counter = 0
        
        for game in team_data:
            game_date = pd.to_datetime(game['formatted_date'])
            if game_date >= date:
                continue
                
            if counter >= x:
                break
                
            self._process_game_stats(game, full_stats, prefix)
            counter += 1

        # Average stats by number of games found
        divisor = counter if counter > 0 else 1
        for key in full_stats:
            full_stats[key] /= divisor

        return full_stats

    def getIndexName(self, team: str):
        """
        Returns the abbreviation of the team given the team name
        team: str --> team name
        """
        for key, value in self.team_dict.items():
            if team in value:
                return key
        return None
    
    def preprocess(self):
        """Main preprocessing function - optimized similar to NBA version"""
        print('starting preprocessing...')
        
        # Update the opponent column to use abbreviations
        self.game_data['opponent'] = self.game_data['opponent'].apply(lambda x: self.getIndexName(x) if x not in self.team_dict else x)

        print('formatted data, now processing...')
        processed_start_time = pd.Timestamp.now()
        print(f'started processing at {processed_start_time.day_name()} {processed_start_time.time()}')
        
        data = []
        for idx, row in self.game_data.iterrows():
            try:
                # get the date of the game
                formatted_date = row['formatted_date']

                # skip the game if it is one of the first 3 games of the season
                if int(row['week']) <= 3:
                    continue

                # get the teams
                team = row['team']
                opponent = row['opponent']
                
                if not team or not opponent:
                    continue
                
                # Sportsbook-style: team_spread = opponent_points - team_points
                team_spread = self._safe_float(row.get('opponent_points', 0)) - self._safe_float(row.get('points', 0))
                opp_spread = self._safe_float(row.get('points', 0)) - self._safe_float(row.get('opponent_points', 0))

                game_total = self._safe_float(row.get('points', 0)) + self._safe_float(row.get('opponent_points', 0))

                # get the last x games stats
                home_team_last_3_games = self.getLastXGameStats(team, formatted_date, 3, 'last_3_team_')
                away_team_last_3_games = self.getLastXGameStats(opponent, formatted_date, 3, 'last_3_opp_')
                home_team_last_game = self.getLastXGameStats(team, formatted_date, 1, 'last_1_team_')
                away_team_last_game = self.getLastXGameStats(opponent, formatted_date, 1, 'last_1_opp_')

                # create a dictionary of the data
                data_row = {
                    'week': row.get('week', 0),
                    'team': team,
                    'opp': opponent,
                    'date': formatted_date,
                    'result': row.get('win', 0),
                    'location': row.get('home', 0),
                    'team_spread': team_spread,
                    'opp_spread': opp_spread,
                    'game_total': game_total,
                    'points': self._safe_float(row.get('points', 0)),
                    'opponent_points': self._safe_float(row.get('opponent_points', 0)),
                    **home_team_last_3_games,
                    **away_team_last_3_games,
                    **home_team_last_game,
                    **away_team_last_game,
                }

                # append the data to the data list
                data.append(data_row)

            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                continue
        
        # convert the data to a DataFrame
        data = pd.DataFrame(data)

        print(f'finished processing at {pd.Timestamp.now().day_name()} {pd.Timestamp.now().time()}')
        print(f'took {pd.Timestamp.now() - processed_start_time} to process')
        
        self.processed_data = data

    def balance(self):
        """
        Balances the data by removing games until there is an equal number of wins and losses
        """
        if self.processed_data is None:
            print("No processed data to balance")
            return

        # Create a copy of the processed data
        grouped_data = self.processed_data.copy()

        # Create a team_pair column where team and opp are alphabetically sorted
        grouped_data['team_pair'] = grouped_data.apply(
            lambda row: '-'.join(sorted([row['team'], row['opp']])), axis=1
        )

        # Sort by team_pair and date to group matching games together
        grouped_data = grouped_data.sort_values(by=['team_pair', 'date'], ascending=False)

        # Create two empty DataFrames: one for each match of the pair
        dataframe_one = pd.DataFrame()
        dataframe_two = pd.DataFrame()

        # Iterate over the grouped data based on the team_pair
        for _, group in grouped_data.groupby(['team_pair', 'date']):
            # If there are two matches for the pair (home and away), split them
            if len(group) == 2:
                dataframe_one = pd.concat([dataframe_one, group.iloc[[0]]])
                dataframe_two = pd.concat([dataframe_two, group.iloc[[1]]])
            else:
                # If there is only one match for the pair, just add it to dataframe_one
                dataframe_one = pd.concat([dataframe_one, group])

        # Reset index for both dataframes
        dataframe_one.reset_index(drop=True, inplace=True)
        dataframe_two.reset_index(drop=True, inplace=True)

        wins, losses = 0, 0
        balanced_data = []
        for index, row in dataframe_one.iterrows():
            if wins <= losses and row['result'] == 1:  # Changed from 'W' to 1
                balanced_data.append(row)
                wins += 1
            elif wins <= losses and row['result'] == 0:  # Changed from 'L' to 0
                if index < len(dataframe_two):
                    balanced_data.append(dataframe_two.iloc[index])
                wins += 1
            elif wins > losses and row['result'] == 1:  # Changed from 'W' to 1
                if index < len(dataframe_two):
                    balanced_data.append(dataframe_two.iloc[index])
                losses += 1
            elif wins > losses and row['result'] == 0:  # Changed from 'L' to 0
                balanced_data.append(row)
                losses += 1
            else:
                balanced_data.append(row)
        
        balanced_data = pd.DataFrame(balanced_data)
        balanced_data = balanced_data.drop(columns=['team_pair'])

        # printing the number of wins and losses in the balanced data
        print(balanced_data['result'].value_counts())

        # resort the data by date in descending order
        balanced_data = balanced_data.sort_values(by='date', ascending=False)
        # drop all rows where the last_1_team_points or the last_1_opp_points is nan
        balanced_data = balanced_data[balanced_data['last_1_team_points'].notna()]
        balanced_data = balanced_data[balanced_data['last_1_opp_points'].notna()]

        self.processed_data = balanced_data
    
    def add_moneyline_odds(self):
        # adding the odds to the processed data
        odds_data = pd.read_csv('data/nfl_odds.csv')

        # remove any rows where the market_name is not Moneyline
        odds_data = odds_data[odds_data['market_name'] == 'Moneyline']
        # convert the start_date to ISO date string (UTC)
        odds_data['start_date'] = pd.to_datetime(
            odds_data['start_date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        processed_moneyline_data = self.processed_data.copy()
        
        # do the same for the processed_data
        processed_moneyline_data['datetime'] = processed_moneyline_data['date']
        processed_moneyline_data['date'] = pd.to_datetime(
            processed_moneyline_data['date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        count = 0
        for index, row in processed_moneyline_data.iterrows():
            # find the row in odds_data where the player and opponent are the same as the row in processed_data
            player_match = (odds_data['player1_name'] == get_dk_name_from_team(row['team'])) & (odds_data['player2_name'] == get_dk_name_from_team(row['opp']))
            opponent_match = (odds_data['player1_name'] == get_dk_name_from_team(row['opp'])) & (odds_data['player2_name'] == get_dk_name_from_team(row['team']))
            odds_row = odds_data[player_match | opponent_match]

            if odds_row.empty:
                continue

            # date values are already normalized to 'YYYY-MM-DD' strings above
            date = row['date']

            # filter for matching event date
            odds_row = odds_row[odds_row['start_date'] == date]

            if odds_row.empty:
                continue

            if len(odds_row) > 1:
                print(f'Multiple odds rows found for {row["team"]} vs {row["opp"]} on {date}')
                continue

            count += 1
            # convert the odds and add them to the row in processed_data
            if odds_row['player1_name'].values[0] == get_dk_name_from_team(row['team']):
                processed_moneyline_data.loc[index, 'player_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
                processed_moneyline_data.loc[index, 'opponent_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
            else:
                processed_moneyline_data.loc[index, 'player_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
                processed_moneyline_data.loc[index, 'opponent_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])

        print(f'Found moneyline odds: {count}')
        processed_moneyline_data['date'] = processed_moneyline_data['datetime']
        processed_moneyline_data = processed_moneyline_data.drop(columns=['datetime'])

        return processed_moneyline_data
    
    def add_spread_odds(self):
        # add the moneyline odds to the processed data
        processed_data = self.add_moneyline_odds()

        # adding the odds to the processed data
        odds_data = pd.read_csv('data/nfl_odds.csv')

        # remove any rows where the market_name is not Spread
        odds_data = odds_data[odds_data['market_name'] == 'Spread']
        # convert the start_date to ISO date string (UTC)
        odds_data['start_date'] = pd.to_datetime(
            odds_data['start_date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        processed_spread_data = processed_data.copy()

        # do the same for the processed_data
        processed_spread_data['date'] = pd.to_datetime(
            processed_spread_data['date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        count = 0
        for index, row in processed_spread_data.iterrows():
            # find the row in odds_data where the player and opponent are the same as the row in processed_data
            player_match = (odds_data['player1_name'] == get_dk_name_from_team(row['team'])) & (odds_data['player2_name'] == get_dk_name_from_team(row['opp']))
            opponent_match = (odds_data['player1_name'] == get_dk_name_from_team(row['opp'])) & (odds_data['player2_name'] == get_dk_name_from_team(row['team']))
            odds_row = odds_data[player_match | opponent_match]

            if odds_row.empty:
                continue

            # date values are already normalized to 'YYYY-MM-DD' strings above
            date = row['date']

            # filter for matching event date
            odds_row = odds_row[odds_row['start_date'] == date]

            if odds_row.empty:
                continue

            if len(odds_row) > 1:
                print(f'Multiple odds rows found for {row["team"]} vs {row["opp"]} on {date}')
                continue

            count += 1
            # convert the odds and add them to the row in processed_data
            if odds_row['player1_name'].values[0] == get_dk_name_from_team(row['team']):
                processed_spread_data.loc[index, 'player_spread_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
                processed_spread_data.loc[index, 'opponent_spread_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
                processed_spread_data.loc[index, 'team_spread_line'] = odds_row['player1_points'].values[0]
                processed_spread_data.loc[index, 'opp_spread_line'] = odds_row['player2_points'].values[0]
            else:
                processed_spread_data.loc[index, 'player_spread_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
                processed_spread_data.loc[index, 'opponent_spread_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
                processed_spread_data.loc[index, 'team_spread_line'] = odds_row['player2_points'].values[0]
                processed_spread_data.loc[index, 'opp_spread_line'] = odds_row['player1_points'].values[0]
        
        # Compute spread result: with team_spread = (opp_points - team_points),
        # team covers if team_spread_line + (team_points - opp_points) > 0,
        # which simplifies to team_spread_line - team_spread > 0. Pushes (== 0) excluded.
        processed_spread_data['team_spread_line'] = pd.to_numeric(
            processed_spread_data['team_spread_line'], errors='coerce'
        )
        processed_spread_data['spread_margin'] = (
            processed_spread_data['team_spread_line'] - processed_spread_data['team_spread']
        )
        # Drop rows where margin is NaN (missing line or scores) or push (== 0)
        processed_spread_data = processed_spread_data[processed_spread_data['spread_margin'].notna()]
        processed_spread_data = processed_spread_data[processed_spread_data['spread_margin'] != 0]
        processed_spread_data['spread_result'] = (processed_spread_data['spread_margin'] > 0).astype(int)

        # remove any rows where there is not a book spread line
        processed_spread_data = processed_spread_data[processed_spread_data['team_spread_line'].notna()]
        processed_spread_data = processed_spread_data[processed_spread_data['opp_spread_line'].notna()]

        print(f'Found spread odds: {count}')
        return processed_spread_data
    
    def add_total_odds(self):
        # add the moneyline odds to the processed data
        processed_data = self.add_moneyline_odds()

        # adding the odds to the processed data
        odds_data = pd.read_csv('data/nfl_odds.csv')

        # remove any rows where the market_name is not Spread
        odds_data = odds_data[odds_data['market_name'] == 'Total']
        # convert the start_date to ISO date string (UTC)
        odds_data['start_date'] = pd.to_datetime(
            odds_data['start_date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        processed_total_data = processed_data.copy()

        # do the same for the processed_data
        processed_total_data['date'] = pd.to_datetime(
            processed_total_data['date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        # create team_name column to odds_data
        odds_data['team_name'] = odds_data['event_name'].apply(
            lambda x: isinstance(x, str) and ' @ ' in x and get_team_from_name(x.split(' @ ')[0])
        )
        odds_data['opp_name'] = odds_data['event_name'].apply(
            lambda x: isinstance(x, str) and ' @ ' in x and get_team_from_name(x.split(' @ ')[1])
        )

        count = 0
        for index, row in processed_total_data.iterrows():
            # find the row in odds_data where the player and opponent are the same as the row in processed_data
            team1_match = (odds_data['team_name'] == row['team']) & (odds_data['opp_name'] == row['opp'])
            team2_match = (odds_data['team_name'] == row['opp']) & (odds_data['opp_name'] == row['team'])
            odds_row = odds_data[team1_match | team2_match]

            if odds_row.empty:
                continue

            # date values are already normalized to 'YYYY-MM-DD' strings above
            date = row['date']

            # filter for matching event date
            odds_row = odds_row[odds_row['start_date'] == date]

            if odds_row.empty:
                continue

            if len(odds_row) > 1:
                print(f'Multiple odds rows found for {row["team"]} vs {row["opp"]} on {date}')
                continue

            count += 1
            # convert the odds and add them to the row in processed_data
            processed_total_data.loc[index, 'over_total_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
            processed_total_data.loc[index, 'under_total_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
            processed_total_data.loc[index, 'total_line'] = odds_row['player1_points'].values[0]
        
        # Compute total result: with game_total = (opp_points + team_points),
        # game covers if (team_points + opp_points) > total_line
        processed_total_data['total_result'] = (processed_total_data['game_total'] > processed_total_data['total_line']).astype(int)

        # remove any rows where there is not a book total line
        processed_total_data = processed_total_data[processed_total_data['total_line'].notna()]

        print(f'Found total odds: {count}')
        return processed_total_data

if __name__ == '__main__':
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data'
    games_path = data_dir / 'nfl_games.csv'

    # Load the data with proper dtypes to avoid mixed type warnings
    dtype_dict = {
        'win': 'float64',  # win column has mixed types (strings and floats)
        'home': 'str',     # home column might have mixed types
    }
    game_data = pd.read_csv(games_path, dtype=dtype_dict, low_memory=False)

    # Preprocess the data (use spread-aware subclass to enable spread features later)
    preprocessor = Preprocessor(game_data)
    preprocessor.preprocess()
    
    # Add spread odds and features BEFORE balancing so we have both sides for opponent features
    spread_data = preprocessor.add_spread_odds()

    # Add total odds and features BEFORE balancing so we have both sides for opponent features
    total_data = preprocessor.add_total_odds()
    
    # Add moneyline odds and enhanced features BEFORE balancing so we have both sides for opponent features
    moneyline_data = preprocessor.add_moneyline_odds()
    
    # Balance the spread data first
    preprocessor.processed_data = spread_data
    preprocessor.balance()
    spread_data = preprocessor.processed_data

    # splitting into upcoming and training data
    upcoming_spread_data = spread_data[spread_data['date'] > pd.Timestamp.now().strftime('%Y-%m-%d')]
    training_spread_data = spread_data[spread_data['date'] <= pd.Timestamp.now().strftime('%Y-%m-%d')]

    # Balance the total data
    preprocessor.processed_data = total_data
    preprocessor.balance()
    total_data = preprocessor.processed_data

    # splitting into upcoming and training data
    upcoming_total_data = total_data[total_data['date'] > pd.Timestamp.now().strftime('%Y-%m-%d')]
    training_total_data = total_data[total_data['date'] <= pd.Timestamp.now().strftime('%Y-%m-%d')]

    # Balance the moneyline data 
    preprocessor.processed_data = moneyline_data
    preprocessor.balance()
    moneyline_data = preprocessor.processed_data

    # splitting into upcoming and training data
    # add a new column with the date shifted back by 3 hours
    moneyline_data['temp_date'] = moneyline_data['date'] - timedelta(hours=3)
    upcoming_data = moneyline_data[moneyline_data['temp_date'] > pd.Timestamp.now().strftime('%Y-%m-%d')]
    upcoming_data = upcoming_data.drop(columns=['temp_date'])
    training_data = moneyline_data[moneyline_data['temp_date'] <= pd.Timestamp.now().strftime('%Y-%m-%d')]
    training_data = training_data.drop(columns=['temp_date'])

    # writing the upcoming and training moneyline data to csv files
    (data_dir / 'moneyline_processed_data_upcoming.csv').parent.mkdir(parents=True, exist_ok=True)
    upcoming_data.to_csv(data_dir / 'moneyline_processed_data_upcoming.csv', index=False)
    (data_dir / 'moneyline_processed_data_training.csv').parent.mkdir(parents=True, exist_ok=True)
    training_data.to_csv(data_dir / 'moneyline_processed_data.csv', index=False)
    
    # writing the upcoming and training spread data to csv files
    (data_dir / 'spread_processed_data_upcoming.csv').parent.mkdir(parents=True, exist_ok=True)
    upcoming_spread_data.to_csv(data_dir / 'spread_processed_data_upcoming.csv', index=False)
    (data_dir / 'spread_processed_data.csv').parent.mkdir(parents=True, exist_ok=True)
    training_spread_data.to_csv(data_dir / 'spread_processed_data.csv', index=False)
    
    # writing the upcoming and training total data to csv files
    (data_dir / 'total_processed_data_upcoming.csv').parent.mkdir(parents=True, exist_ok=True)
    upcoming_total_data.to_csv(data_dir / 'total_processed_data_upcoming.csv', index=False)
    (data_dir / 'total_processed_data.csv').parent.mkdir(parents=True, exist_ok=True)
    training_total_data.to_csv(data_dir / 'total_processed_data.csv', index=False)