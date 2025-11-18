import pandas as pd
from datetime import timedelta
from typing import Dict, List, Any
from pathlib import Path
from functions.general import *
import pytz
import time
et_tz = pytz.timezone('US/Eastern')
utc_tz = pytz.UTC

class Processor:
    def __init__(self, game_data):
        print('initializing preprocessor...')
        self.game_data = self.clean_data(game_data)
        self.processed_data = None
        # Process and convert dates to timezone-aware UTC format
        self._process_dates()
        self.json_data = self.convert_to_json()
        # Pre-compute stat keys for faster lookups
        self._init_stat_keys()
    
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
        # NBA data doesn't have a time column, so we pass empty string
        self.game_data['formatted_date'] = pd.to_datetime(self.game_data.apply(
            lambda row: self.formatDate(row['date'], row.get('time', '')), axis=1
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
        """Pre-compute stat keys used in processing"""
        # Updated to match exact CSV column names
        self.base_stats = {
            'wins': 0,
            'losses': 0,
            'team_pts': 0,
            'opponent_pts': 0,
            'team_field_goals': 0,
            'team_field_goals_attempted': 0,
            'team_field_goals_percentage': 0,
            'team_three_point_field_goals': 0,
            'team_three_point_field_goals_attempted': 0,
            'team_three_point_field_goals_percentage': 0,
            'team_free_throws': 0,
            'team_free_throws_attempted': 0,
            'team_free_throws_percentage': 0,
            'team_offensive_rebounds': 0,
            'team_rebounds': 0,
            'team_assists': 0,
            'team_steals': 0,
            'team_blocks': 0,
            'team_turnovers': 0,
            'team_personal_fouls': 0,
            'opponent_field_goals': 0,
            'opponent_field_goals_attempted': 0,
            'opponent_field_goals_percentage': 0,
            'opponent_three_point_field_goals': 0,
            'opponent_three_point_field_goals_attempted': 0,
            'opponent_three_point_field_goals_percentage': 0,
            'opponent_free_throws': 0,
            'opponent_free_throws_attempted': 0,
            'opponent_free_throws_percentage': 0,
            'opponent_offensive_rebounds': 0,
            'opponent_rebounds': 0,
            'opponent_assists': 0,
            'opponent_steals': 0,
            'opponent_blocks': 0,
            'opponent_turnovers': 0,
            'opponent_personal_fouls': 0
        }
        
        # Create mapping between base_stats keys and CSV column names
        self.stat_mapping = {
            'team_pts': 'points',
            'opponent_pts': 'opponent_points',
            'team_rebounds': 'total_rebounds',
            'opponent_rebounds': 'opponent_total_rebounds'
        }
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float, return 0 if invalid"""
        if isinstance(value, (int, float)):
            return float(value)
        if not value or value == ' ':
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
        
        # Group by team and sort by date in one operation
        json_data = {}
        for team in self.game_data['team'].unique():
            team_data = self.game_data[self.game_data['team'] == team].sort_values('formatted_date', ascending=False)
            json_data[team] = team_data.to_dict('records')
        
        return json_data

    def _process_game_stats(self, game: Dict, stats: Dict, prefix: str):
        """Process a single game's statistics with safe key access"""
        for key in self.base_stats.keys():
            if key == 'wins':
                stats[f'{prefix}{key}'] += 1 if game.get('win') == 1 else 0
            elif key == 'losses':
                stats[f'{prefix}{key}'] += 1 if game.get('win') == 0 else 0
            else:
                # Get the correct column name from mapping or use the key itself
                game_key = self.stat_mapping.get(key, key.replace('team_', '').replace('opponent_', 'opponent_'))
                stats[f'{prefix}{key}'] += self._safe_float(game.get(game_key, 0))
        return stats

    def getGamesInLastXYears(self, team_name: str, date: pd.Timestamp, x: List[int]) -> Dict:
        """Optimized version of getGamesInLastXYears"""
        full_stats = {}
        for years in x:
            prefix = f'last_{years}_yr_'
            full_stats.update({f'{prefix}{k}': v for k, v in self.base_stats.items()})

        if team_name not in self.json_data:
            return full_stats

        team_data = self.json_data[team_name]
        x_game_counts = [0] * len(x)
        
        for game in team_data:
            game_date = pd.to_datetime(game['date'])
            if game_date >= date:
                continue
                
            for i, years in enumerate(x):
                if game_date >= date - pd.DateOffset(years=years):
                    prefix = f'last_{years}_yr_'
                    self._process_game_stats(game, full_stats, prefix)
                    x_game_counts[i] += 1
                else:
                    break

        # Average the percentage stats
        for i, years in enumerate(x):
            prefix = f'last_{years}_yr_'
            divisor = x_game_counts[i] if x_game_counts[i] > 0 else 1
            for stat in ['team_field_goals_percentage', 'opponent_field_goals_percentage',
                        'team_three_point_field_goals_percentage', 'opponent_three_point_field_goals_percentage',
                        'team_free_throws_percentage', 'opponent_free_throws_percentage']:
                full_stats[f'{prefix}{stat}'] /= divisor

        return full_stats
    
    def getLastXGameStats(self, team: str, date, x: int, prefix: str):
        """
        Similar to NFL version - returns the stats of the last x games played by a team
        with a custom prefix for the stat keys
        team: str --> team name
        date: pd.Timestamp --> date of the game
        x: int --> number of games to look back
        prefix: str --> prefix to add to stat keys (e.g., 'last_5_team_')
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

        # Average all stats by number of games found (similar to NFL version)
        divisor = counter if counter > 0 else 1
        for key in full_stats:
            full_stats[key] /= divisor

        return full_stats
    
    def getCurrentSeasonGameNumber(self, team: str, date: pd.Timestamp) -> int:
        """
        Gets the current game number of the season for a team.
        NBA seasons run from October to June of the following year.
        For example, the 2024-2025 season runs from Oct 2024 to June 2025.
        
        team: str --> team name
        date: pd.Timestamp --> date of the game
        """
        if team not in self.json_data:
            return 1
            
        # Determine the season start date
        # If the game is in Oct-Dec, season started in October of that year
        # If the game is in Jan-Sep, season started in October of previous year
        game_year = date.year
        game_month = date.month
        
        if game_month >= 10:  # October, November, December
            season_start = pd.Timestamp(year=game_year, month=10, day=1)
        else:  # January through September
            season_start = pd.Timestamp(year=game_year - 1, month=10, day=1)
        
        # Make season_start timezone-aware if date is timezone-aware
        if date.tz is not None:
            season_start = season_start.tz_localize(date.tz)
        
        # Count games played by this team since season start, before this game's date
        team_data = self.json_data[team]
        games_in_season = [
            game for game in team_data 
            if season_start <= pd.to_datetime(game['formatted_date']) < date
        ]
        
        return len(games_in_season) + 1

    def preprocess(self):
        print('started preprocessing...')
        
        # Pre-allocate the DataFrame with expected size
        data = []

        # create a column that is the game number of the season for the team
        self.game_data['game_number'] = self.game_data.apply(
            lambda row: self.getCurrentSeasonGameNumber(row['team'], row['formatted_date']), axis=1
        )
        
        for idx, row in self.game_data.iterrows():
            try:
                # get the date of the game
                formatted_date = row['formatted_date']

                # skip the game if it is one of the first 5 games of the season
                if row['game_number'] <= 5:
                    continue
                
                # get the teams
                team = row['team']
                opponent = row['opponent']                
                # Sportsbook-style: team_spread = opponent_points - team_points
                team_spread = self._safe_float(row.get('opponent_points', 0)) - self._safe_float(row.get('points', 0))
                opp_spread = self._safe_float(row.get('points', 0)) - self._safe_float(row.get('opponent_points', 0))

                game_total = self._safe_float(row.get('points', 0)) + self._safe_float(row.get('opponent_points', 0))

                # get the last x games stats
                home_team_last_5_games = self.getLastXGameStats(team, formatted_date, 5, 'last_5_team_')
                away_team_last_5_games = self.getLastXGameStats(opponent, formatted_date, 5, 'last_5_opp_')
                home_team_last_game = self.getLastXGameStats(team, formatted_date, 1, 'last_1_team_')
                away_team_last_game = self.getLastXGameStats(opponent, formatted_date, 1, 'last_1_opp_')

                # create a dictionary of the data
                data_row = {
                    'team': team,
                    'opponent': opponent,
                    'date': formatted_date,
                    'result': row.get('win', 0),
                    'location': row.get('home', 0),
                    'team_spread': team_spread,
                    'opp_spread': opp_spread,
                    'game_total': game_total,
                    'points': self._safe_float(row.get('points', 0)),
                    'opponent_points': self._safe_float(row.get('opponent_points', 0)),
                    **home_team_last_5_games,
                    **away_team_last_5_games,
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
        
        self.processed_data = data
    
    def balance(self):
        """
        Balances the data by removing games until there is an equal number of wins and losses.
        Properly handles game pairs to prevent duplicates.
        """
        if self.processed_data is None:
            print("No processed data to balance")
            return

        # Create a copy of the processed data
        grouped_data = self.processed_data.copy()

        # Create a team_pair column where team and opp are alphabetically sorted
        grouped_data['team_pair'] = grouped_data.apply(
            lambda row: '-'.join(sorted([row['team'], row['opponent']])), axis=1
        )

        # Create a unique game identifier
        grouped_data['game_id'] = grouped_data.apply(
            lambda row: f"{row['team_pair']}-{row['date']}", axis=1
        )

        # Sort by team_pair and date to group matching games together
        grouped_data = grouped_data.sort_values(by=['team_pair', 'date'], ascending=False)

        # Create a list of game pairs with explicit mapping
        game_pairs = []
        
        # Iterate over the grouped data based on the team_pair and date
        for game_id, group in grouped_data.groupby(['team_pair', 'date']):
            if len(group) == 2:
                # Both perspectives exist - store as a pair with explicit mapping
                game_pairs.append({
                    'perspective_1': group.iloc[0].to_dict(),
                    'perspective_2': group.iloc[1].to_dict(),
                    'has_both': True
                })
            else:
                # Only one perspective exists
                game_pairs.append({
                    'perspective_1': group.iloc[0].to_dict(),
                    'perspective_2': None,
                    'has_both': False
                })

        # Balance the data using the properly paired games
        wins, losses = 0, 0
        balanced_data = []
        used_games = set()  # Track which games we've already added
        
        for pair in game_pairs:
            p1 = pair['perspective_1']
            p2 = pair['perspective_2']
            game_id = p1['game_id']
            
            # Skip if we've already processed this game
            if game_id in used_games:
                continue
            
            # If only one perspective exists, add it and move on
            if not pair['has_both']:
                balanced_data.append(p1)
                if p1['result'] == 1:
                    wins += 1
                else:
                    losses += 1
                used_games.add(game_id)
                continue
            
            # Both perspectives exist - choose based on balancing logic
            if wins <= losses:
                # We need more wins, prefer the winning perspective
                if p1['result'] == 1:
                    balanced_data.append(p1)
                    wins += 1
                elif p2['result'] == 1:
                    balanced_data.append(p2)
                    wins += 1
                else:
                    # Both are losses, just take perspective 1
                    balanced_data.append(p1)
                    losses += 1
            else:
                # We need more losses, prefer the losing perspective
                if p1['result'] == 0:
                    balanced_data.append(p1)
                    losses += 1
                elif p2['result'] == 0:
                    balanced_data.append(p2)
                    losses += 1
                else:
                    # Both are wins, just take perspective 1
                    balanced_data.append(p1)
                    wins += 1
            
            used_games.add(game_id)
        
        # Convert back to DataFrame
        balanced_data = pd.DataFrame(balanced_data)
        
        # Drop the temporary columns
        balanced_data = balanced_data.drop(columns=['team_pair', 'game_id'])

        # Print the number of wins and losses in the balanced data
        print(f"Balanced data - Wins: {wins}, Losses: {losses}")
        print(balanced_data['result'].value_counts())

        # Resort the data by date in descending order
        balanced_data = balanced_data.sort_values(by='date', ascending=False)
        
        # Drop all rows where the last_1_team_points or the last_1_opp_points is nan
        balanced_data = balanced_data[balanced_data['last_1_team_team_pts'].notna()]
        balanced_data = balanced_data[balanced_data['last_1_opp_team_pts'].notna()]

        self.processed_data = balanced_data
    
    def add_moneyline_odds(self):
        # adding the odds to the processed data
        odds_data = pd.read_csv('data/nba_odds.csv')

        # remove any rows where the market_name is not Moneyline
        odds_data = odds_data[odds_data['market_name'] == 'Moneyline']

        # convert the start_date to ISO date string (UTC)
        odds_data['datetime'] = odds_data['start_date']
        odds_data['start_date'] = pd.to_datetime(
            odds_data['start_date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        processed_moneyline_data = self.processed_data.copy()
        
        # do the same for the processed_data
        processed_moneyline_data['date'] = pd.to_datetime(
            processed_moneyline_data['date'], format='mixed', utc=True
        ).dt.strftime('%Y-%m-%d')

        # add a fallback date in case the date is not found in the odds data that is 1 day after
        processed_moneyline_data['fallback_date'] = (pd.to_datetime(
            processed_moneyline_data['date'], format='mixed', utc=True
        ) + timedelta(days=1)).dt.strftime('%Y-%m-%d')

        count = 0
        for index, row in processed_moneyline_data.iterrows():
            # Convert team abbreviations to full names, then to DK names
            team_name = get_name_from_team(row['team'])
            opp_name = get_name_from_team(row['opponent'])
            
            if not team_name or not opp_name:
                continue
            
            team_dk_name = get_dk_name_from_team(team_name)
            opp_dk_name = get_dk_name_from_team(opp_name)
            
            if not team_dk_name or not opp_dk_name:
                continue
            
            # find the row in odds_data where the player and opponent are the same as the row in processed_data
            player_match = (odds_data['player1_name'] == team_dk_name) & (odds_data['player2_name'] == opp_dk_name)
            opponent_match = (odds_data['player1_name'] == opp_dk_name) & (odds_data['player2_name'] == team_dk_name)
            odds_row = odds_data[player_match | opponent_match]

            if odds_row.empty:
                if (pd.to_datetime(row['fallback_date']) < pd.Timestamp.now() + timedelta(days=7)) and (pd.to_datetime(row['fallback_date']) > pd.Timestamp.now()):
                    print(f'up here -- no odds found for {row["team"]} vs {row["opponent"]} on {row["date"]}')
                continue

            # date values are already normalized to 'YYYY-MM-DD' strings above
            date = row['date']
            fallback_date = row['fallback_date']

            # pre_date_odds_row = odds_row

            # filter for matching event date
            odds_row_att_1 = odds_row[odds_row['start_date'] == date]
            if odds_row_att_1.empty:
                odds_row_att_2 = odds_row[odds_row['start_date'] == fallback_date]
            
            # assign the odds row to either the first or second odds row, whichever is not empty
            if odds_row_att_1.empty:
                odds_row = odds_row_att_2
            else:
                odds_row = odds_row_att_1

            if odds_row.empty:
                # if (pd.to_datetime(row['fallback_date']) < pd.Timestamp.now() + timedelta(days=7)) and (pd.to_datetime(row['fallback_date']) > pd.Timestamp.now()):
                #     for index, odd_row in pre_date_odds_row.iterrows():
                #         print(f'    pre_date_odds_row: {odd_row["player1_name"]} vs {odd_row["player2_name"]} on {odd_row["start_date"]}')
                #     print(f'down here -- no odds found for {row["team"]} vs {row["opponent"]} on {row["date"]} or {row["fallback_date"]}')
                continue

            if len(odds_row) > 1:
                print(f'Multiple odds rows found for {row["team"]} vs {row["opponent"]} on {date}')
                # take the last row (by date)
                odds_row = odds_row.sort_values(by='start_date', ascending=False)

            count += 1
            # convert the odds and add them to the row in processed_data
            if odds_row['player1_name'].values[0] == team_dk_name:
                processed_moneyline_data.loc[index, 'player_odds'] = odds_row['player1_odds'].values[0]
                processed_moneyline_data.loc[index, 'opponent_odds'] = odds_row['player2_odds'].values[0]
            else:
                processed_moneyline_data.loc[index, 'player_odds'] = odds_row['player2_odds'].values[0]
                processed_moneyline_data.loc[index, 'opponent_odds'] = odds_row['player1_odds'].values[0]
            
            # update the date to the date in the odds_data
            processed_moneyline_data.loc[index, 'date'] = odds_row['datetime'].values[0]

        print(f'Found moneyline odds: {count}')

        # drop the fallback_date column
        processed_moneyline_data = processed_moneyline_data.drop(columns=['fallback_date'])

        return processed_moneyline_data

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data'
    games_path = data_dir / 'nba_games.csv'

    processed_start_time = time.time()

    # Load the data with proper dtypes to avoid mixed type warnings
    dtype_dict = {
        'win': 'float64',  # win column has mixed types (strings and floats)
        'home': 'str',     # home column might have mixed types
    }
    game_data = pd.read_csv(games_path, dtype=dtype_dict, low_memory=False)

    # Preprocess the data (use spread-aware subclass to enable spread features later)
    preprocessor = Processor(game_data)
    preprocessor.preprocess()
    
    # Add moneyline odds and enhanced features BEFORE balancing so we have both sides for opponent features
    moneyline_data = preprocessor.add_moneyline_odds()
    
    # Balance the moneyline data 
    preprocessor.processed_data = moneyline_data
    preprocessor.balance()
    moneyline_data = preprocessor.processed_data

    # add a date_str column to the moneyline data that is the date in the format 'YYYY-MM-DD'
    moneyline_data['date_str'] = pd.to_datetime(
        moneyline_data['date'], format='mixed', utc=True
    ).dt.strftime('%Y-%m-%d')

    # splitting into upcoming and training data
    upcoming_data = moneyline_data[moneyline_data['date_str'] > pd.Timestamp.now().strftime('%Y-%m-%d')]
    print(f'upcoming data length: {len(upcoming_data)}')
    training_data = moneyline_data[moneyline_data['date_str'] <= pd.Timestamp.now().strftime('%Y-%m-%d')]
    # drop the date_str column from the upcoming data
    upcoming_data = upcoming_data.drop(columns=['date_str'])
    # drop the date_str column from the training data
    training_data = training_data.drop(columns=['date_str'])

    elapsed_time = time.time() - processed_start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f'done processing, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')

    # writing the upcoming and training moneyline data to csv files
    (data_dir / 'moneyline_processed_data_upcoming.csv').parent.mkdir(parents=True, exist_ok=True)
    upcoming_data.to_csv(data_dir / 'moneyline_processed_data_upcoming.csv', index=False)
    (data_dir / 'moneyline_processed_data_training.csv').parent.mkdir(parents=True, exist_ok=True)
    training_data.to_csv(data_dir / 'moneyline_processed_data.csv', index=False)