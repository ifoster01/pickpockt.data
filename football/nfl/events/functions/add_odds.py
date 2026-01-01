import pandas as pd
from .nfl_images import *
import math

def convert_odds_to_american(odds):
  if odds >= 2:
      return math.floor((odds - 1) * 100)
  else:
      return math.floor(-100 / (odds - 1))

def add_spread_odds(processed_data):
  # adding the odds to the processed data
  odds_data = pd.read_csv('../preprocessing/data/upcoming_nfl_odds.csv')

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
          print(f'No odds found for {row["team"]} vs {row["opp"]}')
          continue

      # date values are already normalized to 'YYYY-MM-DD' strings above
      date = row['date']

      # filter for matching event date
      odds_row = odds_row[odds_row['start_date'] == date]

      if odds_row.empty:
          print(f'No odds found for {row["team"]} vs {row["opp"]} on {date}')
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

  # remove any rows where there is not a book spread line
  try:
    processed_spread_data = processed_spread_data[processed_spread_data['team_spread_line'].notna()]
    processed_spread_data = processed_spread_data[processed_spread_data['opp_spread_line'].notna()]
  except Exception as e:
    print(f'Error removing rows where there is not a book spread line: {e}')
    print('Likely due to no odds found for the game')
    return processed_spread_data

  print(f'Found spread odds: {count}')
  return processed_spread_data

def add_total_odds(processed_data):
  # adding the odds to the processed data
  odds_data = pd.read_csv('../preprocessing/data/upcoming_nfl_odds.csv')

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

  # remove any rows where there is not a book total line
  try:
    processed_total_data = processed_total_data[processed_total_data['total_line'].notna()]
  except Exception as e:
    print(f'Error removing rows where there is not a book total line: {e}')
    print('Likely due to no odds found for the game')
    return processed_total_data

  print(f'Found total odds: {count}')
  return processed_total_data