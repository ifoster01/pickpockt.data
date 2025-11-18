import pandas as pd
from datetime import timedelta
import math

def convert_odds_to_american(odds):
  if odds >= 2:
      return math.floor((odds - 1) * 100)
  else:
      return math.floor(-100 / (odds - 1))

def normalize_name(name):
  name_map = {
    'JooSang Yoo': 'Joo Sang Yoo',
    'Jiri Prochazka': 'Jiří Procházka',
    'Benoit Saint Denis': 'Benoit Saint-Denis',
  }

  if name in name_map:
    return name_map[name]
  else:
    return name
  
def add_moneyline_odds(processed_data):
  odds_data = pd.read_csv('../preprocessing/data/upcoming_ufc_odds.csv')
  processed_moneyline_data = processed_data.copy()
  processed_moneyline_data['f1_name'] = processed_moneyline_data['f1_name'].apply(normalize_name)
  processed_moneyline_data['f2_name'] = processed_moneyline_data['f2_name'].apply(normalize_name)

  # remove any rows where the market_name is not Moneyline
  odds_data = odds_data[odds_data['market_name'] == 'Moneyline']
  
  odds_data['start_date_str'] = pd.to_datetime(
      odds_data['start_date'], format='mixed', utc=True
  ).dt.strftime('%Y-%m-%d')

  # create team_name column to odds_data
  odds_data['team_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0]
  )
  odds_data['opp_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[1]
  )

  count = 0
  for index, row in processed_moneyline_data.iterrows():
      # find the row in odds_data where the player and opponent are the same as the row in processed_data
      team1_match = (odds_data['team_name'] == row['f1_name']) & (odds_data['opp_name'] == row['f2_name'])
      team2_match = (odds_data['team_name'] == row['f2_name']) & (odds_data['opp_name'] == row['f1_name'])
      odds_row = odds_data[team1_match | team2_match]

      if odds_row.empty:
          continue
      
      # date values are already normalized to 'YYYY-MM-DD' strings above
      date1 = row['date']
      # Ensure date1 is a datetime object, then add one day
      if not isinstance(date1, pd.Timestamp):
          date1_dt = pd.to_datetime(date1, utc=True, errors='coerce')
      else:
          date1_dt = date1
      date2 = (date1_dt + timedelta(days=1)).strftime('%Y-%m-%d')
      date1 = date1_dt.strftime('%Y-%m-%d')

      # filter for matching event date
      odds_row = odds_row[(odds_row['start_date_str'] == date1) | (odds_row['start_date_str'] == date2)]

      if odds_row.empty:
          continue

      if len(odds_row) > 1:
          print(f'Multiple odds rows found for {row["f1_name"]} vs {row["f2_name"]} on {date1} or {date2}')
          continue

      count += 1
      # convert the odds and add them to the row in processed_data
      if odds_row['player1_name'].values[0] == row['f1_name']:
        processed_moneyline_data.loc[index, 'f1_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
        processed_moneyline_data.loc[index, 'f2_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
      else:
        processed_moneyline_data.loc[index, 'f1_odds'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
        processed_moneyline_data.loc[index, 'f2_odds'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
      
      # replace the date with the date from the odds data
      processed_moneyline_data.loc[index, 'date'] = odds_row['start_date'].values[0]

  print(f'Found moneyline odds: {count}')
  return processed_moneyline_data

def add_distance_odds(processed_data):
  odds_data = pd.read_csv('../preprocessing/data/upcoming_ufc_odds.csv')
  processed_distance_data = processed_data.copy()

  # remove any rows where the market_name is not Fight to Go the Distance
  odds_data = odds_data[odds_data['market_name'] == 'Fight to Go the Distance']
  # convert the start_date to ISO date string (UTC)
  odds_data['start_date'] = pd.to_datetime(
      odds_data['start_date'], format='mixed', utc=True
  ).dt.strftime('%Y-%m-%d')

  # create team_name column to odds_data
  odds_data['team_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0]
  )
  odds_data['opp_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[1]
  )

  count = 0
  for index, row in processed_distance_data.iterrows():
      # find the row in odds_data where the player and opponent are the same as the row in processed_data
      team1_match = (odds_data['team_name'] == row['f1_name']) & (odds_data['opp_name'] == row['f2_name'])
      team2_match = (odds_data['team_name'] == row['f2_name']) & (odds_data['opp_name'] == row['f1_name'])
      odds_row = odds_data[team1_match | team2_match]


      if odds_row.empty:
          continue
      
      # date values are already normalized to 'YYYY-MM-DD' strings above
      date1 = row['date']
      # Ensure date1 is a datetime object, then add one day
      if not isinstance(date1, pd.Timestamp):
          date1_dt = pd.to_datetime(date1, utc=True, errors='coerce')
      else:
          date1_dt = date1
      date2 = (date1_dt + timedelta(days=1)).strftime('%Y-%m-%d')
      date1 = date1_dt.strftime('%Y-%m-%d')

      # filter for matching event date
      odds_row = odds_row[(odds_row['start_date'] == date1) | (odds_row['start_date'] == date2)]

      if odds_row.empty:
          continue

      if len(odds_row) > 1:
          print(f'Multiple odds rows found for {row["f1_name"]} vs {row["f2_name"]} on {date1} or {date2}')
          continue

      count += 1
      # convert the odds and add them to the row in processed_data
      processed_distance_data.loc[index, 'goes_the_distance_yes'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
      processed_distance_data.loc[index, 'goes_the_distance_no'] = convert_odds_to_american(odds_row['player2_odds'].values[0])

  print(f'Found fight to go the distance odds: {count}')
  return processed_distance_data

def add_rounds_odds(processed_data):
  odds_data = pd.read_csv('../preprocessing/data/upcoming_ufc_odds.csv')
  processed_rounds_data = processed_data.copy()

  # remove any rows where the market_name is not Fight to Go the Distance
  odds_data = odds_data[odds_data['market_name'] == 'Total Rounds']
  # convert the start_date to ISO date string (UTC)
  odds_data['start_date'] = pd.to_datetime(
      odds_data['start_date'], format='mixed', utc=True
  ).dt.strftime('%Y-%m-%d')

  # create team_name column to odds_data
  odds_data['team_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0]
  )
  odds_data['opp_name'] = odds_data['event_name'].apply(
      lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[1]
  )

  count = 0
  for index, row in processed_rounds_data.iterrows():
      # find the row in odds_data where the player and opponent are the same as the row in processed_data
      team1_match = (odds_data['team_name'] == row['f1_name']) & (odds_data['opp_name'] == row['f2_name'])
      team2_match = (odds_data['team_name'] == row['f2_name']) & (odds_data['opp_name'] == row['f1_name'])
      odds_row = odds_data[team1_match | team2_match]

      if odds_row.empty:
          continue
      
      # date values are already normalized to 'YYYY-MM-DD' strings above
      date1 = row['date']
      # Ensure date1 is a datetime object, then add one day
      if not isinstance(date1, pd.Timestamp):
          date1_dt = pd.to_datetime(date1, utc=True, errors='coerce')
      else:
          date1_dt = date1
      date2 = (date1_dt + timedelta(days=1)).strftime('%Y-%m-%d')
      date1 = date1_dt.strftime('%Y-%m-%d')

      # filter for matching event date
      odds_row = odds_row[(odds_row['start_date'] == date1) | (odds_row['start_date'] == date2)]

      if odds_row.empty:
          continue

      if len(odds_row) > 1:
          print(f'Multiple odds rows found for {row["f1_name"]} vs {row["f2_name"]} on {date1} or {date2}')
          continue

      count += 1
      # convert the odds and add them to the row in processed_data
      processed_rounds_data.loc[index, 'total_rounds_odds_over'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
      processed_rounds_data.loc[index, 'total_rounds_odds_under'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
      processed_rounds_data.loc[index, 'total_rounds_line'] = odds_row['player1_points'].values[0]

  print(f'Found total rounds odds: {count}')
  return processed_rounds_data