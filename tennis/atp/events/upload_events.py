import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
from functions.general import *

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Getting all the events
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

prediction_data = pd.read_csv('../preprocessing/data/processed_data_upcoming.csv')
prediction_data = add_total_games_odds_to_processed_data(prediction_data)
prediction_data = add_total_sets_odds_to_processed_data(prediction_data)
new_events = []
for index, row in prediction_data.iterrows():
    # adding the prediction to the dataframe with concat
    new_events.append({
      'player': row['player'],
      'opponent': row['opponent'],
      'date': row['date'],
      'book_odds1': row['player_odds'],
      'book_odds2': row['opponent_odds'],
      'player_country': row['player_country'],
      'opponent_country': row['opponent_country'],
      'tournament': row['tournament'],
      'player_total_games_odds_over': row['player_total_games_odds_over'],
      'player_total_games_odds_under': row['player_total_games_odds_under'],
      'player_total_games_odds_line': row['player_total_games_odds_line'],
      'opponent_total_games_odds_over': row['opponent_total_games_odds_over'],
      'opponent_total_games_odds_under': row['opponent_total_games_odds_under'],
      'opponent_total_games_odds_line': row['opponent_total_games_odds_line'],
      'total_sets_odds_over': row['total_sets_odds_over'],
      'total_sets_odds_under': row['total_sets_odds_under'],
      'total_sets_line': row['total_sets_line']
    })
new_events = pd.DataFrame(new_events)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE PREDICTIONS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# writing the new predictions to the database
load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

# insert the new predictions into the database
for index, row in new_events.iterrows():
    
    player_img = get_image_url(row['player_country'])
    opp_img = get_image_url(row['opponent_country'])

    # creating the row id by combining the two player names in alphabetical order and the date
    first_name = row['player'] if row['player'] < row['opponent'] else row['opponent']
    second_name = row['player'] if row['player'] > row['opponent'] else row['opponent']
    
    row_id = first_name + second_name + row['tournament']

    # if the book odds do not exist, then don't add them to the database
    if pd.isna(row['book_odds1']) or pd.isna(row['book_odds2']):
        print(f"Book odds do not exist for {row_id}")
        continue

    print(row_id, 'tournament:', row['tournament'])

    response = (
        supabase.table('events')
        .upsert({
            'id': row_id,
            'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'event_name': f"{row['player']} vs {row['opponent']}",
            'event_date': row['date'],
            'event_datetime': row['date'],
            'team1': row['player'],
            'team1_name': row['player'],
            'team2': row['opponent'],
            'team2_name': row['opponent'],
            'book_odds1': int(row['book_odds1']) if not pd.isna(row['book_odds1']) else 0,
            'book_odds2': int(row['book_odds2']) if not pd.isna(row['book_odds2']) else 0,
            'team1_pic_url': player_img,
            'team2_pic_url': opp_img,
            'event_type': 'atp',
            'tournament': row['tournament']
        })
        .execute()
    )

    # add the odds data to the moneyline_odds_data table if they exist
    if not pd.isna(row['book_odds1']) and not pd.isna(row['book_odds2']):
        supabase.table('moneyline_book_odds_data').insert({
            'event_id': row_id,
            'odds1': int(row['book_odds1']) if not pd.isna(row['book_odds1']) else 0,
            'odds2': int(row['book_odds2']) if not pd.isna(row['book_odds2']) else 0
        }).execute()
    
    # add the odds data to the moneyline_odds_data table if they exist
    if not pd.isna(row['player_total_games_odds_over']) and not pd.isna(row['player_total_games_odds_under']) and not pd.isna(row['opponent_total_games_odds_over']) and not pd.isna(row['opponent_total_games_odds_under']):
        supabase.table('total_games_book_odds_data').insert({
            'event_id': row_id,
            'team1_total_games_line': float(row['player_total_games_odds_line']) if not pd.isna(row['player_total_games_odds_line']) else 0,
            'team1_total_games_over_odds': int(row['player_total_games_odds_over']) if not pd.isna(row['player_total_games_odds_over']) else 0,
            'team1_total_games_under_odds': int(row['player_total_games_odds_under']) if not pd.isna(row['player_total_games_odds_under']) else 0,
            'team2_total_games_line': float(row['opponent_total_games_odds_line']) if not pd.isna(row['opponent_total_games_odds_line']) else 0,
            'team2_total_games_over_odds': int(row['opponent_total_games_odds_over']) if not pd.isna(row['opponent_total_games_odds_over']) else 0,
            'team2_total_games_under_odds': int(row['opponent_total_games_odds_under']) if not pd.isna(row['opponent_total_games_odds_under']) else 0
        }).execute()
    
    # add the odds data to the moneyline_odds_data table if they exist
    if not pd.isna(row['total_sets_odds_over']) and not pd.isna(row['total_sets_odds_under']):
        supabase.table('total_sets_book_odds_data').insert({
            'event_id': row_id,
            'total_sets_line': float(row['total_sets_line']) if not pd.isna(row['total_sets_line']) else 0,
            'total_sets_over_odds': int(row['total_sets_odds_over']) if not pd.isna(row['total_sets_odds_over']) else 0,
            'total_sets_under_odds': int(row['total_sets_odds_under']) if not pd.isna(row['total_sets_odds_under']) else 0
        }).execute()