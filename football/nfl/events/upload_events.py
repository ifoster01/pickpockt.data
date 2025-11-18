import pandas as pd
from datetime import datetime, timezone, timedelta
from functions.nfl_images import *
import os
from supabase import create_client
from dotenv import load_dotenv

from functions.add_odds import add_spread_odds, add_total_odds

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Formatting the events 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# opening the processed upcoming event data
testing_data = pd.read_csv('../preprocessing/data/moneyline_processed_data_upcoming.csv')
testing_data['original_date'] = testing_data['date']
testing_data = add_spread_odds(testing_data)
testing_data = add_total_odds(testing_data)

events = []
for index, row in testing_data.iterrows():
    events.append({
        'Team': row['team'],
        'Opp': row['opp'],
        'Date': row['original_date'],
        'Book Odds 1': row['player_odds'],
        'Book Odds 2': row['opponent_odds'],
        'Spread Line 1': row['team_spread_line'],
        'Spread Line 2': row['opp_spread_line'],
        'Spread Odds 1': row['player_spread_odds'],
        'Spread Odds 2': row['opponent_spread_odds'],
        'Total Line': row['total_line'],
        'Total Odds 1': row['over_total_odds'],
        'Total Odds 2': row['under_total_odds'],
        'Week': row['week'],
        'Home': row['location']
    })

events = pd.DataFrame(events)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE EVENTS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# insert the new predictions into the database
for index, row in events.iterrows():
    # get the current date and shift it back 2 months
    current_date = pd.Timestamp.now(tz='UTC') - timedelta(days=60)
    game_date = pd.Timestamp(row['Date'])
    if game_date < current_date:
        continue

    # JSON serializable date
    game_date_obj = game_date.strftime('%Y-%m-%d')

    team_img = get_image(row['Team'])
    opp_img = get_image(row['Opp'])
    
    # converting the f1 name f2 name and date to a string to be used as an id for the row
    row_id = ''
    if row['Team'] < row['Opp']:
        row_id = row['Team'] + row['Opp'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
    else:
        row_id = row['Opp'] + row['Team'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')

    team_name_short = get_name_from_team(row['Team']).split(' ')[-1]
    opp_name_short = get_name_from_team(row['Opp']).split(' ')[-1]

    # get the tournament name
    if str(row['Week']).isdigit():
        tournament_name = 'Week ' + str(row['Week'])
    else:
        tournament_name = row['Week']
    
    # check if the event already exists in the database
    response = supabase.table('events').select('*').eq('id', row_id).execute()
    if response.data:
        pass
    else:
        if row['Team'] < row['Opp']:
            row_id = row['Team'] + row['Opp'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
        else:
            row_id = row['Opp'] + row['Team'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')

    response = (
        supabase.table('events')
        .upsert({
            'id': row_id,
            'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'event_name': f"{team_name_short} vs {opp_name_short}",
            'event_date': row['Date'],
            'event_datetime': row['Date'],
            'team1': row['Team'],
            'team1_name': get_name_from_team(row['Team']),
            'team2': row['Opp'],
            'team2_name': get_name_from_team(row['Opp']),
            'book_odds1': int(row['Book Odds 1']) if not pd.isna(row['Book Odds 1']) else 0,
            'book_odds2': int(row['Book Odds 2']) if not pd.isna(row['Book Odds 2']) else 0,
            'team1_pic_url': team_img,
            'team2_pic_url': opp_img,
            'event_type': 'nfl',
            'tournament': tournament_name,
            'home': row['Home']
        })
        .execute()
    )

    # add the odds data to the moneyline_odds_data table if they exist
    if not pd.isna(row['Book Odds 1']) and not pd.isna(row['Book Odds 2']):
        supabase.table('moneyline_book_odds_data').insert({
            'event_id': row_id,
            'odds1': int(row['Book Odds 1']) if not pd.isna(row['Book Odds 1']) else 0,
            'odds2': int(row['Book Odds 2']) if not pd.isna(row['Book Odds 2']) else 0
        }).execute()
    
    # add the spread odds data to the spread_book_odds_data table if they exist
    if not pd.isna(row['Spread Line 1']) and not pd.isna(row['Spread Line 2']) and not pd.isna(row['Spread Odds 1']) and not pd.isna(row['Spread Odds 2']):
        supabase.table('spread_book_odds_data').insert({
            'event_id': row_id,
            'team1_line': float(row['Spread Line 1']) if not pd.isna(row['Spread Line 1']) else 0,
            'team2_line': float(row['Spread Line 2']) if not pd.isna(row['Spread Line 2']) else 0,
            'odds1': int(row['Spread Odds 1']) if not pd.isna(row['Spread Odds 1']) else 0,
            'odds2': int(row['Spread Odds 2']) if not pd.isna(row['Spread Odds 2']) else 0
        }).execute()
    
    # add the total odds data to the total_book_odds_data table if they exist
    if not pd.isna(row['Total Line']) and not pd.isna(row['Total Odds 1']) and not pd.isna(row['Total Odds 2']):
        supabase.table('total_book_odds_data').insert({
            'event_id': row_id,
            'total_line': float(row['Total Line']) if not pd.isna(row['Total Line']) else 0,
            'over_odds': int(row['Total Odds 1']) if not pd.isna(row['Total Odds 1']) else 0,
            'under_odds': int(row['Total Odds 2']) if not pd.isna(row['Total Odds 2']) else 0
        }).execute()