import pandas as pd
from datetime import datetime, timezone, timedelta
from functions.nfl_images import *
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

def formatDate(date: str, time: str):
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

def update_completed_event(team: str, opp: str, date: str, winner: str):
  # get the event id
  team1 = team
  event_id = f'{team}{opp}{date}' if team < opp else f'{opp}{team}{date}'

  supabase_url = os.getenv('SUPABASE_URL')
  supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
  supabase = create_client(supabase_url, supabase_key)

  # get the event id by checking to see if the event exists in the database
  event = supabase.table('events').select('*').eq('id', event_id).execute()
  if event.data:
    event_id = event.data[0]['id']
    team1 = event.data[0]['team1']
  else:
    print(f'Event {event_id} not found in the database')
    event_id = f'{team}{opp}{date}' if opp < team else f'{opp}{team}{date}'
  event = supabase.table('events').select('*').eq('id', event_id).execute()
  if event.data:
    event_id = event.data[0]['id']
    team1 = event.data[0]['team1']
  else:
    print(f'Event {event_id} not found in the database')
    return

  # update the event in the database
  response = (
    supabase.table('events')
    .update({
      'result': winner == team1,
      'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    })
    .eq('id', event_id)
    .execute()
  )

def read_recent_events():
  # open the moneyline_processed_data_training.csv file
  data = pd.read_csv('../preprocessing/data/moneyline_processed_data.csv')

  # Pre-compute date conversions (data is already in UTC)
  data['formatted_date'] = pd.to_datetime(data['date'])

  # get all the events in the past week
  past_week = data[data['formatted_date'] > datetime.now(timezone.utc) - timedelta(days=7)]

  for _, row in past_week.iterrows():
    # get the team and opponent names
    team = row['team']
    opp = row['opp']
    date = row['formatted_date'].strftime('%Y-%m-%d')
    winner = team if row["result"] == 1 else opp

    print(f'{team} vs {opp} on {date} -- winner: {winner}')
    
    # update the completed event in the database
    update_completed_event(team, opp, date, winner)

if __name__ == '__main__':
  read_recent_events()