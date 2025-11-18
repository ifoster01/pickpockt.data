import pandas as pd
from datetime import datetime, timezone
import os, requests
from supabase import create_client
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from functions.general import *

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

# importing the new data
data = pd.read_csv('../preprocessing/data/new_fights_ready.csv')
data = add_moneyline_odds(data)
data = add_distance_odds(data)
data = add_rounds_odds(data)

new_events = []
for index, row in data.iterrows():
    # adding the prediction to the dataframe with concat
    # Safely handle missing columns and missing values
    fighter1_distance_odds = None
    fighter2_distance_odds = None
    if 'goes_the_distance_yes' in row and 'goes_the_distance_no' in row:
        fighter1_distance_odds = row['goes_the_distance_yes'] if not pd.isna(row['goes_the_distance_yes']) else None
        fighter2_distance_odds = row['goes_the_distance_no'] if not pd.isna(row['goes_the_distance_no']) else None

    total_rounds_line = None
    total_rounds_odds_over = None
    total_rounds_odds_under = None
    if (
        'total_rounds_line' in row and 
        'total_rounds_odds_over' in row and 
        'total_rounds_odds_under' in row
    ):
        total_rounds_line = row['total_rounds_line'] if not pd.isna(row['total_rounds_line']) else None
        total_rounds_odds_over = row['total_rounds_odds_over'] if not pd.isna(row['total_rounds_odds_over']) else None
        total_rounds_odds_under = row['total_rounds_odds_under'] if not pd.isna(row['total_rounds_odds_under']) else None

    new_events.append({
        'Fighter 1': row['f1_name'],
        'Fighter 2': row['f2_name'],
        'Date': row['date'],
        'Fighter 1 Odds': row['f1_odds'],
        'Fighter 2 Odds': row['f2_odds'],
        'Fighter 1 Distance Odds': fighter1_distance_odds,
        'Fighter 2 Distance Odds': fighter2_distance_odds,
        'Total Rounds Line': total_rounds_line,
        'Total Rounds Odds Over': total_rounds_odds_over,
        'Total Rounds Odds Under': total_rounds_odds_under,
    })
new_events = pd.DataFrame(new_events)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE EVENTS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# get the link to the upcoming fights
html = requests.get('https://www.ufc.com/').text
soup = BeautifulSoup(html, 'html.parser')

upcoming_fights_div = soup.find('div', 'event-lastnext-paragraph next')
upcoming_fights_links = upcoming_fights_div.find_all('a')
fight_name = ''
try:
    upcoming_fights_link = f"https://www.ufc.com{upcoming_fights_links[0]['href']}"
    fight_name = ' '.join([word.capitalize() for word in upcoming_fights_links[0]['href'].split('/')[-1].replace('-', ' ').split(' ')])
    # capitalize all the letters in the first word
    fight_name = fight_name.split(' ')[0].upper() + ' ' + ' '.join([word.capitalize() for word in fight_name.split(' ')[1:]])
except:
    print('No upcoming fights found -- please manually add the link to the upcoming fights')
    exit(1)

# get the html for the upcoming fights
html = requests.get(upcoming_fights_link).text
soup = BeautifulSoup(html, 'html.parser')
date = soup.find('div', 'c-hero__headline-suffix tz-change-inner')['data-timestamp']
# convert the date timestamp to a date string in UTC
date = datetime.fromtimestamp(int(date), timezone.utc).strftime('%Y-%m-%d')

fights = soup.find_all('div', 'c-listing-fight__content')

fighter_images = []
for fight in fights:
    fighter_links = fight.find_all('a')
    fighter_links = fighter_links[::2]

    imgs = fight.find_all('img', 'image-style-event-fight-card-upper-body-of-standing-athlete')

    fighter1 = fighter_links[0]['href'].split('/')[-1].replace('-', ' ').lower()
    fighter1_img = imgs[0]['src'] if len(imgs) > 0 else None

    fighter2 = fighter_links[1]['href'].split('/')[-1].replace('-', ' ').lower()
    fighter2_img = imgs[1]['src'] if len(imgs) > 1 else None

    fighter_images.append({
        'fighter1': fighter1,
        'fighter2': fighter2,
        'fighter1_img': fighter1_img,
        'fighter2_img': fighter2_img
    })
# A function to normalize names for matching
def normalize_name(name):
    return name.lower()
new_events['p1_name_norm'] = new_events['Fighter 1'].apply(normalize_name)
new_events['p2_name_norm'] = new_events['Fighter 2'].apply(normalize_name)

# insert the new predictions into the database
for index, row in new_events.iterrows():
    f1_img, f2_img = '', ''
    f1_book_odds, f2_book_odds = 0, 0
    
    f1_name_norm = normalize_name(row['Fighter 1'])
    f2_name_norm = normalize_name(row['Fighter 2'])

    for x in fighter_images:
        if x['fighter1'] == f1_name_norm and x['fighter2'] == f2_name_norm:
            f1_img = x['fighter1_img']
            f2_img = x['fighter2_img']
            break
        elif x['fighter1'] == f2_name_norm and x['fighter2'] == f1_name_norm:
            f1_img = x['fighter2_img']
            f2_img = x['fighter1_img']
            break
        
    # converting the f1 name f2 name and date to a string to be used as an id for the row
    row_id = row['Fighter 1'] + row['Fighter 2'] + date

    response = (
        supabase.table('events')
        .upsert({
            'id': row_id,
            'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'event_name': fight_name,
            'event_date': row['Date'],
            'event_datetime': row['Date'],
            'team1': row['Fighter 1'],
            'team1_name': row['Fighter 1'],
            'team2': row['Fighter 2'],
            'team2_name': row['Fighter 2'],
            'book_odds1': int(row['Fighter 1 Odds']) if not pd.isna(row['Fighter 1 Odds']) else 0,
            'book_odds2': int(row['Fighter 2 Odds']) if not pd.isna(row['Fighter 2 Odds']) else 0,
            'team1_pic_url': f1_img,
            'team2_pic_url': f2_img,
            'event_type': 'ufc',
            'tournament': fight_name
        })
        .execute()
    )

    # add the odds data to the moneyline_odds_data table if they exist
    if not pd.isna(row['Fighter 1 Odds']) and not pd.isna(row['Fighter 2 Odds']):
        supabase.table('moneyline_book_odds_data').insert({
            'event_id': row_id,
            'odds1': int(row['Fighter 1 Odds']),
            'odds2': int(row['Fighter 2 Odds'])
        }).execute()
    
    # add the distance odds data to the distance_book_odds_data table if they exist
    if not pd.isna(row['Fighter 1 Distance Odds']) and not pd.isna(row['Fighter 2 Distance Odds']):
        supabase.table('goes_the_distance_book_odds_data').insert({
            'event_id': row_id,
            'goes_the_distance_yes_odds': int(row['Fighter 1 Distance Odds']),
            'goes_the_distance_no_odds': int(row['Fighter 2 Distance Odds'])
        }).execute()
    
    # add the rounds odds data to the rounds_book_odds_data table if they exist
    if not pd.isna(row['Total Rounds Line']) and not pd.isna(row['Total Rounds Odds Over']) and not pd.isna(row['Total Rounds Odds Under']):
        supabase.table('total_rounds_book_odds_data').insert({
            'event_id': row_id,
            'total_rounds_line': float(row['Total Rounds Line']),
            'over_odds': int(row['Total Rounds Odds Over']),
            'under_odds': int(row['Total Rounds Odds Under'])
        }).execute()