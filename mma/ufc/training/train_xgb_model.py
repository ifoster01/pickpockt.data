import pandas as pd
import xgboost as xgb
from datetime import datetime, timezone
from functions.odds_functions import convert_american_to_probability

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# LOADING THE MODEL
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Open the model
xgb_model = xgb.XGBClassifier()
xgb_model.load_model('models/(cur)20918-profit-ac-6944-training-rows.json')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PREDICTING THE WINNER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_odds(win_prob):
    if win_prob<.5:
        fighterOdds=round(100/win_prob - 100)
        opponentOdds = round(1 / (1 / (1 - win_prob) - 1) * 100)
        return [fighterOdds,-1*opponentOdds]
    else:
        fighterOdds = round(1 / (1 / win_prob - 1) * 100)
        opponentOdds = round(100 / (1 - win_prob) - 100)
        return [-1*fighterOdds,opponentOdds]

# creating a dataframe with the historical predictions data
columns = ['Fighter 1', 'Fighter 2', 'Odds 1', 'Odds 2']
predictions = pd.DataFrame(columns=columns)

# Prediction function
def predict_fight(row, model):
    # Ensure the row is a DataFrame
    if not isinstance(row, pd.DataFrame):
        row = pd.DataFrame([row])
    
    # Make prediction
    prediction = model.predict_proba(row)[0, 1]  # Probability of class 1
    return prediction

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

# importing the new data
data = pd.read_csv('../preprocessing/data/new_fights_ready.csv')
data['f1_name'] = data['f1_name'].apply(normalize_name)
data['f2_name'] = data['f2_name'].apply(normalize_name)

# Create interaction features
data['f1_strike_grapple_interaction'] = data['f1_last_yr_strike_math'] * data['f1_last_yr_grapple_stats']
data['f2_strike_grapple_interaction'] = data['f2_last_yr_strike_math'] * data['f2_last_yr_grapple_stats']

# remove any rows where the last_X_yr_strike_math is 0
data = data[(data['f1_last_X_yr_strike_math'] != 0) & (data['f2_last_X_yr_strike_math'] != 0)]
for index, row in data.iterrows():
    # Create a single-row DataFrame
    clean_row = row.drop(['f1_name', 'f2_name', 'date'])
    clean_row_df = pd.DataFrame([clean_row])
    
    new_pred = predict_fight(clean_row, xgb_model)
    predicted_odds = get_odds(new_pred)

    # adding the prediction to the dataframe with concat
    predictions = pd.concat([predictions, pd.DataFrame([[row['f1_name'], row['f2_name'], predicted_odds[0], predicted_odds[1]]], columns=columns)], ignore_index=True)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE PREDICTIONS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# writing the new predictions to the database
import os, json, requests
from supabase import create_client
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

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

# load the odds from the csv
odds_data = pd.read_csv('../preprocessing/data/upcoming_ufc_odds.csv')
moneyline_odds = odds_data[odds_data['market_name'] == 'Moneyline'].copy()
# A function to normalize names for matching
def normalize_name(name):
    return name.lower()
moneyline_odds['p1_name_norm'] = moneyline_odds['player1_name'].apply(normalize_name)
moneyline_odds['p2_name_norm'] = moneyline_odds['player2_name'].apply(normalize_name)

# insert the new predictions into the database
for index, row in predictions.iterrows():
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

    # Find the fight in the odds data
    fight_odds_row = moneyline_odds[
        ((moneyline_odds['p1_name_norm'] == f1_name_norm) & (moneyline_odds['p2_name_norm'] == f2_name_norm)) |
        ((moneyline_odds['p1_name_norm'] == f2_name_norm) & (moneyline_odds['p2_name_norm'] == f1_name_norm))
    ]

    odds_row = None
    if not fight_odds_row.empty:
        odds_row = fight_odds_row.iloc[0]
        if odds_row['p1_name_norm'] == f1_name_norm:
            f1_book_odds = odds_row['player1_american_odds']
            f2_book_odds = odds_row['player2_american_odds']
        else:
            f1_book_odds = odds_row['player2_american_odds']
            f2_book_odds = odds_row['player1_american_odds']
    
    # converting the f1 name f2 name and date to a string to be used as an id for the row
    row_id = row['Fighter 1'] + row['Fighter 2'] + date

    # replace the date with the date from the odds data
    if odds_row is not None:
        date_time = odds_row['start_date']
    else:
        date_time = date
    
    f1_for_output = row['Fighter 1'].upper() + ' ' + ('✅' if row['Odds 1'] < 0 else '')
    f2_for_output = ('✅' if row['Odds 2'] < 0 else '') + ' ' + row['Fighter 2'].upper()
    print(f"{'Teams:':<12} |   {f1_for_output:<25} vs {f2_for_output:>25}")
    print(f"{'Prediction:':<12} |   ({convert_american_to_probability(row['Odds 1'])*100:.0f}%) {str(row['Odds 1']):<20} |  {str(row['Odds 2']):>20} ({convert_american_to_probability(row['Odds 1'])*100:.0f}%)")
    print(f"{'Book Odds:':<12} |   {str(f1_book_odds):<26} |  {str(f2_book_odds):>26}")
    print('-' * 70)

    # get if there is a "pick"
    team1_pick = True if row['Odds 1'] < -200 and f1_book_odds > -300 and f1_book_odds < 0 else False
    team2_pick = True if row['Odds 2'] < -200 and f2_book_odds > -300 and f2_book_odds < 0 else False

    upsert_data = {
        'event_id': row_id,
        'moneyline_odds1': row['Odds 1'],
        'moneyline_odds2': row['Odds 2'],
        'created_by': '1398dacb-0258-4a0c-b74f-da86241ddff4',
        'is_team1_pick': team1_pick,
        'is_team2_pick': team2_pick,
        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    }

    # get the event_odds row id (to see if it already exists)
    event_odds_row = supabase.table('event_odds').select('*').eq('event_id', row_id).eq('created_by', '1398dacb-0258-4a0c-b74f-da86241ddff4').execute()
    if event_odds_row.data:
        upsert_data['id'] = event_odds_row.data[0]['id']

    response = (
        supabase.table('event_odds')
        .upsert(upsert_data)
        .execute()
    )