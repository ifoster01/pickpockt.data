import pandas as pd
import xgboost as xgb
from functions.general import *
from functions.odds_functions import convert_american_to_probability
from datetime import datetime, timezone

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# LOADING THE MODEL
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Open the model
xgb_model = xgb.XGBClassifier()
xgb_model.load_model('models/xgb_model.json')

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


# opening the processed upcoming event data
testing_data = pd.read_csv('../preprocessing/data/moneyline_processed_data_upcoming.csv')
predictions_list = []
for index, row in testing_data.iterrows():
    result = row['result']
    clean_row = row.drop([
        'result', 'team', 'opponent', 'date',
        'team_spread', 'opp_spread', 'game_total',
        'points', 'opponent_points'
    ])
    clean_row_df = pd.DataFrame([clean_row])
    # convert the row date to YYYY-MM-DD
    row_date = pd.Timestamp(row['date'])
    row_date = row_date.strftime('%Y-%m-%d')

    new_pred = xgb_model.predict_proba(clean_row_df)[0, 1]
    odds = get_odds(new_pred)

    pred_result = 'no'
    if new_pred > 0.5 and result == 1:
        pred_result = 'yes'
    elif new_pred < 0.5 and result == 0: # if the prediction is less than 0.5 and the result is 0, then the prediction is correct
        pred_result = 'yes'

    predictions_list.append({
        'Team': row['team'],
        'Opp': row['opponent'],
        'Date': row['date'],
        'Odds 1': odds[0],
        'Odds 2': odds[1],
        'Book Odds 1': row['player_odds'],
        'Book Odds 2': row['opponent_odds'],
        'Result': pred_result,
    })

predictions = pd.DataFrame(predictions_list)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE PREDICTIONS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# writing the new predictions to the database
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

# insert the new predictions into the database
for index, row in predictions.iterrows():
    current_date = pd.Timestamp.now(tz='UTC')
    game_date = pd.Timestamp(row['Date'])
    try:
        if game_date < current_date:
            continue
    except Exception as e:
        game_date = pd.Timestamp(row['Date']).tz_localize('UTC')
    
    # converting the f1 name f2 name and date to a string to be used as an id for the row
    row_id = ''
    if row['Team'] < row['Opp']:
        row_id = row['Team'] + row['Opp'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
    else:
        row_id = row['Opp'] + row['Team'] + pd.to_datetime(row['Date']).strftime('%Y-%m-%d')

    team_name_short = get_name_from_team(row['Team']).split(' ')[-1]
    opp_name_short = get_name_from_team(row['Opp']).split(' ')[-1]

    t1_for_output = team_name_short + ' ' + ('✅' if row['Odds 1'] < 0 else '')
    t2_for_output = ('✅' if row['Odds 2'] < 0 else '') + ' ' + opp_name_short
    print('-' * 48)
    print(f"{'Teams:':<12} |   {t1_for_output:<12} vs {t2_for_output:>12}")
    print(f"{'Prediction:':<12} |   ({convert_american_to_probability(row['Odds 1'])*100:.0f}%) {str(row['Odds 1']):<7} |  {str(row['Odds 2']):>7} ({convert_american_to_probability(row['Odds 2'])*100:.0f}%)")
    print(f"{'Book Odds:':<12} |   {str(row['Book Odds 1']):<13} |  {str(row['Book Odds 2']):>12}")
    print('-' * 48)
    
    # get if there is a "pick"
    team1_pick = True if row['Odds 1'] < -120 and row['Book Odds 1'] < 0 else False
    team2_pick = True if row['Odds 2'] < -120 and row['Book Odds 2'] < 0 else False

    upsert_data = {
        'event_id': row_id,
        'moneyline_odds1': row['Odds 1'],
        'moneyline_odds2': row['Odds 2'],
        'created_by': '1398dacb-0258-4a0c-b74f-da86241ddff4',
        'is_team1_pick': team1_pick,
        'is_team2_pick': team2_pick,
        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        # get the event_odds row id (to see if it already exists)
        event_odds_row = supabase.table('event_odds').select('*').eq('event_id', row_id).eq('created_by', '1398dacb-0258-4a0c-b74f-da86241ddff4').execute()
        if event_odds_row.data:
            upsert_data['id'] = event_odds_row.data[0]['id']

        response = (
            supabase.table('event_odds')
            .upsert(upsert_data)
            .execute()
        )
    except Exception as e:
        print(f"Error inserting event odds for {row['Team']} vs {row['Opp']}: {e}")
        continue