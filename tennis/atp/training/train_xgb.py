import pandas as pd
import xgboost as xgb
from supabase import create_client
from dotenv import load_dotenv
from functions.general import get_image_url
import os
from datetime import datetime, timezone

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# LOADING THE MODEL
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# open the model
xgb_model = xgb.XGBClassifier()
xgb_model.load_model('models/(CUR)3160-profit-classic.bin')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# PREDICTING THE WINNER
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_odds(win_prob):
    if win_prob < 0.5:
        fighterOdds = round(100 / win_prob - 100)
        opponentOdds = round(1 / (1 / (1 - win_prob) - 1) * 100)
        return [fighterOdds, -1 * opponentOdds]
    else:
        fighterOdds = round(1 / (1 / win_prob - 1) * 100)
        opponentOdds = round(100 / (1 - win_prob) - 100)
        return [-1 * fighterOdds,opponentOdds]

def convert_american_to_probability(odds):
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    else:
        return 100 / (abs(odds) + 100)
    
# Prediction function
def predict_game(row, model):
    # Ensure the row is a DataFrame
    if not isinstance(row, pd.DataFrame):
        row = pd.DataFrame([row])
    
    # Make prediction
    prediction = model.predict_proba(row)[0, 1]  # Probability of class 1
    return prediction

win_count = 0
counter = 0
predictions = []
prediction_data = pd.read_csv('../preprocessing/data/new_processed_data.csv')

for index, row in prediction_data.iterrows():
    # Create a single-row DataFrame
    clean_row = row.drop(['player', 'opponent', 'date', 'result', 'player_odds', 'opponent_odds', 'player_rank', 'opponent_rank', 'tournament', 'player_country', 'opponent_country', 'player_total_games_won', 'opponent_total_games_won'])
    clean_row_df = pd.DataFrame([clean_row])
    
    new_pred = predict_game(clean_row, xgb_model)
    predicted_odds = get_odds(new_pred)

    # adding the prediction to the dataframe with concat
    predictions.append({
        'player': row['player'],
        'opponent': row['opponent'],
        'date': row['date'],
        'player_odds': predicted_odds[0],
        'opponent_odds': predicted_odds[1],
        'book_odds1': row['player_odds'],
        'book_odds2': row['opponent_odds'],
        'player_country': row['player_country'],
        'opponent_country': row['opponent_country'],
        'tournament': row['tournament']
    })
predictions = pd.DataFrame(predictions)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# WRITING THE PREDICTIONS TO THE DATABASE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# writing the new predictions to the database
load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

# insert the new predictions into the database
for index, row in predictions.iterrows():
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

    f1_for_output = row['player'].upper() + ' ' + ('✅' if row['player_odds'] < 0 else '')
    f2_for_output = ('✅' if row['opponent_odds'] < 0 else '') + ' ' + row['opponent'].upper()
    print(f"{'Teams:':<12} |   {f1_for_output:<25} vs {f2_for_output:>25}")
    print(f"{'Prediction:':<12} |   ({convert_american_to_probability(row['player_odds'])*100:.0f}%) {str(row['player_odds']):<20} |  {str(row['opponent_odds']):>20} ({convert_american_to_probability(row['player_odds'])*100:.0f}%)")
    print(f"{'Book Odds:':<12} |   {str(row['book_odds1']):<26} |  {str(row['book_odds2']):>26}")
    print('-' * 70)

    # get if there is a "pick"
    team1_pick = True if row['player_odds'] < -130 and int(row['book_odds1']) > -150 and int(row['book_odds1']) < 0 else False
    team2_pick = True if row['opponent_odds'] < -130 and int(row['book_odds2']) > -150 and int(row['book_odds2']) < 0 else False

    upsert_data = {
        'event_id': row_id,
        'moneyline_odds1': row['player_odds'],
        'moneyline_odds2': row['opponent_odds'],
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