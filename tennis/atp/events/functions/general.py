import pandas as pd
import math

def convert_odds_to_american(odds):
  if odds >= 2:
      return math.floor((odds - 1) * 100)
  else:
      return math.floor(-100 / (odds - 1))

def get_image_url(country):
    country_images = {
        'AGO': 'https://cdn-icons-png.flaticon.com/512/197/197513.png',
        'ALG': 'https://cdn-icons-png.flaticon.com/512/197/197511.png',
        'ANT': 'https://cdn-icons-png.flaticon.com/512/197/197533.png',
        'ARG': 'https://cdn-icons-png.flaticon.com/512/197/197573.png',
        'AUS': 'https://cdn-icons-png.flaticon.com/512/197/197507.png',
        'AUT': 'https://cdn-icons-png.flaticon.com/512/197/197447.png',
        'AZE': 'https://cdn-icons-png.flaticon.com/512/197/197512.png',
        'BAH': 'https://cdn-icons-png.flaticon.com/128/197/197521.png',
        'BAR': 'https://cdn-icons-png.flaticon.com/512/197/197526.png',
        'BDI': 'https://cdn-icons-png.flaticon.com/512/197/197534.png',
        'BEL': 'https://cdn-icons-png.flaticon.com/512/197/197583.png',
        'BER': 'https://cdn-icons-png.flaticon.com/512/197/197523.png',
        'BIH': 'https://cdn-icons-png.flaticon.com/512/197/197524.png',
        'BLR': 'https://cdn-icons-png.flaticon.com/512/197/197527.png',
        'BOL': 'https://cdn-icons-png.flaticon.com/512/197/197504.png',
        'BRA': 'https://cdn-icons-png.flaticon.com/512/197/197386.png',
        'BUL': 'https://cdn-icons-png.flaticon.com/512/197/197502.png',
        'CAN': 'https://cdn-icons-png.flaticon.com/512/197/197430.png',
        'CHI': 'https://cdn-icons-png.flaticon.com/512/197/197586.png',
        'CHN': 'https://cdn-icons-png.flaticon.com/512/197/197375.png',
        'CIV': 'https://cdn-icons-png.flaticon.com/512/197/197391.png',
        'CMR': 'https://cdn-icons-png.flaticon.com/512/197/197531.png',
        'COL': 'https://cdn-icons-png.flaticon.com/512/197/197575.png',
        'CRC': 'https://cdn-icons-png.flaticon.com/512/197/197506.png',
        'CRO': 'https://cdn-icons-png.flaticon.com/512/197/197503.png',
        'CUW': 'https://cdn-icons-png.flaticon.com/512/197/197424.png',
        'CYP': 'https://cdn-icons-png.flaticon.com/512/197/197607.png',
        'CZE': 'https://cdn-icons-png.flaticon.com/512/197/197576.png',
        'DEN': 'https://cdn-icons-png.flaticon.com/512/197/197565.png',
        'DOM': 'https://cdn-icons-png.flaticon.com/512/197/197619.png',
        'ECU': 'https://cdn-icons-png.flaticon.com/512/197/197588.png',
        'EGY': 'https://cdn-icons-png.flaticon.com/512/197/197558.png',
        'ESP': 'https://cdn-icons-png.flaticon.com/512/197/197593.png',
        'EST': 'https://cdn-icons-png.flaticon.com/512/197/197379.png',
        'FIN': 'https://cdn-icons-png.flaticon.com/512/197/197585.png',
        'FRA': 'https://cdn-icons-png.flaticon.com/512/197/197560.png',
        'GBR': 'https://cdn-icons-png.flaticon.com/512/197/197374.png',
        'GEO': 'https://cdn-icons-png.flaticon.com/512/197/197380.png',
        'GER': 'https://cdn-icons-png.flaticon.com/512/197/197571.png',
        'GHA': 'https://cdn-icons-png.flaticon.com/512/197/197381.png',
        'GRE': 'https://cdn-icons-png.flaticon.com/512/197/197566.png',
        'GUA': 'https://cdn-icons-png.flaticon.com/512/197/197597.png',
        'GUD': 'https://cdn-icons-png.flaticon.com/512/197/197560.png',
        'HKG': 'https://cdn-icons-png.flaticon.com/512/197/197570.png',
        'HUN': 'https://cdn-icons-png.flaticon.com/512/197/197584.png',
        'INA': 'https://cdn-icons-png.flaticon.com/512/197/197559.png',
        'IND': 'https://cdn-icons-png.flaticon.com/512/197/197419.png',
        'IRI': 'https://cdn-icons-png.flaticon.com/512/197/197574.png',
        'IRL': 'https://cdn-icons-png.flaticon.com/512/197/197567.png',
        'ISR': 'https://cdn-icons-png.flaticon.com/512/197/197577.png',
        'ITA': 'https://cdn-icons-png.flaticon.com/512/197/197626.png',
        'JAM': 'https://cdn-icons-png.flaticon.com/512/197/197611.png',
        'JOR': 'https://cdn-icons-png.flaticon.com/512/197/197595.png',
        'JPN': 'https://cdn-icons-png.flaticon.com/512/197/197604.png',
        'KAZ': 'https://cdn-icons-png.flaticon.com/512/197/197603.png',
        'KOR': 'https://cdn-icons-png.flaticon.com/512/197/197582.png',
        'KUW': 'https://cdn-icons-png.flaticon.com/512/197/197459.png',
        'LAT': 'https://cdn-icons-png.flaticon.com/512/197/197605.png',
        'LBA': 'https://cdn-icons-png.flaticon.com/512/197/197411.png',
        'LBN': 'https://cdn-icons-png.flaticon.com/512/197/197629.png',
        'LIB': 'https://cdn-icons-png.flaticon.com/512/197/197418.png',
        'LTU': 'https://cdn-icons-png.flaticon.com/512/197/197612.png',
        'LUX': 'https://cdn-icons-png.flaticon.com/512/197/197614.png',
        'MAR': 'https://cdn-icons-png.flaticon.com/512/197/197551.png',
        'MAS': 'https://cdn-icons-png.flaticon.com/512/197/197581.png',
        'MDA': 'https://cdn-icons-png.flaticon.com/512/197/197405.png',
        'MEX': 'https://cdn-icons-png.flaticon.com/512/197/197397.png',
        'MKD': 'https://cdn-icons-png.flaticon.com/512/197/197413.png',
        'MLT': 'https://cdn-icons-png.flaticon.com/512/197/197625.png',
        'MNE': 'https://cdn-icons-png.flaticon.com/512/197/197406.png',
        'MON': 'https://cdn-icons-png.flaticon.com/512/197/197594.png',
        'MOZ': 'https://cdn-icons-png.flaticon.com/512/197/197631.png',
        'NAM': 'https://cdn-icons-png.flaticon.com/512/197/197617.png',
        'NED': 'https://cdn-icons-png.flaticon.com/512/197/197441.png',
        'NGR': 'https://cdn-icons-png.flaticon.com/512/197/197627.png',
        'NMI': 'https://cdn-icons-png.flaticon.com/512/197/197465.png',
        'NOR': 'https://cdn-icons-png.flaticon.com/512/197/197579.png',
        'NZL': 'https://cdn-icons-png.flaticon.com/512/197/197589.png',
        'PAR': 'https://cdn-icons-png.flaticon.com/512/197/197376.png',
        'PER': 'https://cdn-icons-png.flaticon.com/512/197/197563.png',
        'PHI': 'https://cdn-icons-png.flaticon.com/512/197/197561.png',
        'POL': 'https://cdn-icons-png.flaticon.com/512/197/197529.png',
        'POR': 'https://cdn-icons-png.flaticon.com/512/197/197463.png',
        'PRY': 'https://cdn-icons-png.flaticon.com/512/197/197376.png',
        'PUR': 'https://cdn-icons-png.flaticon.com/512/197/197632.png',
        'ROU': 'https://cdn-icons-png.flaticon.com/512/197/197587.png',
        'RSA': 'https://cdn-icons-png.flaticon.com/512/197/197562.png',
        'RUS': 'https://cdn-icons-png.flaticon.com/512/197/197408.png',
        'SEN': 'https://cdn-icons-png.flaticon.com/512/197/197377.png',
        'SGP': 'https://cdn-icons-png.flaticon.com/512/197/197496.png',
        'SLO': 'https://cdn-icons-png.flaticon.com/512/197/197633.png',
        'SRB': 'https://cdn-icons-png.flaticon.com/512/197/197602.png',
        'SUI': 'https://cdn-icons-png.flaticon.com/512/197/197540.png',
        'SVK': 'https://cdn-icons-png.flaticon.com/512/197/197592.png',
        'SWE': 'https://cdn-icons-png.flaticon.com/512/197/197564.png',
        'SYR': 'https://cdn-icons-png.flaticon.com/512/197/197598.png',
        'THA': 'https://cdn-icons-png.flaticon.com/512/197/197452.png',
        'TOG': 'https://cdn-icons-png.flaticon.com/512/197/197443.png',
        'TPE': 'https://cdn-icons-png.flaticon.com/512/197/197557.png',
        'TUN': 'https://cdn-icons-png.flaticon.com/512/197/197624.png',
        'TUR': 'https://cdn-icons-png.flaticon.com/512/197/197518.png',
        'UKR': 'https://cdn-icons-png.flaticon.com/512/197/197572.png',
        'URU': 'https://cdn-icons-png.flaticon.com/512/197/197599.png',
        'USA': 'https://cdn-icons-png.flaticon.com/512/197/197484.png',
        'UZB': 'https://cdn-icons-png.flaticon.com/512/197/197416.png',
        'VEN': 'https://cdn-icons-png.flaticon.com/512/197/197580.png',
        'VIE': 'https://cdn-icons-png.flaticon.com/512/197/197473.png',
        'ZIM': 'https://cdn-icons-png.flaticon.com/512/197/197394.png',
    }

    return country_images[country]

def add_total_games_odds_to_processed_data(processed_data):
    # adding the total games odds to the processed data
    tennis_odds = pd.read_csv('../preprocessing/data/upcoming_tennis_odds.csv')

    # remove any rows where the market_name is not Total Games
    tennis_odds = tennis_odds[tennis_odds['market_name'].str.contains('Player Total Games Won', na=False)]
    
    count = 0
    for index, row in processed_data.iterrows():
        # find the row in odds_data where the player and opponent are the same as the row in processed_data
        player_match = tennis_odds['event_name'].apply(
            lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0] == row['player'] and x.split(' vs ')[1] == row['opponent']
        )
        opponent_match = tennis_odds['event_name'].apply(
            lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0] == row['opponent'] and x.split(' vs ')[1] == row['player']
        )
        odds_row = tennis_odds[player_match | opponent_match]

        if odds_row.empty:
            continue

        # find the odds for the player total games won market
        player_odds_row = odds_row[odds_row['market_name'].str.contains(row['player'])]
        opponent_odds_row = odds_row[odds_row['market_name'].str.contains(row['opponent'])]

        if player_odds_row.empty:
            continue
        
        if opponent_odds_row.empty:
            continue

        # if there is more than one line for the player or opponent, then we need to find the line with the smallest difference between the player1_odds and player2_odds
        player_odds_row = player_odds_row.loc[
            [(player_odds_row['player1_odds'] - player_odds_row['player2_odds']).abs().idxmin()]
        ].reset_index(drop=True)

        opponent_odds_row = opponent_odds_row.loc[
            [(opponent_odds_row['player1_odds'] - opponent_odds_row['player2_odds']).abs().idxmin()]
        ].reset_index(drop=True)

        # get the year of the tournament from the processed_data
        date = pd.to_datetime(row['date'], utc=True)
        year = date.year

        # filter out player_odds_row and opponent_odds_row where the year is not the same as the year in the processed_data
        player_odds_row = player_odds_row[player_odds_row['start_date'].str.split('-').str[0] == str(year)]
        opponent_odds_row = opponent_odds_row[opponent_odds_row['start_date'].str.split('-').str[0] == str(year)]

        if player_odds_row.empty:
            continue
        
        if opponent_odds_row.empty:
            continue

        # get the name of the tournament from the processed_data
        tournament = row['tournament']

        # filter out player_odds_row and opponent_odds_row where the tournament is not the same as the tournament in the processed_data
        player_odds_row = player_odds_row[
            (player_odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (player_odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in player_odds_row['tournament_name'])
        ]
        opponent_odds_row = opponent_odds_row[
            (opponent_odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (opponent_odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in opponent_odds_row['tournament_name'])
        ]

        # TODO: and the year and tournament are the same --> for now just matching on year and month
        if player_odds_row.empty:
            continue

        if opponent_odds_row.empty:
            continue

        # convert the odds and add them to the row in processed_data
        processed_data.loc[index, 'player_total_games_odds_over'] = convert_odds_to_american(player_odds_row['player1_odds'].values[0])
        processed_data.loc[index, 'player_total_games_odds_under'] = convert_odds_to_american(player_odds_row['player2_odds'].values[0])
        processed_data.loc[index, 'player_total_games_odds_line'] = player_odds_row['player1_points'].values[0]
        processed_data.loc[index, 'opponent_total_games_odds_over'] = convert_odds_to_american(opponent_odds_row['player1_odds'].values[0])
        processed_data.loc[index, 'opponent_total_games_odds_under'] = convert_odds_to_american(opponent_odds_row['player2_odds'].values[0])
        processed_data.loc[index, 'opponent_total_games_odds_line'] = opponent_odds_row['player1_points'].values[0]

        count += 1
    
    print(f'Found total games odds: {count}')
    return processed_data

def add_total_sets_odds_to_processed_data(processed_data):
    # adding the total games odds to the processed data
    tennis_odds = pd.read_csv('../preprocessing/data/upcoming_tennis_odds.csv')

    # remove any rows where the market_name is not Total Games
    tennis_odds = tennis_odds[tennis_odds['market_name'].str.contains('Total Sets', na=False)]

    # create player_name and opponent_name column to odds_data
    tennis_odds['player_name'] = tennis_odds['event_name'].apply(
        lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[0]
    )
    tennis_odds['opponent_name'] = tennis_odds['event_name'].apply(
        lambda x: isinstance(x, str) and ' vs ' in x and x.split(' vs ')[1]
    )
    
    count = 0
    for index, row in processed_data.iterrows():
        team1_match = (tennis_odds['player_name'] == row['player']) & (tennis_odds['opponent_name'] == row['opponent'])
        team2_match = (tennis_odds['player_name'] == row['opponent']) & (tennis_odds['opponent_name'] == row['player'])
        odds_row = tennis_odds[team1_match | team2_match]

        if odds_row.empty:
            continue

        # get the year of the tournament from the processed_data
        date = pd.to_datetime(row['date'], utc=True)
        year = date.year

        # filter out player_odds_row and opponent_odds_row where the year is not the same as the year in the processed_data
        odds_row = odds_row[odds_row['start_date'].str.split('-').str[0] == str(year)]

        if odds_row.empty:
            continue

        # get the name of the tournament from the processed_data
        tournament = row['tournament']

        # filter out player_odds_row and opponent_odds_row where the tournament is not the same as the tournament in the processed_data
        odds_row = odds_row[
            (odds_row['tournament_name'].str.split(' - ').str[-1].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].str.split(' (', regex=False).str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].str.split(' ').str[0].apply(lambda x: pd.notna(x) and x in tournament)) |
            (odds_row['tournament_name'].apply(lambda x: pd.notna(x) and x in tournament)) |
            (tournament in odds_row['tournament_name'])
        ]

        if odds_row.empty:
            continue

        # convert the odds and add them to the row in processed_data
        processed_data.loc[index, 'total_sets_odds_over'] = convert_odds_to_american(odds_row['player1_odds'].values[0])
        processed_data.loc[index, 'total_sets_odds_under'] = convert_odds_to_american(odds_row['player2_odds'].values[0])
        processed_data.loc[index, 'total_sets_line'] = odds_row['player1_points'].values[0]

        count += 1
    
    print(f'Found total sets odds: {count}')
    return processed_data