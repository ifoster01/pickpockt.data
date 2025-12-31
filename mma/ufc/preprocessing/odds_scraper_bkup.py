import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import csv
from datetime import datetime, timezone
import pandas as pd
import os

def create_session():
    """Create a session with retry strategy and browser-like headers"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Headers that mimic a real browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://sportsbook.draftkings.com/',
        'Origin': 'https://sportsbook.draftkings.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'DNT': '1',
        'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"macOS"'
    })
    
    return session

def fetch_with_retry(session, url, timeout=30):
    """Fetch data from URL with retry logic and error handling"""
    try:
        # Add a small delay before the request to avoid rate limiting
        time.sleep(1)
        
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {str(e)}")
        if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
            print(f"Response text: {e.response.text}")
        return None

def fetch_event_links_alternate(session, tournament_link):
    """Fetch UFC event links alternate method"""
    print("Fetching UFC event links alternate method...")

    league_url = f'https://sportsbook-nash.draftkings.com/sites/US-NY-SB/api/sportscontent/controldata/home/primaryMarkets/v1/markets?eventsQuery=%24filter%3DleagueId%20eq%20%27{tournament_link['eventGroupId']}%27&marketsQuery=%24filter%3Dtags%2Fany(t%3A%20t%20eq%20%27PrimaryMarket%27)&top=25&include=Events&entity=events&isBatchable=true'
    response = session.get(league_url, timeout=30)

    events = response.json()
    
    # Extract the event links
    event_links = []
    try:
        all_events = events['events']
    except:
        print(f"No events found for {tournament_link['urlName']}")
        return []
    
    for event in all_events:
        event_links.append({
            'urlName': event['name'].replace(' ', '-').lower(),
            'eventId': event['id'],
            'eventGroupName': tournament_link['urlName'].replace('-', ' ').title().replace('Atp', 'ATP'),
            'name': event['name'],
            'startDate': event['startEventDate']
        })
    
    return event_links

def fetch_fight_links_alternate(session):
    """Fetch UFC event links alternate method"""
    print("Fetching UFC event links alternate method...")

    league_url = f'https://sportsbook-nash.draftkings.com/sites/US-NY-SB/api/sportscontent/controldata/home/primaryMarkets/v1/markets?eventsQuery=%24filter%3DleagueId%20eq%20%279034%27&marketsQuery=%24filter%3Dtags%2Fany(t%3A%20t%20eq%20%27PrimaryMarket%27)&top=25&include=Events&entity=events&isBatchable=true'
    response = session.get(league_url, timeout=30)

    events = response.json()
    
    # Extract the event links
    event_links = []
    try:
        all_events = events['events']
    except:
        print(f"No events found for UFC")
        return []
    
    for event in all_events:
        event_links.append({
            'urlName': event['name'].replace(' ', '-').lower(),
            'eventId': event['id'],
            'eventGroupName': 'UFC',
            'name': event['name'],
            'startDate': event['startEventDate']
        })
    
    return event_links

def fetch_fight_links(session):
    """Fetch UFC fight links"""
    print("Fetching UFC fight links...")
    
    # Get the current event group ids for tennis
    response = session.get('https://sportsbook.draftkings.com/leagues/mma/ufc', timeout=30)

    # Extract the variable window.__INITIAL_STATE__
    fights = response.text.split('window.__INITIAL_STATE__ = ')[1].split('"helpPage":')[0]
    fights += '"helpPage": {"content": ""}}'
    fights = json.loads(fights)

    # Extract the event links
    fight_links = []
    try:
        all_fights = fights['eventGroups']['9034']['events']
    except:
        print(f"No events found for UFC")
        return []
    
    for fight in all_fights.values():
        fight_links.append({
            'urlName': fight['urlName'],
            'eventId': fight['eventId'],
            'eventGroupName': fight['eventGroupName'],
            'name': fight['name'],
            'startDate': fight['startDate']
        })
    
    return fight_links

def fetch_fight_data(session, fight_link):
    """Fetch UFC fight data"""

    # for moneyline, spread, and total markets
    SUBCATEGORY_ID = '13025'

    # Get the current event group ids for ufc
    url = f"https://sportsbook-nash.draftkings.com/sites/US-NY-SB/api/sportscontent/controldata/event/eventSubcategory/v1/markets?isBatchable=false&templateVars={fight_link.get('eventId')}&marketsQuery=%24filter%3DeventId%20eq%20%27{fight_link.get('eventId')}%27%20AND%20clientMetadata%2FsubCategoryId%20eq%20%27{SUBCATEGORY_ID}%27%20AND%20tags%2Fall%28t%3A%20t%20ne%20%27SportcastBetBuilder%27%29&include=MarketSplits&entity=markets"
    response = session.get(url, timeout=30)
    events = response.json()

    # Cross reference the marketIds from the markets data with the marketIds from the selections data
    markets = events['markets']
    selections = events['selections']

    # Create a lookup for markets by ID
    markets_by_id = {market['id']: market for market in markets}

    extracted_data = []
    for selection in selections:
        market_id = selection.get('marketId')
        market = markets_by_id.get(market_id)

        participant_data = {}
        if selection.get('participants'):
            for i in range(len(selection.get('participants'))):
                try:
                    participant_data[f'participants-name-{i}'] = selection.get('participants')[i]['name']
                except:
                    print(f"No participant names found for market {market_id}")
                try:
                    participant_data[f'participants-type-{i}'] = selection.get('participants')[i]['type']
                except:
                    print(f"No participant types found for market {market_id}")
                try:
                    participant_data[f'participants-venueRole-{i}'] = selection.get('participants')[i]['venueRole']
                except:
                    print(f"No participant venue roles found for market {market_id}")

        if market:
            data_point = {
                'market_id': market.get('id'),
                'tournament_name': fight_link.get('eventGroupName'),
                'event_name': fight_link.get('name'),
                'start_date': fight_link.get('startDate'),
                'event_year': fight_link.get('startDate').split('-')[0],
                'market_name': market.get('name'),
                'true_odds': selection.get('trueOdds'),
                'label': selection.get('label'),
                'outcome_type': selection.get('outcomeType'),
                'points': selection.get('points'),
                **participant_data
            }
            extracted_data.append(data_point)
    
    return extracted_data

def convert_odds_to_american(odds):
    if odds >= 2:
        return int((odds - 1) * 100)
    else:
        return int(-100 / (odds - 1))

def format_and_save_data(all_event_data):
    """Format and save the data to a CSV file"""
    if not all_event_data:
        print("No event data to process.")
        return

    # Flatten the list of lists into a single list of dictionaries
    flat_data = [item for sublist in all_event_data for item in sublist if item]
    if not flat_data:
        print("No data points to process.")
        return

    # Create a DataFrame from the newly scraped data
    df_new = pd.DataFrame(flat_data)
    
    # Identify markets with exactly two outcomes
    market_counts = df_new['market_id'].value_counts()
    two_outcome_markets = market_counts[market_counts == 2].index
    two_way_df = df_new[df_new['market_id'].isin(two_outcome_markets)].copy()

    processed_data = []
    if not two_way_df.empty:
        # Group by market_id and pivot the player data
        for market_id, group in two_way_df.groupby('market_id'):
            p1 = group.iloc[0]
            p2 = group.iloc[1]
            
            processed_data.append({
                'scraped_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'market_id': market_id,
                'tournament_name': p1['tournament_name'],
                'event_name': p1['event_name'],
                'start_date': p1['start_date'],
                'market_name': p1['market_name'],
                'player1_name': p1['label'],
                'player1_odds': p1['true_odds'],
                'player1_american_odds': convert_odds_to_american(p1['true_odds']),
                'player1_points': p1['points'],
                'player1_outcome_type': p1['outcome_type'],
                'player1_label': p1['label'],
                'player2_name': p2['label'],
                'player2_odds': p2['true_odds'],
                'player2_american_odds': convert_odds_to_american(p2['true_odds']),
                'player2_points': p2['points'],
                'player2_outcome_type': p2['outcome_type'],
                'player2_label': p2['label']
            })
        
        df_new_processed = pd.DataFrame(processed_data)
    else:
        print("No new two-way markets found to process.")
        df_new_processed = pd.DataFrame()

    # Define the path to the data directory and the CSV file
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    csv_path = os.path.join(data_dir, 'ufc_odds.csv')

    # write the df_new_processed to the upcoming_ufc_odds.csv file
    df_new_processed.to_csv('data/upcoming_ufc_odds.csv', index=False)

    # Load existing data
    if os.path.exists(csv_path):
        try:
            df_existing = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            df_existing = pd.DataFrame()
    else:
        df_existing = pd.DataFrame()

    # Merge new data with existing data
    if not df_new_processed.empty:
        df_combined = pd.concat([df_existing, df_new_processed], ignore_index=True)
        df_combined.drop_duplicates(subset=['market_id'], keep='last', inplace=True)
    else:
        df_combined = df_existing

    # order the dataframe by start_date
    df_combined = df_combined.sort_values(by='start_date', ascending=False)

    # Save the combined data
    df_combined.to_csv(csv_path, index=False)
    print(f"Successfully saved and merged data to {csv_path}")

def main():
    """Main function to run the script"""
    print("Starting UFC Odds Scraper")
    
    # Create session
    session = create_session()
    
    try:
        # Step 1: Get fight links
        fight_links = fetch_fight_links(session)
        print(f"Found {len(fight_links)} fight links")
        print(fight_links)
        if len(fight_links) == 0:
            print("No fight links found, trying alternate method...")
            fight_links = fetch_fight_links_alternate(session)
            print(f"Found {len(fight_links)} fight links")

        # Step 2: Get event data for each event
        all_fight_data = []
        for fight_link in fight_links:
            try:
                print(f"Fetching fight data for {fight_link['name']}...")
                fight_data = fetch_fight_data(session, fight_link)
                all_fight_data.append(fight_data)
            except Exception as e:
                print(f"Error fetching fight data for {fight_link['name']}: {str(e)}")
                continue
        print(f"Found {len(all_fight_data)} fight data")

        # Step 3: Format and save the data
        format_and_save_data(all_fight_data)

        print("\nScript completed successfully!")
        
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == '__main__':
    main()