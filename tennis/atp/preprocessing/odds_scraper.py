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

def fetch_tournament_links(session):
    """Fetch tennis ATP event tournament links"""
    print("Fetching tennis ATP event tournament links...")
    
    # Get the current event group ids for tennis
    response = session.get('https://sportsbook.draftkings.com/sports/tennis', timeout=30)

    # Extract the variable window.__INITIAL_STATE__
    tournaments = response.text.split('window.__INITIAL_STATE__ = ')[1].split('"helpPage":')[0]
    tournaments += '"helpPage": {"content": ""}}'
    tournaments = json.loads(tournaments)
    
    # Extract the tournament links
    tournament_links = []
    for tournament in tournaments['sports']['data']:
        if (tournament['displayName'] == 'Tennis'):
            for event_group in tournament['eventGroupInfos']:
                if (len(event_group['tags']) > 0):
                    tournament_links.append({
                        'urlName': event_group['urlName'],
                        'eventGroupId': event_group['eventGroupId']
                    })
    
    return tournament_links

def fetch_event_links_alternate(session, tournament_link):
    """Fetch tennis ATP event links alternate method"""
    print("Fetching tennis ATP event links alternate method...")

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

def fetch_event_links(session, tournament_link):
    """Fetch tennis ATP event links"""

    tournament_url = f'https://sportsbook.draftkings.com/leagues/tennis/{tournament_link["urlName"]}'
    
    # Get the current event group ids for tennis
    response = session.get(tournament_url, timeout=30)

    # Extract the variable window.__INITIAL_STATE__
    events = response.text.split('window.__INITIAL_STATE__ = ')[1].split('"helpPage":')[0]
    events += '"helpPage": {"content": ""}}'
    events = json.loads(events)

    # Extract the event links
    event_links = []
    try:
        all_events = events['eventGroups'][str(tournament_link['eventGroupId'])]['events']
    except:
        print(f"No events found for {tournament_link['urlName']}")
        return []
    
    for event in all_events.values():
        event_links.append({
            'urlName': event['urlName'],
            'eventId': event['eventId'],
            'eventGroupName': event['eventGroupName'],
            'name': event['name'],
            'startDate': event['startDate']
        })
    
    return event_links

def fetch_event_data(session, event_link):
    """Fetch tennis ATP event data"""

    try:
        # Get the current event group ids for tennis
        url = f"https://sportsbook.draftkings.com/event/{event_link['urlName']}/{event_link['eventId']}"
        response = session.get(url, timeout=30)

        # Extract the variable window.__INITIAL_STATE__
        events = response.text.split('window.__INITIAL_STATE__ = ')[1].split('"helpPage":')[0]
        events += '"helpPage": {"content": ""}}'
        events = json.loads(events)
        
        # Cross reference the marketIds from the markets data with the marketIds from the selections data
        markets = events['stadiumEventData']['markets']
        selections = events['stadiumEventData']['selections']

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
                    'tournament_name': event_link.get('eventGroupName'),
                    'event_name': event_link.get('name'),
                    'start_date': event_link.get('startDate'),
                    'event_year': event_link.get('startDate').split('-')[0],
                    'market_name': market.get('name'),
                    'true_odds': selection.get('trueOdds'),
                    'label': selection.get('label'),
                    'outcome_type': selection.get('outcomeType'),
                    'points': selection.get('points'),
                    **participant_data
                }
                extracted_data.append(data_point)
        
        return extracted_data
    except Exception as e:
        print(f"Error fetching event data for {event_link['name']}: {str(e)}")
        return []

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
                # add the scraped_at timestamp to the data and ensure its in UTC
                'scraped_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'market_id': market_id,
                'tournament_name': p1['tournament_name'],
                'event_name': p1['event_name'],
                'start_date': p1['start_date'],
                'market_name': p1['market_name'],
                'player1_name': p1['label'],
                'player1_odds': p1['true_odds'],
                'player1_points': p1['points'],
                'player1_outcome_type': p1['outcome_type'],
                'player1_label': p1['label'],
                'player2_name': p2['label'],
                'player2_odds': p2['true_odds'],
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
    csv_path = os.path.join(data_dir, 'tennis_odds.csv')

    # write the df_new_processed to the upcoming_tennis_odds.csv file
    df_new_processed.to_csv('data/upcoming_tennis_odds.csv', index=False)

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

    # Save the combined data
    df_combined.to_csv(csv_path, index=False)
    print(f"Successfully saved and merged data to {csv_path}")

def main():
    """Main function to run the script"""
    print("Starting Tennis Odds Scraper")
    
    # Create session
    session = create_session()
    
    try:
        # Step 1: Get tournament links
        tournament_links = fetch_tournament_links(session)
        print(f"Found {len(tournament_links)} tournament links")

        # Step 2: Get event links for each tournament
        event_links = []
        for tournament_link in tournament_links:
            print(f"Fetching event links for {tournament_link['urlName']}...")
            new_event_links = fetch_event_links(session, tournament_link)
            if len(new_event_links) > 0:
                event_links.extend(new_event_links)
            else:
                print(f"No event links found for {tournament_link['urlName']}, trying alternate method...")
                event_links.extend(fetch_event_links_alternate(session, tournament_link))

        # Step 3: Get event data for each event
        all_event_data = []
        for event_link in event_links:
            print(f"Fetching event data for {event_link['name']}...")
            try:
                event_data = fetch_event_data(session, event_link)
            except Exception as e:
                print(f"Error fetching event data for {event_link['name']}: {str(e)}")
                continue
            try:
                all_event_data.append(event_data)
            except Exception as e:
                print(f"Error appending event data for {event_link['name']}: {str(e)}")
                continue
        print(f"Found {len(all_event_data)} event data")

        # Step 4: Convert to a DataFrame, format, and save as a CSV
        format_and_save_data(all_event_data)
        
        print("\nScript completed successfully!")
        
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == '__main__':
    main()
