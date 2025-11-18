import requests, json, time, re, string
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random
import time
import json
import sys
from datetime import datetime, timezone, timedelta

# database imports
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)


class ATPMatchDataScraper:
    def __init__(self, new_matches=False):
        print('Initializing ATPMatchDataScraper')
        if new_matches:
            self.player_links_list = []
        else:
            self.player_links_list = self.create_player_name_map()
        self.session = self.create_session_with_retries()
        self.date_map = {
            'Jan': '01',
            'Feb': '02',
            'Mar': '03',
            'Apr': '04',
            'May': '05',
            'Jun': '06',
            'Jul': '07',
            'Aug': '08',
            'Sep': '09',
            'Oct': '10',
            'Nov': '11',
            'Dec': '12'
        }
    
    def get_country_from_name(self, country):
        country_names = {
            'AGO': ['Angola'],
            'ALG': ['Algeria'],
            'ANT': ['Antigua and Barbuda'],
            'ARG': ['Argentina'],
            'AUS': ['Australia'],
            'AUT': ['Austria'],
            'AZE': ['Azerbaijan'],
            'BAH': ['Bahamas'],
            'BAR': ['Barbados'],
            'BDI': ['Burundi'],
            'BEL': ['Belgium'],
            'BER': ['Belarus'],
            'BIH': ['Bosnia and Herzegovina'],
            'BLR': ['Belarus'],
            'BOL': ['Bolivia'],
            'BRA': ['Brazil'],
            'BUL': ['Bulgaria'],
            'CAN': ['Canada'],
            'CHI': ['Chile'],
            'CHN': ['China'],
            'CIV': ['Côte d\'Ivoire'],
            'CMR': ['Cameroon'],
            'COL': ['Colombia'],
            'CRC': ['Costa Rica'],
            'CRO': ['Croatia'],
            'CUW': ['Curaçao'],
            'CYP': ['Cyprus'],
            'CZE': ['Czech Republic', 'Czechia'],
            'DEN': ['Denmark'],
            'DOM': ['Dominican Republic'],
            'ECU': ['Ecuador'],
            'EGY': ['Egypt'],
            'ESP': ['Spain'],
            'EST': ['Estonia'],
            'FIN': ['Finland'],
            'FRA': ['France'],
            'GBR': ['United Kingdom'],
            'GEO': ['Georgia'],
            'GER': ['Germany'],
            'GHA': ['Ghana'],
            'GRE': ['Greece'],
            'GUA': ['Guatemala'],
            'GUD': ['Guatemala'],
            'HKG': ['Hong Kong'],
            'HUN': ['Hungary'],
            'INA': ['Indonesia'],
            'IND': ['India'],
            'IRI': ['Iran'],
            'IRL': ['Ireland'],
            'ISR': ['Israel'],
            'ITA': ['Italy'],
            'JAM': ['Jamaica'],
            'JOR': ['Jordan'],
            'JPN': ['Japan'],
            'KAZ': ['Kazakhstan'],
            'KOR': ['South Korea'],
            'KUW': ['Kuwait'],
            'LAT': ['Latvia'],
            'LBA': ['Libya'],
            'LBN': ['Lebanon'],
            'LIB': ['Libya'],
            'LTU': ['Lithuania'],
            'LUX': ['Luxembourg'],
            'MAR': ['Morocco'],
            'MAS': ['Malaysia'],
            'MDA': ['Moldova'],
            'MEX': ['Mexico'],
            'MKD': ['North Macedonia'],
            'MLT': ['Malta'],
            'MNE': ['Montenegro'],
            'MON': ['Monaco'],
            'MOZ': ['Mozambique'],
            'NAM': ['Namibia'],
            'NED': ['Netherlands'],
            'NGR': ['Nigeria'],
            'NMI': ['Northern Mariana Islands'],
            'NOR': ['Norway'],
            'NZL': ['New Zealand'],
            'PAR': ['Paraguay'],
            'PER': ['Peru'],
            'PHI': ['Philippines'],
            'POL': ['Poland'],
            'POR': ['Portugal'],
            'PRY': ['Paraguay'],
            'PUR': ['Puerto Rico'],
            'ROU': ['Romania'],
            'RSA': ['South Africa'],
            'RUS': ['Russia'],
            'SEN': ['Senegal'],
            'SGP': ['Singapore'],
            'SLO': ['Slovenia'],
            'SRB': ['Serbia'],
            'SUI': ['Switzerland'],
            'SVK': ['Slovakia'],
            'SWE': ['Sweden'],
            'SYR': ['Syria'],
            'THA': ['Thailand'],
            'TOG': ['Togo'],
            'TPE': ['Taiwan'],
            'TUN': ['Tunisia'],
            'TUR': ['Turkey'],
            'UKR': ['Ukraine'],
            'URU': ['Uruguay'],
            'USA': ['United States'],
            'UZB': ['Uzbekistan'],
            'VEN': ['Venezuela'],
            'VIE': ['Vietnam'],
            'ZIM': ['Zimbabwe'],
        }

        for key, value in country_names.items():
            for v in value:
                if country in v:
                    return key
        return country

    def create_session_with_retries(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def create_player_name_map(self):
        # Create headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        try:
            # Create a session object
            session = requests.Session()
            
            # Get the links for the current tournaments
            url = 'https://tennisabstract.com/reports/atpRankings.html'
            page = session.get(url, headers=headers, timeout=10)
            
            # Raise an error for bad status codes
            page.raise_for_status()

            # extract string from the file at the second <table tag
            table_start = page.text.find('<table id="reportable"')
            table_end = page.text.find('</table>')
            table_content = page.text[table_start:table_end + len('</table>')]

            soup = BeautifulSoup(table_content, 'html.parser')
            player_rows = soup.find_all('tr')
            player_links_list = []
            for row in player_rows:
                player = row.find('a')
                if not player:
                    continue
                player_name = player.text.replace('\xa0', ' ')
                player_link = f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={player_name.replace(' ', '')}&f=ACareerqq'
                player_links_list.append((player_name, player_link))

            return player_links_list

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def scrape_full_match_history(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        player_matches = []
        count = 0
        for player_name, player_link in self.player_links_list:
            print(f'Scraping match history for {player_name}')
            matches = []
            try:
                # Add a random delay between requests (2-4 seconds)
                time.sleep(random.uniform(2, 4))

                page = self.session.get(player_link, headers=headers, timeout=10)
                page.raise_for_status()

                soup = BeautifulSoup(page.content, 'html.parser')

                matches = self.parse_matchmx(str(soup), player_name)
                player_matches.extend(matches)
                print()
                print(f'Successfully scraped {len(matches)} matches for {player_name}')
                print()
                count += 1
            except requests.RequestException as e:
                print(f"Error fetching data for {player_name}: {e}")
                if e.response is not None and e.response.status_code == 429:
                    # If rate limited, wait for a longer time
                    wait_time = int(e.response.headers.get('Retry-After', 60))
                    print(f"Rate limited. Waiting for {wait_time} seconds...")
                    time.sleep(wait_time)
                continue
            
            # print progress at 10% increments
            if count % (len(self.player_links_list) / 10) == 0:
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print(f'{count / len(self.player_links_list) * 100}% complete')
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

        return player_matches

    def tournament_link_construction(self):
        # open the tennis_odds.csv file
        tennis_odds = pd.read_csv('data/upcoming_tennis_odds.csv')
        # get the unique tournament names
        tournament_names = tennis_odds['tournament_name'].unique()
        # get only tournament names that include 'ATP'
        tournament_names = [name for name in tournament_names if 'ATP' in name]
        # remove any tournament names that include 'Quals.' or 'Doubles'
        tournament_names = [name for name in tournament_names if 'Quals.' not in name and 'Doubles' not in name]
        # convert the tournament names from 'ATP - <tournament name>' to ATP<tournament name>
        tournament_names = [name.replace('ATP - ', 'ATP') for name in tournament_names]
        # remove any spaces from the tournament names
        tournament_names = [name.replace(' ', '') for name in tournament_names]
        # get the current year
        current_year = datetime.now().year
        
        # construct the tournament links
        tournament_links = [f'<a href="https://www.tennisabstract.com/current/{current_year}{name}.html">Results and Forecasts</a>' for name in tournament_names]
        # convert each link to a beautiful soup object
        tournament_links = [BeautifulSoup(link, 'html.parser').find('a') for link in tournament_links]
        
        return tournament_links
    
    def scrape_match_links_fallback(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        # Get the match links from the atptour website
        tournament_link_data = requests.get('https://www.atptour.com/en/-/tournaments/calendar/tour', headers=headers, timeout=10)
        tournament_link_data.raise_for_status()
        tournament_link_data = tournament_link_data.json()['TournamentDates']
        
        print(f'scraping {len(tournament_link_data)} tournaments for links')
        tournament_links = []
        for month_data in tournament_link_data:
            for tournament in month_data['Tournaments']:
                tournament_links.append({
                    'tournament_link': f"https://www.atptour.com{tournament['ScoresUrl']}",
                    'past': 'archive' in tournament['ScoresUrl']
                })
        
        print(f'scraping {len(tournament_links)} tournaments for completed match links')
        tournament_completed_match_links = []
        current_year = datetime.now().year
        for tournament_link in tournament_links:
            if tournament_link['past']:
                continue

            page = self.session.get(tournament_link['tournament_link'])
            page.raise_for_status()
            soup = BeautifulSoup(page.content, 'html.parser')
            # get every 'match' component
            matches = soup.find_all('div', class_='match')
            for match in matches:
                # get the match links
                match_links = match.find_all('a', href=True)
                for match_link in match_links:
                    if not 'match-stats' in match_link['href']:
                        continue
                    tournament_completed_match_links.append({
                        'match_stats_link': f'https://www.atptour.com/-/Hawkeye/MatchStats/Complete/{match_link["href"].split("/")[-3]}/{match_link["href"].split("/")[-2]}/{match_link["href"].split("/")[-1]}',
                        'tournament': tournament_link['tournament_link'].split('/')[-2],
                    })
        print(f'found {len(tournament_completed_match_links)} completed match links')

        tournament_upcoming_match_links = []
        for tournament_link in tournament_links:
            if tournament_link['past']:
                continue
            # getting the tournament data
            formatted_tournament_link = f'https://www.atptour.com/en/-/tournaments/profile/{tournament_link["tournament_link"].split("/")[-2]}/overview'
            tournament_data = self.scrape_tournament_data_fallback(formatted_tournament_link)
            
            link = tournament_link['tournament_link'].replace('results', 'daily-schedule')
            page = self.session.get(link)
            page.raise_for_status()
            soup = BeautifulSoup(page.content, 'html.parser')
            
            # get all the tournament days
            day_options = soup.find('select', id='matchDate-filter')
            tournament_day_links = []
            for day_option in day_options:
                if day_option.text.strip() == '':
                    continue
                tournament_day_links.append(f'{link}?day={day_option.text.strip().split(" ")[1]}')
            
            for day_link in tournament_day_links:
                page = self.session.get(day_link)
                page.raise_for_status()
                soup = BeautifulSoup(page.content, 'html.parser')
                
                # get the tournament day
                current_year = datetime.now().year
                tournament_day_text = soup.find('div', class_='tournament-day').text.strip().split(',')[1].strip()
                tournament_day_full = f'{tournament_day_text}, {current_year}'
                # Try parsing with full month name first, then fallback to abbreviated month name
                try:
                    tournament_day = datetime.strptime(tournament_day_full, '%d %B, %Y')
                except ValueError:
                    tournament_day = datetime.strptime(tournament_day_full, '%d %b, %Y')

                # get every upcoming match
                soup = soup.find('div', class_='content')
                upcoming_match_schedules = soup.find_all('div', class_='schedule')
                for match_schedule in upcoming_match_schedules:
                    try:
                        # get the match h2h link
                        match_h2h_link = match_schedule.find('div', class_='schedule-cta').find('a')
                        if match_h2h_link:
                            match_h2h_link = match_h2h_link['href']
                        else:
                            continue
                        # get the match round
                        match_round = match_schedule.find('div', class_='schedule-type').text.strip()
                        # get the match participants
                        upcoming_match = match_schedule.find('div', class_='schedule-players')
                        player = upcoming_match.find('div', class_='player')
                        player_name = player.find_all('div', class_='name')
                        if len(player_name) != 1:
                            continue

                        p1_id = match_h2h_link.split('/')[-2]
                        p2_id = match_h2h_link.split('/')[-1]

                        tournament_upcoming_match_links.append({
                            'round': match_round,
                            'match_surface': tournament_data['Surface'],
                            'tournament': tournament_data['Name'],
                            'match_h2h_link': f'https://www.atptour.com/en/-/www/h2h/{p1_id}/{p2_id}',
                            'match_date': tournament_day.strftime('%Y-%m-%d')
                        })
                    except:
                        continue
            break

        print(json.dumps(tournament_completed_match_links, indent=4))
        
        return [tournament_completed_match_links, tournament_upcoming_match_links]

    def scrape_tournament_data_fallback(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        # Get the match links from the atptour website
        tournament_link_data = requests.get(url, headers=headers, timeout=10)
        tournament_link_data.raise_for_status()

        # get the tournament surface
        tournament_data = {
            'Surface': tournament_link_data.json()['Surface'],
            'Name': tournament_link_data.json()['SponsorTitle']
        }

        return tournament_data
    
    def scrape_upcoming_match_links_fallback(self, match_list):
        print(f'scraping upcoming match links for {len(match_list)} matches')
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        formatted_data = []
        for match in match_list:
            time.sleep(random.uniform(2, 4))
            data = requests.get(match['match_h2h_link'], headers=headers, timeout=10)
            data.raise_for_status()
            
            formatted_data.append({
                'Player_Name': data.json()['PlayerTeam1']['PlayerFullName'],
                'Dominant_Hand': data.json()['PlayerTeam1']['PlayHand'],
                'Backhand_Type': 2 if data.json()['PlayerTeam1']['BackHand']['Description'] == 'Two-Handed' else 1,
                'Height': data.json()['PlayerTeam1']['HeightCm'],
                'DOB': pd.to_datetime(data.json()['PlayerTeam1']['BirthDate'], format='mixed').strftime('%Y-%m-%d').replace('-', ''),
                'Country': self.get_country_from_name(data.json()['PlayerTeam1']['BirthPlace']),
                'Date': match['match_date'],
                'Tournament': match['tournament'],
                'Surface': match['match_surface'],
                'Level': 'G' if match['tournament'] in ['Wimbledon', 'US Open', 'Australian Open', 'French Open'] else 'M',
                'Outcome': None,
                'ATP_Rank': data.json()['PlayerTeam1']['Ranking'],
                'Seed': None,
                'Entry': None,
                'Round': match['round'],
                'w1': None,
                'l1': None,
                'w2': None,
                'l2': None,
                'w3': None,
                'l3': None,
                'w4': None,
                'l4': None,
                'w5': None,
                'l5': None,
                'player_sets': None,
                'opponent_sets': None,
                'Best_of': 5,
                'Opponent_Name': data.json()['PlayerTeam2']['PlayerFullName'],
                'Opponent_Rank': data.json()['PlayerTeam2']['Ranking'],
                'Opponent_Seed': None,
                'Opponent_Entry': None,
                'Opponent_Hand': data.json()['PlayerTeam2']['PlayHand'],
                'Opponent_DOB': pd.to_datetime(data.json()['PlayerTeam2']['BirthDate'], format='mixed').strftime('%Y-%m-%d').replace('-', ''),
                'Opponent_Height': data.json()['PlayerTeam2']['HeightCm'],
                'Opponent_Nationality': self.get_country_from_name(data.json()['PlayerTeam2']['BirthPlace']),
                'Opponent_Backhand_Type': 2 if data.json()['PlayerTeam2']['BackHand']['Description'] == 'Two-Handed' else 1,
                'Total_Points': None,
                'Match_Length': None,
                'Aces': None,
                'Double_Faults': None,
                'Service_Points': None,
                '1st_Serve_Points': None,
                '1st_Serve_Points_Won': None,
                '2nd_Serve_Points_Won': None,
                'Games_Served': None,
                'Break_Points_Saved': None,
                'Break_Point_Opprotunities': None,
                'Opponent_Aces': None,
                'Opponent_Double_Faults': None,
                'Opponent_Service_Points': None,
                'Opponent_1st_Serve_Points': None,
                'Opponent_1st_Serve_Points_Won': None,
                'Opponent_2nd_Serve_Points_Won': None,
                'Opponent_Games_Served': None,
                'Opponent_Break_Points_Saved': None,
                'Opponent_Break_Point_Opprotunities': None,
                'Dominance_Ratio': None,
                'Opponent_Dominance_Ratio': None,
                'Ace_Percent': None,
                'Opponent_Ace_Percent': None,
                'Double_Fault_Percent': None,
                'Opponent_Double_Fault_Percent': None,
                '1st_In_Play_Rate': None,
                'Opponent_1st_In_Play_Rate': None,
                '1st_Serve_Points_Won_Rate': None,
                '2nd_Serve_Points_Won_Rate': None,
                'Opponent_1st_Serve_Points_Won_Rate': None,
                'Opponent_2nd_Serve_Points_Won_Rate': None,
                'Break_Point_Save_Rate': None,
                'Opponent_Break_Point_Save_Rate': None,
                'Total_Points_Won_Percent': None,
                'Return_Points_Won_Percent': None,
                'Opponent_Total_Points_Won_Percent': None,
                'Opponent_Return_Points_Won_Percent': None,
                'Break_Point_Opportunities_Converted': None,
                'Opponent_Break_Point_Opportunities_Converted': None
            })

        upcoming_matches_df = pd.DataFrame(formatted_data)
        # write the upcoming_matches_df to a csv file
        upcoming_matches_df.to_csv('data/atp_upcoming_matches.csv', index=False)

    def scrape_completed_match_links_fallback(self, match_list):
        print(f'scraping completed match links for {len(match_list)} matches')
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        formatted_data = []
        count = 0
        for match in match_list:
            time.sleep(random.uniform(2, 4))
            data = requests.get(match['match_stats_link'], headers=headers, timeout=10)
            data.raise_for_status()
            
            print(f'scraping {match["match_stats_link"]}')
            
            count += 1
            if count == 2:
                break

    def scrape_new_match_links(self, fallback_tournament_links=[]):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

        # read in the already scraped matches
        player_matches = pd.read_csv('data/atp_player_match_data.csv')
        # convert all the dates to just the year
        player_matches['Date'] = player_matches['Date'].apply(lambda x: x.split('-')[0])

        # getting the tournament links
        page = self.session.get('https://www.tennisabstract.com/', headers=headers, timeout=10)
        page.raise_for_status()

        soup = BeautifulSoup(page.content, 'html.parser')
        all_tournament_links = soup.find_all('a', href=True)

        tournament_links = [link for link in all_tournament_links if 'Results and Forecasts' in link.text]
        tournament_links = [link for link in tournament_links if 'atp' in link['href'].lower()]
        # combine the tournament links with the fallback tournament links
        tournament_links = tournament_links + fallback_tournament_links
        # make sure the tournament links are unique
        tournament_links = list(set(tournament_links))

        # getting a unique list of the relevant player links for each tournament
        full_player_links = []
        for link in tournament_links:
            time.sleep(random.uniform(2, 4))
            page = self.session.get(link['href'], headers=headers, timeout=10)
            page.raise_for_status()

            soup = BeautifulSoup(page.content, 'html.parser')
            tourny_title = soup.find('h2').text
            tourny_year = tourny_title.split(' ')[0]
            tourny_name = tourny_title[len(tourny_year) + 1:].replace('ATP', '').replace(' ', '')

            # get the upcoming singles
            upcoming_singles_regex = re.compile(r'var upcomingSingles = \'(.*)\';')
            upcoming_singles = upcoming_singles_regex.search(page.text)
            if upcoming_singles:
                upcoming_singles = upcoming_singles.group(1)
            else:
                print('No upcoming singles found')
                continue
            upcoming_singles = upcoming_singles.split('<br/>')
            upcoming_singles_obj = []
            for single in upcoming_singles:
                if '(q)' in single:
                    continue
                player_soup = BeautifulSoup('<div>' + single + '</div>', 'html.parser')
                players = [player.text for player in player_soup.find_all('a', href=True)]
                players = [player for player in players if player != 'd.' and not player.startswith('[')]
                if len(players) != 2:
                    continue
                upcoming_singles_obj.append({
                    'player1': players[0],
                    'player2': players[1],
                    'tourny_name': tourny_name,
                    'tourny_year': tourny_year
                })

            # get the completed singles
            completed_singles_regex = re.compile(r'var completedSingles = \'(.*)\';')
            completed_singles = completed_singles_regex.search(page.text)
            if completed_singles:
                completed_singles = completed_singles.group(1)
            else:
                print('No completed singles found')
                continue
            completed_singles = completed_singles.split('<br/>')
            completed_singles_obj = []
            for single in completed_singles:
                if '(q)' in single:
                    continue
                player_soup = BeautifulSoup('<div>' + single + '</div>', 'html.parser')
                players = [player.text for player in player_soup.find_all('a', href=True)]
                players = [player for player in players if player != 'd.' and not player.startswith('[')]
                if len(players) != 2:
                    continue
                completed_singles_obj.append({
                    'player1': players[0],
                    'player2': players[1],
                    'tourny_name': tourny_name,
                    'tourny_year': tourny_year
                })

            for obj in upcoming_singles_obj:
                # check to see if the player link is already in the full_player_links list
                if {
                    'player_name': obj['player1'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player1"].replace(" ", "")}&f=ACareerqq'
                } in full_player_links:
                    continue
                if {
                    'player_name': obj['player2'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player2"].replace(" ", "")}&f=ACareerqq'
                } in full_player_links:
                    continue
                full_player_links.append({
                    'player_name': obj['player1'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player1"].replace(" ", "")}&f=ACareerqq'
                })
                full_player_links.append({
                    'player_name': obj['player2'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player2"].replace(" ", "")}&f=ACareerqq'
                })

            completed_singles_df = pd.DataFrame(completed_singles_obj)
            if not completed_singles_df.empty:
                # remove all the completed singles that are already in the player_matches dataframe by comparing the tourny_name, tourny_year, player1, and player2
                completed_singles_df = completed_singles_df[~completed_singles_df[['tourny_name', 'tourny_year', 'player1', 'player2']].isin(player_matches[['Tournament', 'Date', 'Player_Name', 'Opponent_Name']])]

            for obj in completed_singles_df.to_dict('records'):
                # check to see if the player link is already in the full_player_links list
                if {
                    'player_name': obj['player1'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player1"].replace(" ", "")}&f=ACareerqq'
                } in full_player_links:
                    continue
                if {
                    'player_name': obj['player2'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player2"].replace(" ", "")}&f=ACareerqq'
                } in full_player_links:
                    continue
                full_player_links.append({
                    'player_name': obj['player1'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player1"].replace(" ", "")}&f=ACareerqq'
                })
                full_player_links.append({
                    'player_name': obj['player2'],
                    'player_link': f'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={obj["player2"].replace(" ", "")}&f=ACareerqq'
                })
        
        if len(full_player_links) == 0:
            tournament_links = [link for link in all_tournament_links if 'forecast' in link.text.lower()]
            tournament_links = [link for link in tournament_links if 'atp' in link['href'].lower() or ('challenger' not in link['href'].lower() and 'women' not in link['href'].lower())]

            tournament_player_links = []
            for link in tournament_links:
                time.sleep(random.uniform(2, 4))
                page = self.session.get(link['href'], headers=headers, timeout=10)
                page.raise_for_status()

                soup = BeautifulSoup(page.content, 'html.parser')

                # get all the table rows
                table_rows = soup.find_all('tr')
                for row in table_rows:
                    # get all the columns
                    all_columns = row.find_all('td')
                    for column in all_columns:
                        # get the player link
                        player_link = column.find('a')
                        if player_link:
                            tournament_player_links.append(player_link['href'])
            
            # make sure the links are unique
            tournament_player_links = list(set(tournament_player_links))
            for player_link in tournament_player_links:
                print(player_link)
                print()
        
        # fallback in case there are issues with tennisabstract.com
        if (upcoming_singles_obj is None or len(upcoming_singles_obj) == 0) and len(fallback_tournament_links) == 0:
            tournament_links = self.tournament_link_construction()
            full_player_links = self.scrape_new_match_links(tournament_links)

        return full_player_links

    def scrape_new_matches(self):
        player_links = self.scrape_new_match_links()
        print(f'scraping the links for {len(player_links)} players in upcoming or recently completed matches')

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        scraped_matches = []
        for link in player_links:
            time.sleep(random.uniform(2, 4))
            page = self.session.get(link['player_link'], headers=headers, timeout=10)
            page.raise_for_status()
            soup = BeautifulSoup(page.content, 'html.parser')
            matches = self.parse_matchmx(str(soup), link['player_name'])
            scraped_matches.extend(matches)

        for match in scraped_matches:
            if match['New_Match']:
                print(match['Tournament'], match['Player_Name'], match['Opponent_Name'], match['Date'])
        
        # split the scraped_matches into two lists based on the New_Match column
        new_matches = [match for match in scraped_matches if match['New_Match']]
        recent_matches = [match for match in scraped_matches if not match['New_Match']]

        # ensure all the recent match results are in the database
        for match in recent_matches:
            player = match['Player_Name']
            opponent = match['Opponent_Name']
            date = pd.to_datetime(match['Date'])
            result = match['Outcome']

            if date.tz_localize('UTC') < pd.to_datetime('2025-06-28', format='mixed', utc=True):
                continue

            if player > opponent:
                row_id = f'{opponent}{player}{date.strftime("%Y-%m-%d")}'
                result = (result == 'L')
            else:
                row_id = f'{player}{opponent}{date.strftime("%Y-%m-%d")}'
                result = (result == 'W')

            print(f'row_id: {row_id}, result: {result}')

            # adding the match result to the db
            response = (
                supabase.table('events')
                .update({
                    'result': result,
                    'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                })
                .eq('id', row_id)
                .execute()
            )

            if response.data == [] and response.count == None:
                # reconstruct the row_id and shift the day forward by 1 day
                if player > opponent:
                    row_id = f'{opponent}{player}{(date + timedelta(days=1)).strftime("%Y-%m-%d")}'
                    result = (result == 'L')
                else:
                    row_id = f'{player}{opponent}{(date + timedelta(days=1)).strftime("%Y-%m-%d")}'
                    result = (result == 'W')
                
                # adding the match result to the db
                response = (
                    supabase.table('events')
                    .update({
                        'result': result,
                        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    })
                    .eq('id', row_id)
                    .execute()
                )

                if response.data == [] and response.count == None:
                    # reconstruct the row_id and shift the day back by 1 day
                    if player > opponent:
                        row_id = f'{opponent}{player}{(date - timedelta(days=1)).strftime("%Y-%m-%d")}'
                        result = (result == 'L')
                    else:
                        row_id = f'{player}{opponent}{(date - timedelta(days=1)).strftime("%Y-%m-%d")}'
                        result = (result == 'W')

                    # adding the match result to the db
                    response = (
                        supabase.table('events')
                        .update({
                            'result': result,
                            'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        })
                        .eq('id', row_id)
                        .execute()
                    )

                    if response.data == [] and response.count == None:
                        continue
                    else:
                        print(f'!!!!!! Added match result to the db for {row_id}')
                else:
                    print(f'!!!!!! Added match result to the db for {row_id}')
            else:
                print(f'!!!!!! Added match result to the db for {row_id}')

        # save the new matches to a csv
        new_matches_df = pd.DataFrame(new_matches)
        # drop the New_Match column
        new_matches_df = new_matches_df.drop(columns=['New_Match'])
        # save the new matches to a csv
        new_matches_df.to_csv('data/atp_new_matches.csv', index=False)
        
        recent_matches_df = pd.DataFrame(recent_matches)
        # drop the New_Match column
        recent_matches_df = recent_matches_df.drop(columns=['New_Match'])
        # open the old matches csv
        old_matches = pd.read_csv('data/atp_player_match_data.csv')
        # merge the old matches with the new matches
        merged_matches = pd.concat([old_matches, recent_matches_df])
        # drop duplicates
        merged_matches = merged_matches.drop_duplicates()
        # save the merged matches to a csv
        merged_matches.to_csv('data/atp_player_match_data.csv', index=False)
    
    def parse_matchmx(self, html_content, player_name):
        dob = re.search(r'var dob = (\d+);', html_content)
        if dob:
            dob = dob.group(1)
        else:
            dob = None
        # getting the player's height by finding var ht = 
        height = re.search(r'var ht = (\d+);', html_content)
        if height:
            height = height.group(1)
        else:
            height = None
        # getting the player's dominant hand by finding var hand = 
        dominant_hand = re.search(r'var hand = \'(\w+)\';', html_content)
        if dominant_hand:
            dominant_hand = dominant_hand.group(1)
        else:
            dominant_hand = None
        # getting the player's backhand type by finding var backhand = 
        backhand_type = re.search(r'var backhand = \'(\w+)\';', html_content)
        if backhand_type:
            backhand_type = backhand_type.group(1)
        else:
            backhand_type = None
        # getting the player's country by finding var country = 
        country = re.search(r'var country = \'(\w+)\';', html_content)
        if country:
            country = country.group(1)
        else:
            country = None

        pattern = r'var matchmx = (\[.*?\]);'
        match = re.search(pattern, html_content, re.DOTALL)
        if not match:
            return []
        
        array_str = match.group(1)
        array_str = re.sub(r',\s*,', ',null,', array_str)
        array_str = re.sub(r'\[\s*,', '[null,', array_str)
        array_str = re.sub(r',\s*\]', ',null]', array_str)
        array_str = array_str.replace('""', 'null')
        
        try:
            raw_matches = json.loads(array_str)
            formatted_matches = []
            
            for match in raw_matches:
                def safe_division(numerator, denominator, decimal_places=3):
                    try:
                        num = float(numerator) if numerator else 0
                        denom = float(denominator) if denominator else 0
                        if denom == 0:
                            return 0
                        return round(num / denom * 100, decimal_places)
                    except (ValueError, TypeError):
                        return 0

                # Format date from YYYYMMDD
                try:
                    date_str = str(match[0])
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                except (IndexError, TypeError):
                    formatted_date = None
                
                new_match = match[4] == 'U'
                if not new_match and ('RET' in match[9] or 'W/O' in match[9] or 'def' in match[9].lower() or not match[25] or not match[32]):
                    continue

                # getting the individual set scores
                # remove parentheses and anything in them with a regex
                match[9] = re.sub(r'\([^)]*\)', '', match[9]) if match[9] else None

                match1, match2, match3, match4, match5 = [], [], [], [], []
                w1, l1, w2, l2, w3, l3, w4, l4, w5, l5 = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                total_points_won = 0
                return_points_won = 0
                opponent_points_won = 0
                opponent_return_points_won = 0
                games_won = 0
                games_lost = 0
                try:
                    if not new_match:
                        if match[10] == '3':
                            went_to_third_set = len(match[9].split(' ')) > 2

                            match1 = match[9].split(' ')[0].split('-')
                            match2 = match[9].split(' ')[1].split('-')
                            if went_to_third_set:
                                match3 = match[9].split(' ')[2].split('-')
                        elif match[10] == '5':
                            went_to_fourth_set = len(match[9].split(' ')) > 3
                            went_to_fifth_set = len(match[9].split(' ')) > 4

                            match1 = match[9].split(' ')[0].split('-')
                            match2 = match[9].split(' ')[1].split('-')
                            match3 = match[9].split(' ')[2].split('-')
                            if went_to_fourth_set:
                                match4 = match[9].split(' ')[3].split('-')
                            if went_to_fifth_set:
                                match5 = match[9].split(' ')[4].split('-')

                        w1 = match1[0] if match[4] == 'W' else match1[1]
                        l1 = match1[1] if match[4] == 'W' else match1[0]
                        w1 = re.sub(r'[^0-9]', '', w1)
                        l1 = re.sub(r'[^0-9]', '', l1)
                        games_won += 1 if w1 > l1 else 0
                        games_lost += 1 if l1 > w1 else 0

                        w2 = match2[0] if match[4] == 'W' else match2[1]
                        l2 = match2[1] if match[4] == 'W' else match2[0]
                        w2 = re.sub(r'[^0-9]', '', w2)
                        l2 = re.sub(r'[^0-9]', '', l2)
                        games_won += 1 if w2 > l2 else 0
                        games_lost += 1 if l2 > w2 else 0

                        if len(match3) > 0:
                            w3 = match3[0] if match[4] == 'W' else match3[1]
                            l3 = match3[1] if match[4] == 'W' else match3[0]
                            w3 = re.sub(r'[^0-9]', '', w3)
                            l3 = re.sub(r'[^0-9]', '', l3)
                            games_won += 1 if w3 > l3 else 0
                            games_lost += 1 if l3 > w3 else 0
                        else:
                            w3 = 0
                            l3 = 0
                        if len(match4) > 0:
                            w4 = match4[0] if match[4] == 'W' else match4[1]
                            l4 = match4[1] if match[4] == 'W' else match4[0]
                            w4 = re.sub(r'[^0-9]', '', w4)
                            l4 = re.sub(r'[^0-9]', '', l4)
                            games_won += 1 if w4 > l4 else 0
                            games_lost += 1 if l4 > w4 else 0
                        else:
                            w4 = 0
                            l4 = 0
                        if len(match5) > 0:
                            w5 = match5[0] if match[4] == 'W' else match5[1]
                            l5 = match5[1] if match[4] == 'W' else match5[0]
                            w5 = re.sub(r'[^0-9]', '', w5)
                            l5 = re.sub(r'[^0-9]', '', l5)
                            games_won += 1 if w5 > l5 else 0
                            games_lost += 1 if l5 > w5 else 0
                        else:
                            w5 = 0
                            l5 = 0

                        # getting the number of points won by the player
                        total_points_won = float(match[25]) + float(match[26]) + (float(match[32]) - (float(match[34]) + float(match[35])))
                        return_points_won = float(match[32]) - (float(match[34]) + float(match[35]))
                        # getting the number of points won by the opponent
                        opponent_points_won = float(match[34]) + float(match[35]) + (float(match[23]) - (float(match[25]) + float(match[26])))
                        opponent_return_points_won = float(match[23]) - (float(match[25]) + float(match[26]))

                    match_obj = {
                        "New_Match": new_match,
                        "Player_Name": player_name,
                        "Dominant_Hand": dominant_hand,
                        "Backhand_Type": backhand_type,
                        "Height": height,
                        "DOB": dob,
                        "Country": country,
                        # General Match Details
                        "Date": formatted_date,
                        "Tournament": match[1],
                        "Surface": match[2],
                        "Level": match[3],  # G=Grand Slam, M=Masters, A=ATP 250, O=Olympics, C=Challenger
                        "Outcome": match[4] if not new_match else None,  # W=Win, L=Loss
                        "ATP_Rank": int(match[5]) if match[5] else 0,
                        "Seed": int(match[6]) if match[6] else 0,
                        "Entry": match[7],  # WC=Wild Card, PR=Protected Ranking
                        "Round": match[8],
                        "w1": int(w1) if w1 else 0 if not new_match else None,
                        "l1": int(l1) if l1 else 0 if not new_match else None,
                        "w2": int(w2) if w2 else 0 if not new_match else None,
                        "l2": int(l2) if l2 else 0 if not new_match else None,
                        "w3": int(w3) if w3 else 0 if not new_match else None,
                        "l3": int(l3) if l3 else 0 if not new_match else None,
                        "w4": int(w4) if w4 else 0 if not new_match else None,
                        "l4": int(l4) if l4 else 0 if not new_match else None,
                        "w5": int(w5) if w5 else 0 if not new_match else None,
                        "l5": int(l5) if l5 else 0 if not new_match else None,
                        "player_sets": games_won if not new_match else None,
                        "opponent_sets": games_lost if not new_match else None,
                        "Best_of": int(match[10]) if match[10] else 0,

                        # Opponent Details
                        "Opponent_Name": match[11],
                        "Opponent_Rank": int(match[12]) if match[12] else 0,
                        "Opponent_Seed": int(match[13]) if match[13] else 0,
                        "Opponent_Entry": match[14],
                        "Opponent_Hand": match[15],  # R=Right, L=Left
                        "Opponent_DOB": match[16],
                        "Opponent_Height": int(match[17]) if match[17] else 0,
                        "Opponent_Nationality": match[18],
                        "Opponent_Backhand_Type": int(match[39]) if match[39] else 0,

                        # Match Statistics
                        "Total_Points": float(match[23]) + float(match[32]) if not new_match else None,
                        "Match_Length": int(match[20]) if match[20] else 0 if not new_match else None,
                        "Aces": int(match[21]) if match[21] else 0 if not new_match else None,
                        "Double_Faults": int(match[22]) if match[22] else 0 if not new_match else None,
                        "Service_Points": int(match[23]) if match[23] else 0 if not new_match else None,
                        "1st_Serve_Points": int(match[24]) if match[24] else 0 if not new_match else None,
                        "1st_Serve_Points_Won": int(match[25]) if match[25] else 0 if not new_match else None,
                        "2nd_Serve_Points_Won": int(match[26]) if match[26] else 0 if not new_match else None,
                        "Games_Served": int(match[27]) if match[27] else 0 if not new_match else None,
                        "Break_Points_Saved": int(match[28]) if match[28] else 0 if not new_match else None,
                        "Break_Point_Opprotunities": int(match[29]) if match[29] else 0 if not new_match else None,
                        "Opponent_Aces": int(match[30]) if match[30] else 0 if not new_match else None,
                        "Opponent_Double_Faults": int(match[31]) if match[31] else 0 if not new_match else None,
                        "Opponent_Service_Points": int(match[32]) if match[32] else 0 if not new_match else None,
                        "Opponent_1st_Serve_Points": int(match[33]) if match[33] else 0 if not new_match else None,
                        "Opponent_1st_Serve_Points_Won": int(match[34]) if match[34] else 0 if not new_match else None,
                        "Opponent_2nd_Serve_Points_Won": int(match[35]) if match[35] else 0 if not new_match else None,
                        "Opponent_Games_Served": int(match[36]) if match[36] else 0 if not new_match else None,
                        "Opponent_Break_Points_Saved": int(match[37]) if match[37] else 0 if not new_match else None,
                        "Opponent_Break_Point_Opprotunities": int(match[38]) if match[38] else 0 if not new_match else None,

                        # Additional Calculated Statistics
                        "Dominance_Ratio": safe_division(safe_division(return_points_won, float(match[32]), 1), safe_division(opponent_return_points_won, float(match[23]), 1), 1) if not new_match else None,
                        "Opponent_Dominance_Ratio": safe_division(safe_division(opponent_return_points_won, float(match[23]), 1), safe_division(return_points_won, float(match[32]), 1), 1) if not new_match else None,
                        "Ace_Percent": safe_division(match[21], match[23], 1) if not new_match else None,
                        "Opponent_Ace_Percent": safe_division(match[30], match[32], 1) if not new_match else None,
                        "Double_Fault_Percent": safe_division(match[22], match[23], 1) if not new_match else None,
                        "Opponent_Double_Fault_Percent": safe_division(match[31], match[32], 1) if not new_match else None,
                        "1st_In_Play_Rate": safe_division(match[24], match[23], 1) if not new_match else None,
                        "Opponent_1st_In_Play_Rate": safe_division(match[33], match[32], 1) if not new_match else None,
                        "1st_Serve_Points_Won_Rate": safe_division(match[25], match[24], 1) if not new_match else None,
                        "2nd_Serve_Points_Won_Rate": safe_division(match[26], float(match[23]) - float(match[24]), 1) if not new_match else None,
                        "Opponent_1st_Serve_Points_Won_Rate": safe_division(match[34], match[33], 1) if not new_match else None,
                        "Opponent_2nd_Serve_Points_Won_Rate": safe_division(match[35], float(match[32]) - float(match[33]), 1) if not new_match else None,
                        "Break_Point_Save_Rate": safe_division(match[28], match[29], 1) if not new_match else None,
                        "Opponent_Break_Point_Save_Rate": safe_division(match[37], match[38], 1) if not new_match else None,
                        "Total_Points_Won_Percent": safe_division(total_points_won, float(match[23]) + float(match[32]), 1) if not new_match else None,
                        "Return_Points_Won_Percent": safe_division(return_points_won, total_points_won, 1) if not new_match else None,
                        "Opponent_Total_Points_Won_Percent": safe_division(opponent_points_won, float(match[32]) + float(match[23]), 1) if not new_match else None,
                        "Opponent_Return_Points_Won_Percent": safe_division(opponent_return_points_won, opponent_points_won, 1) if not new_match else None,
                        "Break_Point_Opportunities_Converted": safe_division(float(match[38]) - float(match[37]), float(match[38]), 1) if not new_match else None,
                        "Opponent_Break_Point_Opportunities_Converted": safe_division(float(match[29]) - float(match[28]), float(match[29]), 1) if not new_match else None,
                    }
                    
                    formatted_matches.append(match_obj)
                except Exception as e:
                    print(f"Error parsing match: {e}")
                    continue

            return formatted_matches
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return []

if __name__ == "__main__":
    start_time = time.time()

    # scraper = ATPMatchDataScraper()
    # match_links = scraper.scrape_match_links_fallback()
    # scraper.scrape_completed_match_links_fallback(match_links[0])
    # scraper.scrape_upcoming_match_links_fallback(match_links[1])

    if len(sys.argv) > 1 and sys.argv[1] == 'new':
        scraper = ATPMatchDataScraper(new_matches=True)
        scraper.scrape_new_matches()
    else:
        scraper = ATPMatchDataScraper()
        player_matches = scraper.scrape_full_match_history()
        print(f'Scraped {len(player_matches)} matches')
        # save to csv
        player_matches_df = pd.DataFrame(player_matches).drop(columns=['New_Match'])
        player_matches_df.to_csv('data/atp_player_match_data.csv', index=False)

    # print the time it took to scrape and save data
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f'done scraping and saving data, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print('SUCCESS -- New match data saved to csv -- SUCCESS')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')