import requests, time, random
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from functions.general import get_team_from_name
from crawlbase import CrawlingAPI
import os
from dotenv import load_dotenv

load_dotenv()

class Scraper:
    def __init__(self, use_crawlbase=True):
        # Initialize Crawlbase Smart AI Proxy
        crawlbase_token = os.getenv('CRAWLBASE_TOKEN')
        if not crawlbase_token:
            raise ValueError("CRAWLBASE_TOKEN environment variable is required")
        self.crawlbase_api = CrawlingAPI({'token': crawlbase_token})

        self.use_crawlbase = use_crawlbase
        self.session = self.get_session()
        self.nbaTeams = [
            'bos', 'nyk', 'phi', 'brk', 'tor', 'cle', 'mil',
            'ind', 'det', 'chi', 'atl', 'orl', 'mia', 'cho',
            'was', 'okc', 'den', 'min', 'por', 'uta', 'lac',
            'lal', 'sac', 'gsw', 'pho', 'hou', 'mem', 'dal',
            'sas', 'nop'
        ]
        self.newGames = []

    def get_session(self):
        # Note: With Crawlbase, the session is handled internally
        # We still create a session for any non-Crawlbase requests if needed
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # Basic headers - Crawlbase will handle most browser simulation
        session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
        })
        return session

    def get_game_data(self, team, year):
        print(f"Scraping {team} {year}")
        url = f"https://www.basketball-reference.com/teams/{team}/{year}/gamelog/"
        
        try:
            response = None
            html_content = None
            if self.use_crawlbase:
                # Use Crawlbase Smart AI Proxy to bypass Cloudflare
                response = self.crawlbase_api.get(url)
                if response['status_code'] != 200:
                    print(f"Error scraping {team} {year}: {response['status_code']}")
                    print(f"Response headers: {response.get('headers', {})}")
                    return []
                
                # Get the HTML content from Crawlbase response
                html_content = response['body']
            else:
                response = self.session.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                response.raise_for_status()
                
                if response.status_code != 200:
                    print(f"Error scraping {team} {year}: {response.status_code}")
                    return []
                
                html_content = response.content
            
            soup = BeautifulSoup(html_content, "html.parser")

            trs = soup.find_all("tr", {"id": lambda x: x and x.startswith("team_game_log")})
            games = []
            for tr in trs:
                games.append({
                    "team": team,
                    "date": tr.find("td", {"data-stat": "date"}).text,
                    "opponent": tr.find("td", {"data-stat": "opp_name_abbr"}).text.lower(),
                    "home": tr.find("td", {"data-stat": "game_location"}).text == "@" or False,
                    "win": 1 if tr.find("td", {"data-stat": "team_game_result"}).text == "W" else 0,
                    "points": tr.find("td", {"data-stat": "team_game_score"}).text,
                    "opponent_points": tr.find("td", {"data-stat": "opp_team_game_score"}).text,
                    "field_goals": tr.find("td", {"data-stat": "fg"}).text,
                    "field_goals_attempted": tr.find("td", {"data-stat": "fga"}).text,
                    "field_goals_percentage": tr.find("td", {"data-stat": "fg_pct"}).text,
                    "three_point_field_goals": tr.find("td", {"data-stat": "fg3"}).text,
                    "three_point_field_goals_attempted": tr.find("td", {"data-stat": "fg3a"}).text,
                    "three_point_field_goals_percentage": tr.find("td", {"data-stat": "fg3_pct"}).text,
                    "free_throws": tr.find("td", {"data-stat": "ft"}).text,
                    "free_throws_attempted": tr.find("td", {"data-stat": "fta"}).text,
                    "free_throws_percentage": tr.find("td", {"data-stat": "ft_pct"}).text,
                    "offensive_rebounds": tr.find("td", {"data-stat": "orb"}).text,
                    "total_rebounds": tr.find("td", {"data-stat": "trb"}).text,
                    "assists": tr.find("td", {"data-stat": "ast"}).text,
                    "steals": tr.find("td", {"data-stat": "stl"}).text,
                    "blocks": tr.find("td", {"data-stat": "blk"}).text,
                    "turnovers": tr.find("td", {"data-stat": "tov"}).text,
                    "personal_fouls": tr.find("td", {"data-stat": "pf"}).text,
                    "opponent_field_goals": tr.find("td", {"data-stat": "opp_fg"}).text,
                    "opponent_field_goals_attempted": tr.find("td", {"data-stat": "opp_fga"}).text,
                    "opponent_field_goals_percentage": tr.find("td", {"data-stat": "opp_fg_pct"}).text,
                    "opponent_three_point_field_goals": tr.find("td", {"data-stat": "opp_fg3"}).text,
                    "opponent_three_point_field_goals_attempted": tr.find("td", {"data-stat": "opp_fg3a"}).text,
                    "opponent_three_point_field_goals_percentage": tr.find("td", {"data-stat": "opp_fg3_pct"}).text,
                    "opponent_free_throws": tr.find("td", {"data-stat": "opp_ft"}).text,
                    "opponent_free_throws_attempted": tr.find("td", {"data-stat": "opp_fta"}).text,
                    "opponent_free_throws_percentage": tr.find("td", {"data-stat": "opp_ft_pct"}).text,
                    "opponent_offensive_rebounds": tr.find("td", {"data-stat": "opp_orb"}).text,
                    "opponent_total_rebounds": tr.find("td", {"data-stat": "opp_trb"}).text,
                    "opponent_assists": tr.find("td", {"data-stat": "opp_ast"}).text,
                    "opponent_steals": tr.find("td", {"data-stat": "opp_stl"}).text,
                    "opponent_blocks": tr.find("td", {"data-stat": "opp_blk"}).text,
                    "opponent_turnovers": tr.find("td", {"data-stat": "opp_tov"}).text,
                    "opponent_personal_fouls": tr.find("td", {"data-stat": "opp_pf"}).text,
                })
            
            return games
        
        except Exception as e:
            print(f"Error scraping {team} {year}: {e}")
            return []

    def scrape_nba_data(self, start_year, end_year):
        all_games = []
        for team in self.nbaTeams:
            for year in range(start_year, end_year + 1):
                # Add a random delay between requests
                time.sleep(random.uniform(3, 4))

                # handling all the weird corner cases
                if team == 'brk' or team == 'njn':
                    if year < 2013:
                        team = 'njn'
                    else:
                        team = 'brk'
                elif team == 'cho' or team == 'cha':
                    if year < 2005:
                        continue
                    elif year < 2015:
                        team = 'cha'
                    else:
                        team = 'cho'
                elif team == 'okc' or team == 'sea':
                    if year < 2009:
                        team = 'sea'
                    else:
                        team = 'okc'
                elif team == 'mem' or team == 'van':
                    if year < 2002:
                        team = 'van'
                    else:
                        team = 'mem'
                elif team == 'nop' or team == 'noh' or team == 'nok':
                    if year < 2003:
                        continue
                    elif year < 2006:
                        team = 'noh'
                    elif year < 2008:
                        team = 'nok'
                    elif year < 2014:
                        team = 'noh'
                    else:
                        team = 'nop'

                stats = self.get_game_data(team.lower(), year)
                if not stats:
                    continue

                all_games.extend(stats)
        
        print(f'Scraped {len(all_games)} games')

        self.new_games = self.scrape_next_games(end_year)
        
        # add the new games to the all games
        all_games.extend(self.new_games)

        print(f'Scraped {len(all_games)} games')

        # read the old scraped data
        try:
            old_games_df = pd.read_csv("data/nba_games.csv")
        except FileNotFoundError:
            old_games_df = pd.DataFrame()
        print(f'Read {len(old_games_df)} games from data/nba_games.csv')
        # convert the date column to datetime
        if not old_games_df.empty:
            old_games_df['date'] = pd.to_datetime(old_games_df['date'], errors='coerce')

        # create new games DataFrame
        new_games_df = pd.DataFrame(all_games)
        if not new_games_df.empty:
            new_games_df['date'] = pd.to_datetime(new_games_df['date'], errors='coerce')

        # combine the new and old games (new first to prefer updated data)
        combined_games = pd.concat([new_games_df, old_games_df])

        # remove duplicates on team, opponent, and date (keeps first occurrence, so new data takes precedence)
        combined_games = combined_games.drop_duplicates(subset=['team', 'opponent', 'date'])
        print(f'Saved {len(combined_games)} games to data/nba_games.csv')

        # sort by year (grouped), then team, then date ascending
        combined_games['year'] = combined_games['date'].dt.year
        combined_games = combined_games.sort_values(by=['year', 'team', 'date'], ascending=True)
        combined_games = combined_games.drop(columns=['year'])
        combined_games.to_csv("data/nba_games.csv", index=False)

        return combined_games
    
    def scrape_next_games(self, current_year):
        print('Scraping next games...')

        next_games = []
        # loop through all the teams
        for team in self.nbaTeams:
            print(f'Scraping {team} {current_year}')

            url = f'https://www.basketball-reference.com/teams/{team.upper()}/{current_year}_games.html'
            
            try:
                response = None
                html_content = None
                if self.use_crawlbase:
                    # Use Crawlbase Smart AI Proxy to bypass Cloudflare
                    response = self.crawlbase_api.get(url)
                    if response['status_code'] != 200:
                        print(f"Error scraping {team} {current_year}: {response['status_code']}")
                        continue
                    
                    # Get the HTML content from Crawlbase response
                    html_content = response['body']
                else:
                    response = self.session.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                    response.raise_for_status()
                    
                    if response.status_code != 200:
                        print(f"Error scraping {team} {current_year}: {response.status_code}")
                        continue
                    
                    html_content = response.content
                
                soup = BeautifulSoup(html_content, "html.parser")
            except Exception as e:
                print(f"Error scraping {team} {current_year}: {e}")
                continue
            trs = soup.find_all("tr", {"class": None})

            # get the first tr with no text in the td with the data-stat 'game_result'
            for tr in trs:
                date_elem = tr.find("td", {"data-stat": "date_game"})
                if not date_elem:
                    continue
                date = date_elem['csk']

                # if the date is tomorrow, add the game to the list
                home = not tr.find("td", {"data-stat": "game_location"}).text == "@"
                opponent = tr.find("td", {"data-stat": "opp_name"}).text.lower()
                opponent_team = get_team_from_name(opponent)
                next_games.append({
                    "date": date,
                    "team": team,
                    "opponent": opponent_team,
                    "home": home,
                    "win": None,
                    "points": None,
                    "opponent_points": None,
                    "field_goals": None,
                    "field_goals_attempted": None,
                    "field_goals_percentage": None,
                    "three_point_field_goals": None,
                    "three_point_field_goals_attempted": None,
                    "three_point_field_goals_percentage": None,
                    "free_throws": None,
                    "free_throws_attempted": None,
                    "free_throws_percentage": None,
                    "offensive_rebounds": None,
                    "total_rebounds": None,
                    "assists": None,
                    "steals": None,
                    "blocks": None,
                    "turnovers": None,
                    "personal_fouls": None,
                    "opponent_field_goals": None,
                    "opponent_field_goals_attempted": None,
                    "opponent_field_goals_percentage": None,
                    "opponent_three_point_field_goals": None,
                    "opponent_three_point_field_goals_attempted": None,
                    "opponent_three_point_field_goals_percentage": None,
                    "opponent_free_throws": None,
                    "opponent_free_throws_attempted": None,
                    "opponent_free_throws_percentage": None,
                    "opponent_offensive_rebounds": None,
                    "opponent_total_rebounds": None,
                    "opponent_assists": None,
                    "opponent_steals": None,
                    "opponent_blocks": None,
                    "opponent_turnovers": None,
                    "opponent_personal_fouls": None
                })

            time.sleep(random.uniform(3, 4))

        print(f'Scraped {len(next_games)} next games')

        # return next_games
        return next_games

if __name__ == "__main__":
    start_time = time.time()

    # get the current year
    current_year = None
    if datetime.now().month >= 9:
        current_year = datetime.now().year + 1
    else:
        current_year = datetime.now().year
    print(f'Current year: {current_year}')

    scraper = Scraper(use_crawlbase=True)
    scraper.scrape_nba_data(current_year, current_year)

    print("Data scraping completed and saved to nba_games.csv")
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(f'done processing data, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')