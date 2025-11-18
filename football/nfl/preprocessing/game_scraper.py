import requests, time, random, json, re
from bs4 import BeautifulSoup, Comment
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from functions.general import get_team_from_name
from functions.extract_game_data import extract_game_data
from datetime import datetime
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
        self.nflTeams = [
            'crd', 'atl', 'rav', 'buf', 'car', 'chi', 'cin', 'cle', 'dal', 'den',
            'det', 'gnb', 'htx', 'clt', 'jax', 'kan', 'rai', 'sdg', 'ram', 'mia',
            'min', 'nwe', 'nor', 'nyg', 'nyj', 'phi', 'pit', 'sfo', 'sea', 'tam',
            'oti', 'was',
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
    
    def get_current_week(self, soup: BeautifulSoup):
        """
        Get the current week for a given team and year
        """
        time.sleep(3)
        game_rows = soup.find("tbody").find_all("tr")
        current_week = None
        for tr in game_rows:
            week_elem = tr.find("th", {"data-stat": "week_num"})
            
            if (not week_elem) or (week_elem and week_elem.text.strip() in ['Bye Week', 'Playoffs']):
                continue

            outcome_elem = tr.find("td", {"data-stat": "game_outcome"})
            # None indicates future/unplayed game
            outcome_text = outcome_elem.text.strip() if outcome_elem else ""
            win = 1 if outcome_text == "W" else 0 if outcome_text == "L" else None
            
            if win is None:
                current_week = week_elem.text.strip()
                break
        
        return int(current_week) if current_week else None

    def get_game_data(self, team, year, look_back_weeks=None):
        print(f"Scraping {team} {year}")
        url = f"https://www.pro-football-reference.com/teams/{team}/{year}.htm"

        try:
            print(f"Requesting URL: {url} via Crawlbase Smart AI Proxy")

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
                response = self.session.get(url)
                html_content = response.content

        except Exception as e:
            print(f"Crawlbase request failed for {team} {year}: {e}")
            return []

        soup = BeautifulSoup(html_content, "html.parser")

        # Find the game log table - it should be in a table with tbody containing game rows
        game_table = None
        for table in soup.find_all("table"):
            if table.find("th", {"data-stat": "week_num"}):
                game_table = table
                break

        if not game_table:
            print(f"Could not find game log table for {team} {year}")
            return []

        # Find all game rows - they should be in tbody and have week_num data
        game_rows = game_table.find("tbody").find_all("tr")
        games = []

        # get the current week
        current_week = None
        if look_back_weeks is not None:
            current_week = self.get_current_week(game_table)
        print(f"Current week: {current_week}")

        for tr in game_rows:
            week_elem = tr.find("th", {"data-stat": "week_num"})

            # Skip non-game rows and playoff summary rows
            if (not week_elem) or (week_elem and week_elem.text.strip() in ['Bye Week', 'Playoffs']):
                continue

            current_loop_week = int(week_elem.text.strip())
            print(f"Current loop week: {current_loop_week}")
            if current_week is not None and current_loop_week < current_week - look_back_weeks:
                continue

            # Extract game data
            try:
                # Get date
                date_elem = tr.find("td", {"data-stat": "game_date"})
                date = date_elem.get('csk') if date_elem and date_elem.get('csk') else date_elem.text.strip() if date_elem else ""
                if date == '' or date == 'Playoffs':
                    continue

                # Get time (avoid shadowing the time module)
                time_elem = tr.find("td", {"data-stat": "game_time"})
                game_time = time_elem.text.strip() if time_elem else ""

                # Get opponent
                opp_elem = tr.find("td", {"data-stat": "opp"})
                opponent = ""
                if opp_elem:
                    opp_link = opp_elem.find("a")
                    if opp_link and opp_link.get('href'):
                        # Extract team code from href like "/teams/buf/2024.htm"
                        href = opp_link.get('href')
                        opponent = href.split('/')[2] if len(href.split('/')) > 2 else opp_elem.text.strip()
                    else:
                        opponent = opp_elem.text.strip()

                # Get home/away status
                location_elem = tr.find("td", {"data-stat": "game_location"})
                home = not (location_elem and location_elem.text.strip() == "@")

                # Get win/loss
                outcome_elem = tr.find("td", {"data-stat": "game_outcome"})
                # None indicates future/unplayed game
                outcome_text = outcome_elem.text.strip() if outcome_elem else ""
                win = 1 if outcome_text == "W" else 0 if outcome_text == "L" else None

                # Get scores
                pts_off_elem = tr.find("td", {"data-stat": "pts_off"})
                pts_def_elem = tr.find("td", {"data-stat": "pts_def"})
                points = pts_off_elem.text.strip() if pts_off_elem else ""
                opponent_points = pts_def_elem.text.strip() if pts_def_elem else ""

                # Get offensive stats
                first_down_off_elem = tr.find("td", {"data-stat": "first_down_off"})
                yards_off_elem = tr.find("td", {"data-stat": "yards_off"})
                pass_yds_off_elem = tr.find("td", {"data-stat": "pass_yds_off"})
                rush_yds_off_elem = tr.find("td", {"data-stat": "rush_yds_off"})
                to_off_elem = tr.find("td", {"data-stat": "to_off"})

                # Get defensive stats
                first_down_def_elem = tr.find("td", {"data-stat": "first_down_def"})
                yards_def_elem = tr.find("td", {"data-stat": "yards_def"})
                pass_yds_def_elem = tr.find("td", {"data-stat": "pass_yds_def"})
                rush_yds_def_elem = tr.find("td", {"data-stat": "rush_yds_def"})
                to_def_elem = tr.find("td", {"data-stat": "to_def"})

                # Get boxscore link for PBP (be robust to structure changes)
                boxscore_td = tr.find("td", {"data-stat": "boxscore_word"})
                boxscore_link = None
                if boxscore_td:
                    a_tag = boxscore_td.find("a", href=True)
                    if not a_tag:
                        # Fallback: any boxscore-like link in the row
                        a_tag = tr.find("a", href=re.compile(r"^/boxscores/"))
                    if a_tag:
                        boxscore_link = a_tag.get('href')

                game_row = {
                    "team": team,
                    "week": week_elem.text.strip(),
                    "date": date,
                    "time": game_time,
                    "opponent": opponent.lower(),
                    "home": home,
                    "win": win if win is not None else "",
                    "points": points,
                    "opponent_points": opponent_points,
                    "first_downs_off": first_down_off_elem.text.strip() if first_down_off_elem else "",
                    "total_yards_off": yards_off_elem.text.strip() if yards_off_elem else "",
                    "pass_yards_off": pass_yds_off_elem.text.strip() if pass_yds_off_elem else "",
                    "rush_yards_off": rush_yds_off_elem.text.strip() if rush_yds_off_elem else "",
                    "turnovers_off": to_off_elem.text.strip() if to_off_elem else "",
                    "first_downs_def": first_down_def_elem.text.strip() if first_down_def_elem else "",
                    "total_yards_def": yards_def_elem.text.strip() if yards_def_elem else "",
                    "pass_yards_def": pass_yds_def_elem.text.strip() if pass_yds_def_elem else "",
                    "rush_yards_def": rush_yds_def_elem.text.strip() if rush_yds_def_elem else "",
                    "turnovers_def": to_def_elem.text.strip() if to_def_elem else "",
                }

                # If completed game and we have a boxscore link, fetch PBP and compute strong drives
                try:
                    # Treat ties as completed as well
                    if boxscore_link and outcome_text in ("W", "L", "T"):
                        # Print before sleeping to verify entry; force flush to avoid buffering
                        print(
                            f" --> Scraping PBP for {team} vs {game_row['opponent']} {game_row['date']}",
                            flush=True,
                        )
                        # Sleep to reduce rate limiting risk
                        time.sleep(random.uniform(3, 4))

                        pbp_url = f"https://www.pro-football-reference.com{boxscore_link}"
                        print(f"Requesting PBP URL: {pbp_url} via Crawlbase Smart AI Proxy")
                        
                        pbp_response = None
                        pbp_html_content = None
                        if self.use_crawlbase:
                            # Use Crawlbase for PBP requests as well
                            pbp_response = self.crawlbase_api.get(pbp_url)
                            if pbp_response is None or pbp_response['status_code'] != 200:
                                print(f"PBP request failed with status: {pbp_response['status_code']}")
                                continue
                            
                            # Get the HTML content from Crawlbase response
                            pbp_html_content = pbp_response['body']
                        else:
                            pbp_response = self.session.get(pbp_url)
                            pbp_html_content = pbp_response.content

                        # Use uppercase codes internally
                        team_upper = team.upper()
                        opp_upper = game_row['opponent'].upper()

                        # extract more granular game data
                        features = extract_game_data(pbp_html_content, team_upper, opp_upper)

                        # add team/opp prefixed features onto your game_row
                        game_row.update({f"team_{k}": v for k, v in features.get(team_upper, {}).items()})
                        game_row.update({f"opp_{k}": v for k, v in features.get(opp_upper, {}).items()})

                        # summery of the game
                        print(f" --> Summary of the game: {team} vs {game_row['opponent']} on {game_row['date']}")
                        print(f" --> {team} {game_row['points']} - {game_row['opponent_points']} {game_row['opponent']}")

                except requests.exceptions.RequestException as e:
                    # Network/PBP missing - leave as 0 but surface minimal context
                    print(
                        f" --> PBP request failed for {team} vs {game_row['opponent']} on {game_row['date']}: {e}",
                        flush=True,
                    )
                except Exception as e:
                    # Unexpected structure - leave as 0 but surface minimal context
                    print(
                        f" --> PBP parse failed for {team} vs {game_row['opponent']} on {game_row['date']}: {e}",
                        flush=True,
                    )

                games.append(game_row)

            except Exception as e:
                print(f"Error parsing game row: {e}")
                continue

        # remove any games where the opponent is a bye week
        games = [game for game in games if game['opponent'] != 'bye week']
        return games

    def extract_game_data(self, res, team, opp):
        """
        Extract strong drive counts from the PBP page. team/opp are uppercase codes (e.g., 'BUF').
        """
        try:
            soup = BeautifulSoup(res.content, "html.parser")
            # PFR embeds the PBP table inside HTML comments
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            idx = -1
            for i in range(len(comments)):
                if "<div class=\"table_container\" id=\"div_pbp\">" in comments[i]:
                    idx = i
                    break
            if idx == -1:
                return {team: 0, opp: 0}
            soup = BeautifulSoup(comments[idx], "html.parser")

            data_rows = soup.find_all("tr")
            team_w_ball = team
            locations = []
            starting_team = []
            for row in data_rows:
                details = row.find("td", {"data-stat": "detail"})

                if details and "coin toss" in details.text:
                    try:
                        starting_team_name = details.text.split("to receive")[0].split(" ")[-2]
                        starting_team.append(get_team_from_name(starting_team_name))
                        team_w_ball = team if starting_team[0] == team else opp
                        if len(starting_team) > 1:
                            continue
                    except Exception:
                        pass

                if row.get("class") and "divider" in row.get("class"):
                    locations.append("divider")

                if row.find("td", {"data-stat": "onecell"}) and "3rd Quarter" in row.find("td", {"data-stat": "onecell"}).text:
                    locations.append("newhalf")
                    continue

                if row.find("td", {"data-stat": "onecell"}) and "Overtime" in row.find("td", {"data-stat": "onecell"}).text and not "End of Overtime" in row.find("td", {"data-stat": "onecell"}).text:
                    locations.append("overtime")
                    continue

                yards = re.search(r'-?\d+(?!.*-?\d)', details.text) if details else None
                yards_str = yards.group(0) if yards else ""

                if details and "intercepted" in details.text:
                    yards_str += " interception"
                if details and "fumble" in details.text:
                    yards_str += " fumble"
                if details and "field goal" in details.text:
                    yards_str += " field goal"
                if details and "penalty" in details.text.lower() and not "(no play)" in details.text:
                    yards_str = "penalty"
                if details and "punts" in details.text:
                    if details and (details.text.split(" ")[-1] == "yards" or "returned" not in details.text):
                        yards_str = "punts"
                    else:
                        yards_str += " punts"
                if details and "kicks off" in details.text:
                    if details and "returned" not in details.text:
                        yards_str = "kickoff"

                locations.append(yards_str)

            locations = locations[1:]
            locations = [x for x in locations if x]

            num_strong_drives = {team: 0, opp: 0}
            for i in range(len(locations)):
                if locations[i] == "newhalf":
                    team_w_ball = team if (starting_team and starting_team[0] == opp) else opp
                    continue
                if locations[i] == "overtime":
                    team_w_ball = starting_team[1] if len(starting_team) > 1 else (team if team_w_ball == opp else opp)
                    continue
                if locations[i] == "divider" and not locations[i-1] == "newhalf" and not locations[i-1] == "overtime":
                    team_w_ball = team if team_w_ball == opp else opp
                    continue

                play_yrds = int(re.search(r'-?\d+(?!.*-?\d)', locations[i]).group(0)) if re.search(r'-?\d+(?!.*-?\d)', locations[i]) else 0
                if locations[i] and play_yrds >= 15:
                    if "interception" in locations[i]:
                        num_strong_drives[team if team_w_ball == opp else opp] += 1
                    elif "punts" in locations[i]:
                        num_strong_drives[team if team_w_ball == opp else opp] += 1
                    elif "fumble" in locations[i]:
                        pass
                    elif "field goal" in locations[i]:
                        pass
                    else:
                        num_strong_drives[team_w_ball] += 1
            
            return num_strong_drives
        except Exception:
            return {team: 0, opp: 0}

    def scrape_nfl_data(self, start_year, end_year, look_back_weeks=None):
        all_games = []
        for team in self.nflTeams:
            for year in range(start_year, end_year + 1):
                # Add a random delay between requests
                time.sleep(random.uniform(3, 4))

                # handling NFL team relocations and name changes
                # Most NFL teams have been stable, but handle a few cases
                if team == 'rai':
                    if year < 1995:
                        team = 'rai'  # Los Angeles Raiders
                    elif year < 2020:
                        team = 'rai'  # Oakland Raiders  
                    else:
                        team = 'rai'  # Las Vegas Raiders (same abbreviation)
                elif team == 'ram':
                    if year < 1995:
                        team = 'ram'  # Los Angeles Rams
                    elif year < 2016:
                        team = 'ram'  # St. Louis Rams
                    else:
                        team = 'ram'  # Los Angeles Rams (same abbreviation)
                elif team == 'sdg':
                    if year < 2017:
                        team = 'sdg'  # San Diego Chargers
                    else:
                        team = 'sdg'  # Los Angeles Chargers (keeping same abbreviation for consistency)

                stats = self.get_game_data(team, year, look_back_weeks)
                if not stats:
                    print(f"No stats for {team} {year}")
                    continue

                all_games.extend(stats)
        
        print(f'Scraped {len(all_games)} games')

        # read the old scraped data
        try:
            old_games_df = pd.read_csv("data/nfl_games.csv")
        except FileNotFoundError:
            old_games_df = pd.DataFrame()
        print(f'Read {len(old_games_df)} games from data/nfl_games.csv')
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
        print(f'Saved {len(combined_games)} games to data/nfl_games.csv')

        # sort by year (grouped), then team, then date ascending
        combined_games['year'] = combined_games['date'].dt.year
        combined_games = combined_games.sort_values(by=['year', 'team', 'date'], ascending=True)
        combined_games = combined_games.drop(columns=['year'])
        combined_games.to_csv("data/nfl_games.csv", index=False)

        return combined_games

if __name__ == "__main__":
    start_time = time.time()

    # get the current year
    current_year = datetime.now().year

    scraper = Scraper(use_crawlbase=True)
    scraper.scrape_nfl_data(current_year, current_year, look_back_weeks=1)

    print("Data scraping completed and saved to nfl_games.csv")
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    print(f'done processing data, took {int(hours)} hours, {int(minutes)} minutes, and {seconds:.2f} seconds')
    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')