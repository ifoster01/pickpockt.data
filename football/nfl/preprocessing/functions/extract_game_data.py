# ./functions/extract_game_data.py
import re
import statistics
from bs4 import BeautifulSoup, Comment
from functions.general import get_team_from_name

def extract_game_data(res_or_html, team, opp):
    """
    Extract detailed game-level stats for both teams from a Pro-Football-Reference
    play-by-play page. Possession tracking is based on coin toss + divider/newhalf/overtime
    markers, not on posteam columns.
    
    Args:
        res_or_html: requests.Response or raw HTML string
        team (str): e.g., 'BUF'
        opp (str): e.g., 'CRD'

    Returns:
        dict: {team: {feature: value, ...}, opp: {feature: value, ...}}
    """
    # Handle either Response or HTML string
    if hasattr(res_or_html, "content"):
        html = res_or_html.content
    else:
        html = res_or_html

    soup = BeautifulSoup(html, "html.parser")

    # Extract the commented PBP table
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    pbp_soup = None
    for c in comments:
        if 'id="pbp"' in c or 'id="div_pbp"' in c:
            pbp_soup = BeautifulSoup(c, "html.parser")
            break
    if pbp_soup is None:
        return {team: {}, opp: {}}

    data_rows = pbp_soup.find_all("tr")
    team_w_ball = team
    starting_team = []
    possessions = []  # (team_code, play_dict)

    for row in data_rows:
        detail_td = row.find("td", {"data-stat": "detail"})
        if not detail_td:
            continue
        detail_text = detail_td.text.strip().lower()

        # Detect coin toss & first possession
        if "coin toss" in detail_text:
            try:
                starting_team_name = detail_text.split("to receive")[0].split(" ")[-2]
                starting_team_code = get_team_from_name(starting_team_name)
                starting_team.append(starting_team_code)
                team_w_ball = team if starting_team[0] == team else opp
                continue
            except Exception:
                pass

        # Handle quarter/half transitions
        if row.get("class") and "divider" in row.get("class"):
            team_w_ball = team if team_w_ball == opp else opp
            continue
        onecell = row.find("td", {"data-stat": "onecell"})
        if onecell:
            txt = onecell.text.strip()
            if "3rd Quarter" in txt:
                team_w_ball = team if (starting_team and starting_team[0] == opp) else opp
                continue
            if "Overtime" in txt and "End of Overtime" not in txt:
                if len(starting_team) > 1:
                    team_w_ball = starting_team[1]
                else:
                    team_w_ball = team if team_w_ball == opp else opp
                continue

        # Check if play changes possession
        if any(word in detail_text for word in ["punts", "intercepted", "kicks off", "turnover on downs"]):
            possessions.append((team_w_ball, {"detail": detail_text, "row": row}))
            team_w_ball = team if team_w_ball == opp else opp
            continue
        if "fumble" in detail_text:
            # Determine recovery
            recovery_match = re.search(r"recovered by\s+([A-Z]{2,3})", detail_text.upper())
            if recovery_match:
                recovered_team = recovery_match.group(1)
                if recovered_team != team_w_ball:
                    possessions.append((team_w_ball, {"detail": detail_text, "row": row}))
                    team_w_ball = recovered_team
                    continue

        # Append play to possession list
        possessions.append((team_w_ball, {"detail": detail_text, "row": row}))

    # Helper to initialize stat dict
    def blank_stats():
        return {
            "epa_per_play": 0.0,
            "epa_sum": 0.0,
            "epa_std": 0.0,
            "success_rate": 0.0,
            "explosive_15": 0.0,
            "explosive_20": 0.0,
            "explosive_30": 0.0,
            "ints": 0,
            "fumbles": 0,
            "fumbles_lost": 0,
            "turnovers_total": 0,
            "penalties": 0,
            "penalty_yards": 0,
            "sacks": 0,
            "sack_yards": 0,
            "third_down_conv": 0.0,
            "third_down_short_conv": 0.0,
            "third_down_med_conv": 0.0,
            "third_down_long_conv": 0.0,
            "fourth_down_conv": 0.0,
            "red_zone_td_pct": 0.0,
            "avg_plays_per_drive": 0.0,
            "avg_yards_per_drive": 0.0,
            "scoring_rate_per_drive": 0.0,
            "plays_per_minute": 0.0,
            "avg_start_pos": 0.0,
            "total_plays": 0,
            # New enhanced statistics
            "efficiency_when_leading": 0.0,
            "efficiency_when_trailing": 0.0,
            "efficiency_close_games": 0.0,
            "goal_line_td_rate": 0.0,
            "short_yardage_conv_rate": 0.0,
            "red_zone_fg_rate": 0.0,
            "qb_pressure_rate": 0.0,
            "pressure_to_sack_rate": 0.0,
            "turnover_epa_impact": 0.0,
            "short_field_scores": 0,
            "fumble_recovery_rate": 0.0,
            "punt_net_average": 0.0,
            "kick_return_average": 0.0,
            "field_position_advantage": 0.0,
            "time_of_possession": 0.0,
            # Tracking counters for calculations
            "epa_values": [],
            "successful_plays": 0,
            "third_down_attempts": 0,
            "third_down_short_attempts": 0,
            "third_down_med_attempts": 0,
            "third_down_long_attempts": 0,
            "fourth_down_attempts": 0,
            "red_zone_attempts": 0,
            "red_zone_tds": 0,
            "drives": [],  # List of drive data
            "scoring_drives": 0,
            # Time of possession and game flow
            "total_possession_time": 0.0,
            "possession_seconds": [],  # Track individual possession times
            "plays_in_hurry_up": 0,
            "pace_seconds": [],  # Time between plays
            # Situational efficiency
            "epa_when_leading": [],
            "epa_when_trailing": [],
            "epa_when_close": [],  # Within 7 points
            "plays_when_leading": 0,
            "plays_when_trailing": 0,
            "plays_when_close": 0,
            # Enhanced red zone and short yardage
            "goal_line_attempts": 0,  # Inside 5-yard line
            "goal_line_tds": 0,
            "short_yardage_attempts": 0,  # 3rd/4th and 1-2 yards
            "short_yardage_conv": 0,
            "red_zone_fgs": 0,
            # Pass rush and pressure
            "qb_pressures": 0,
            "qb_hurries": 0,
            "qb_hits": 0,
            "qb_dropbacks": 0,
            # Turnover context
            "turnover_epa_impact": 0.0,
            "short_field_scores": 0,  # Scores with <50 yards to go
            "fumble_recoveries": 0,
            "fumble_opportunities": 0,
            # Special teams
            "punt_yards": 0,
            "punt_attempts": 0,
            "kickoff_return_yards": 0,
            "kickoff_returns": 0,
            "field_position_starts": []  # Starting field positions
        }

    stats = {team: blank_stats(), opp: blank_stats()}
    team_plays = {team: [], opp: []}
    drives = {team: [], opp: []}  # Track drives for each team
    current_drive = {"plays": [], "team": None, "start_pos": None, "end_result": None}
    
    # Helper function to parse field position
    def parse_field_position(location_text, possession_team):
        """Convert location like 'BUF 35' to yards from own goal line for possession team"""
        if not location_text or location_text.strip() == "":
            return None
        
        location_text = location_text.strip()
        # Handle formats like "BUF 35" or "ARI 45"
        parts = location_text.split()
        if len(parts) == 2:
            field_team, yard_line = parts[0].upper(), int(parts[1])
            possession_team_upper = possession_team.upper()
            
            # Convert to yards from possession team's goal line
            if field_team == possession_team_upper:
                return yard_line  # Own territory
            else:
                return 100 - yard_line  # Opponent territory
        return None
    
    # Helper function to parse game time
    def parse_game_time(quarter, time_remaining):
        """Convert quarter and time remaining to total game seconds elapsed"""
        if not quarter or not time_remaining:
            return None
        try:
            quarter_num = int(quarter)
            # Parse time like "14:26" or "2:00"
            time_parts = time_remaining.split(':')
            if len(time_parts) == 2:
                minutes, seconds = int(time_parts[0]), int(time_parts[1])
                quarter_seconds_remaining = minutes * 60 + seconds
                
                # Calculate total elapsed seconds
                if quarter_num <= 4:
                    elapsed_seconds = (quarter_num - 1) * 900 + (900 - quarter_seconds_remaining)
                else:  # Overtime
                    elapsed_seconds = 3600 + (quarter_num - 5) * 900 + (900 - quarter_seconds_remaining)
                    
                return elapsed_seconds
        except (ValueError, IndexError):
            pass
        return None
    
    # Helper function to determine if play was successful
    def is_successful_play(down, yds_to_go, yards_gained, epa):
        """Determine if a play was successful based on down and distance"""
        if epa and epa > 0:
            return True
        if down == 1 and yards_gained >= yds_to_go * 0.5:
            return True
        elif down == 2 and yards_gained >= yds_to_go * 0.7:
            return True
        elif down in [3, 4] and yards_gained >= yds_to_go:
            return True
        return False
    
    # Helper function to end current drive
    def end_drive():
        if current_drive["plays"] and current_drive["team"]:
            drives[current_drive["team"]].append(current_drive.copy())
            current_drive["plays"] = []
            current_drive["team"] = None
            current_drive["start_pos"] = None
            current_drive["end_result"] = None

    # Iterate plays to populate stats
    for poss_team, play in possessions:
        row = play["row"]
        txt = play["detail"]
        
        # Extract all data from the row
        quarter_td = row.find("td", {"data-stat": "quarter"})
        time_td = row.find("td", {"data-stat": "qtr_time_remain"})
        down_td = row.find("td", {"data-stat": "down"})
        yds_to_go_td = row.find("td", {"data-stat": "yds_to_go"})
        location_td = row.find("td", {"data-stat": "location"})
        exp_pts_before_td = row.find("td", {"data-stat": "exp_pts_before"})
        exp_pts_after_td = row.find("td", {"data-stat": "exp_pts_after"})
        
        # Skip plays without basic data
        if not down_td or not down_td.text.strip():
            continue
            
        stats[poss_team]["total_plays"] += 1
        team_plays[poss_team].append(play)
        
        # Extract EPA values
        epa_value = None
        if exp_pts_before_td and exp_pts_after_td:
            try:
                epb = float(exp_pts_before_td.text.strip())
                epa_after = float(exp_pts_after_td.text.strip())
                epa_value = epa_after - epb
                stats[poss_team]["epa_values"].append(epa_value)
                stats[poss_team]["epa_sum"] += epa_value
            except (ValueError, AttributeError):
                pass
        
        # Extract down and distance
        down = None
        yds_to_go = None
        try:
            down = int(down_td.text.strip())
        except (ValueError, AttributeError):
            pass
        try:
            yds_to_go = int(yds_to_go_td.text.strip())
        except (ValueError, AttributeError):
            pass
        
        # Extract field position
        field_pos = None
        if location_td:
            field_pos = parse_field_position(location_td.text, poss_team)
            
        # Extract game time
        game_time = None
        if quarter_td and time_td:
            quarter_text = quarter_td.text.strip() if hasattr(quarter_td, 'text') else str(quarter_td)
            time_text = time_td.get_text(strip=True) if time_td else ""
            # Remove any HTML tags/links
            if time_text:
                time_text = time_text.split('>')[-1] if '>' in time_text else time_text
            game_time = parse_game_time(quarter_text, time_text)
        
        # Extract yards gained from play description
        yards_gained = 0
        yards_match = re.search(r"for\s+(\d+)\s+yard", txt)
        if yards_match:
            yards_gained = int(yards_match.group(1))
        elif "for no gain" in txt or "for 0 yards" in txt:
            yards_gained = 0
        elif "for loss of" in txt:
            loss_match = re.search(r"for\s+loss\s+of\s+(\d+)", txt)
            if loss_match:
                yards_gained = -int(loss_match.group(1))
        
        # Extract current score for situational analysis
        home_score = 0
        away_score = 0
        home_score_td = row.find("td", {"data-stat": "pbp_score_hm"})
        away_score_td = row.find("td", {"data-stat": "pbp_score_aw"})
        try:
            if home_score_td and home_score_td.text.strip():
                home_score = int(home_score_td.text.strip())
            if away_score_td and away_score_td.text.strip():
                away_score = int(away_score_td.text.strip())
        except ValueError:
            pass
            
        # Determine if team is leading, trailing, or close
        if poss_team == team:  # If possession team is our target team
            score_diff = home_score - away_score if team != "buf" else away_score - home_score
        else:  # If possession team is opponent
            score_diff = away_score - home_score if team != "buf" else home_score - away_score
            
        is_leading = score_diff > 0
        is_trailing = score_diff < 0
        is_close = abs(score_diff) <= 7
        
        # Drive tracking
        if current_drive["team"] != poss_team:
            end_drive()  # End previous drive
            current_drive["team"] = poss_team
            current_drive["start_pos"] = field_pos
            current_drive["start_time"] = game_time
            if field_pos:
                stats[poss_team]["field_position_starts"].append(field_pos)
            
        current_drive["plays"].append({
            "detail": txt,
            "down": down,
            "yds_to_go": yds_to_go,
            "yards_gained": yards_gained,
            "field_pos": field_pos,
            "epa": epa_value,
            "game_time": game_time,
            "score_diff": score_diff
        })
        
        # Success rate calculation
        if down and yds_to_go is not None:
            if is_successful_play(down, yds_to_go, yards_gained, epa_value):
                stats[poss_team]["successful_plays"] += 1
                
        # Situational efficiency tracking
        if epa_value is not None:
            if is_leading:
                stats[poss_team]["epa_when_leading"].append(epa_value)
                stats[poss_team]["plays_when_leading"] += 1
            elif is_trailing:
                stats[poss_team]["epa_when_trailing"].append(epa_value)
                stats[poss_team]["plays_when_trailing"] += 1
            if is_close:
                stats[poss_team]["epa_when_close"].append(epa_value)
                stats[poss_team]["plays_when_close"] += 1
        
        # Explosive plays
        if yards_gained >= 15:
            stats[poss_team]["explosive_15"] += 1
        if yards_gained >= 20:
            stats[poss_team]["explosive_20"] += 1
        if yards_gained >= 30:
            stats[poss_team]["explosive_30"] += 1

        # Enhanced turnover tracking with context
        if "intercepted" in txt:
            stats[poss_team]["ints"] += 1
            stats[poss_team]["turnovers_total"] += 1
            # Track EPA impact of turnover
            if epa_value is not None:
                stats[poss_team]["turnover_epa_impact"] += epa_value
                
        if "fumble" in txt:
            stats[poss_team]["fumbles"] += 1
            stats[poss_team]["fumble_opportunities"] += 1
            if "recovered by" in txt:
                if poss_team.upper() not in txt.upper():
                    stats[poss_team]["fumbles_lost"] += 1
                    stats[poss_team]["turnovers_total"] += 1
                    if epa_value is not None:
                        stats[poss_team]["turnover_epa_impact"] += epa_value
                else:
                    stats[poss_team]["fumble_recoveries"] += 1
                    
        # Track short field scoring opportunities
        if ("touchdown" in txt or "field goal" in txt) and field_pos and field_pos >= 50:
            stats[poss_team]["short_field_scores"] += 1

        # Penalties
        if "penalty" in txt:
            stats[poss_team]["penalties"] += 1
            pen_yards = re.search(r"(\d+)\s+yard", txt)
            if pen_yards:
                stats[poss_team]["penalty_yards"] += int(pen_yards.group(1))

        # Sacks and QB pressure
        if "sacked" in txt:
            stats[poss_team]["sacks"] += 1
            stats[poss_team]["qb_pressures"] += 1
            if yards_gained < 0:
                stats[poss_team]["sack_yards"] += yards_gained  # yards_gained is already negative
                
        # Track QB dropbacks and pressure
        if "pass" in txt and "complete" in txt or "incomplete" in txt:
            stats[poss_team]["qb_dropbacks"] += 1
            
        # QB pressure indicators in play description
        if any(pressure_word in txt.lower() for pressure_word in ["hurried", "rushed", "under pressure"]):
            stats[poss_team]["qb_hurries"] += 1
            stats[poss_team]["qb_pressures"] += 1
            
        if "hit" in txt and ("qb" in txt.lower() or "quarterback" in txt.lower()):
            stats[poss_team]["qb_hits"] += 1
            stats[poss_team]["qb_pressures"] += 1

        # Down conversions tracking
        if down == 3:
            stats[poss_team]["third_down_attempts"] += 1
            if yds_to_go and yds_to_go <= 3:
                stats[poss_team]["third_down_short_attempts"] += 1
            elif yds_to_go and 4 <= yds_to_go <= 6:
                stats[poss_team]["third_down_med_attempts"] += 1
            elif yds_to_go and yds_to_go >= 7:
                stats[poss_team]["third_down_long_attempts"] += 1
                
            if "first down" in txt or yards_gained >= yds_to_go:
                stats[poss_team]["third_down_conv"] += 1
                if yds_to_go and yds_to_go <= 3:
                    stats[poss_team]["third_down_short_conv"] += 1
                elif yds_to_go and 4 <= yds_to_go <= 6:
                    stats[poss_team]["third_down_med_conv"] += 1
                elif yds_to_go and yds_to_go >= 7:
                    stats[poss_team]["third_down_long_conv"] += 1
                    
        elif down == 4:
            stats[poss_team]["fourth_down_attempts"] += 1
            if "first down" in txt or yards_gained >= yds_to_go:
                stats[poss_team]["fourth_down_conv"] += 1

        # Enhanced red zone and short yardage tracking
        if field_pos:
            # Red zone (20 yards or less)
            if field_pos >= 80:  # 20 yards from goal line
                if down == 1:  # Only count on first down to avoid double counting
                    stats[poss_team]["red_zone_attempts"] += 1
                if "touchdown" in txt:
                    stats[poss_team]["red_zone_tds"] += 1
                    current_drive["end_result"] = "touchdown"
                elif "field goal" in txt:
                    stats[poss_team]["red_zone_fgs"] += 1
                    
            # Goal line (5 yards or less)
            if field_pos >= 95:  # 5 yards from goal line
                if down == 1:
                    stats[poss_team]["goal_line_attempts"] += 1
                if "touchdown" in txt:
                    stats[poss_team]["goal_line_tds"] += 1
                    
        # Short yardage situations (3rd/4th and 1-2 yards)
        if down in [3, 4] and yds_to_go and yds_to_go <= 2:
            stats[poss_team]["short_yardage_attempts"] += 1
            if "first down" in txt or yards_gained >= yds_to_go:
                stats[poss_team]["short_yardage_conv"] += 1
                
        # Special teams tracking
        if "punt" in txt and "yards" in txt:
            punt_yards_match = re.search(r"punts\s+(\d+)\s+yard", txt)
            if punt_yards_match:
                punt_yards = int(punt_yards_match.group(1))
                stats[poss_team]["punt_yards"] += punt_yards
                stats[poss_team]["punt_attempts"] += 1
                
        if "kicks off" in txt and "returned" in txt:
            return_yards_match = re.search(r"for\s+(\d+)\s+yard", txt)
            if return_yards_match:
                return_yards = int(return_yards_match.group(1))
                # This gets credited to the receiving team
                receiving_team = team if poss_team == opp else opp
                stats[receiving_team]["kickoff_return_yards"] += return_yards
                stats[receiving_team]["kickoff_returns"] += 1
                
        # Check for scoring plays
        if "touchdown" in txt or "field goal" in txt:
            stats[poss_team]["scoring_drives"] += 1
            current_drive["end_result"] = "score"
    
    # End the last drive
    end_drive()
    
    # Calculate final statistics
    for t in [team, opp]:
        total = stats[t]["total_plays"]
        
        # EPA calculations
        if stats[t]["epa_values"]:
            stats[t]["epa_per_play"] = stats[t]["epa_sum"] / len(stats[t]["epa_values"])
            if len(stats[t]["epa_values"]) > 1:
                stats[t]["epa_std"] = statistics.stdev(stats[t]["epa_values"])
        
        # Success rate
        if total > 0:
            stats[t]["success_rate"] = stats[t]["successful_plays"] / total
            
        # Normalize explosive plays
        if total > 0:
            stats[t]["explosive_15"] /= total
            stats[t]["explosive_20"] /= total
            stats[t]["explosive_30"] /= total
        
        # Down conversion rates
        if stats[t]["third_down_attempts"] > 0:
            stats[t]["third_down_conv"] /= stats[t]["third_down_attempts"]
        if stats[t]["third_down_short_attempts"] > 0:
            stats[t]["third_down_short_conv"] /= stats[t]["third_down_short_attempts"]
        if stats[t]["third_down_med_attempts"] > 0:
            stats[t]["third_down_med_conv"] /= stats[t]["third_down_med_attempts"]
        if stats[t]["third_down_long_attempts"] > 0:
            stats[t]["third_down_long_conv"] /= stats[t]["third_down_long_attempts"]
        if stats[t]["fourth_down_attempts"] > 0:
            stats[t]["fourth_down_conv"] /= stats[t]["fourth_down_attempts"]
        
        # Red zone TD percentage
        if stats[t]["red_zone_attempts"] > 0:
            stats[t]["red_zone_td_pct"] = stats[t]["red_zone_tds"] / stats[t]["red_zone_attempts"]
        
        # Calculate situational efficiency
        if stats[t]["epa_when_leading"]:
            stats[t]["efficiency_when_leading"] = sum(stats[t]["epa_when_leading"]) / len(stats[t]["epa_when_leading"])
        else:
            stats[t]["efficiency_when_leading"] = 0.0
            
        if stats[t]["epa_when_trailing"]:
            stats[t]["efficiency_when_trailing"] = sum(stats[t]["epa_when_trailing"]) / len(stats[t]["epa_when_trailing"])
        else:
            stats[t]["efficiency_when_trailing"] = 0.0
            
        if stats[t]["epa_when_close"]:
            stats[t]["efficiency_close_games"] = sum(stats[t]["epa_when_close"]) / len(stats[t]["epa_when_close"])
        else:
            stats[t]["efficiency_close_games"] = 0.0
        
        # Enhanced red zone and short yardage rates
        if stats[t]["goal_line_attempts"] > 0:
            stats[t]["goal_line_td_rate"] = stats[t]["goal_line_tds"] / stats[t]["goal_line_attempts"]
        else:
            stats[t]["goal_line_td_rate"] = 0.0
            
        if stats[t]["short_yardage_attempts"] > 0:
            stats[t]["short_yardage_conv_rate"] = stats[t]["short_yardage_conv"] / stats[t]["short_yardage_attempts"]
        else:
            stats[t]["short_yardage_conv_rate"] = 0.0
            
        if stats[t]["red_zone_attempts"] > 0:
            stats[t]["red_zone_fg_rate"] = stats[t]["red_zone_fgs"] / stats[t]["red_zone_attempts"]
        else:
            stats[t]["red_zone_fg_rate"] = 0.0
        
        # Pass rush and pressure rates
        if stats[t]["qb_dropbacks"] > 0:
            stats[t]["qb_pressure_rate"] = stats[t]["qb_pressures"] / stats[t]["qb_dropbacks"]
        else:
            stats[t]["qb_pressure_rate"] = 0.0
            
        if stats[t]["qb_pressures"] > 0:
            stats[t]["pressure_to_sack_rate"] = stats[t]["sacks"] / stats[t]["qb_pressures"]
        else:
            stats[t]["pressure_to_sack_rate"] = 0.0
        
        # Turnover context
        if stats[t]["fumble_opportunities"] > 0:
            stats[t]["fumble_recovery_rate"] = stats[t]["fumble_recoveries"] / stats[t]["fumble_opportunities"]
        else:
            stats[t]["fumble_recovery_rate"] = 0.0
        
        # Special teams averages
        if stats[t]["punt_attempts"] > 0:
            stats[t]["punt_net_average"] = stats[t]["punt_yards"] / stats[t]["punt_attempts"]
        else:
            stats[t]["punt_net_average"] = 0.0
            
        if stats[t]["kickoff_returns"] > 0:
            stats[t]["kick_return_average"] = stats[t]["kickoff_return_yards"] / stats[t]["kickoff_returns"]
        else:
            stats[t]["kick_return_average"] = 0.0
        
        # Drive-level statistics
        team_drives = drives[t]
        if team_drives:
            total_drive_plays = sum(len(drive["plays"]) for drive in team_drives)
            total_drive_yards = 0
            total_drive_time = 0
            
            for drive in team_drives:
                drive_yards = sum(play["yards_gained"] for play in drive["plays"] if play["yards_gained"] is not None)
                total_drive_yards += drive_yards
                
                # Calculate drive time if we have start and end times
                drive_times = [play["game_time"] for play in drive["plays"] if play["game_time"] is not None]
                if len(drive_times) >= 2:
                    drive_time = max(drive_times) - min(drive_times)
                    total_drive_time += drive_time
            
            stats[t]["avg_plays_per_drive"] = total_drive_plays / len(team_drives)
            stats[t]["avg_yards_per_drive"] = total_drive_yards / len(team_drives)
            
            if len(team_drives) > 0:
                stats[t]["scoring_rate_per_drive"] = stats[t]["scoring_drives"] / len(team_drives)
            
            # Time of possession (approximate)
            if total_drive_time > 0:
                stats[t]["time_of_possession"] = total_drive_time / 60.0  # Convert to minutes
            
            # Calculate average starting position
            if stats[t]["field_position_starts"]:
                stats[t]["avg_start_pos"] = sum(stats[t]["field_position_starts"]) / len(stats[t]["field_position_starts"])
            else:
                stats[t]["avg_start_pos"] = 0.0
                
            # Field position advantage (relative to 50-yard line)
            if stats[t]["field_position_starts"]:
                stats[t]["field_position_advantage"] = stats[t]["avg_start_pos"] - 50.0
            else:
                stats[t]["field_position_advantage"] = 0.0
        else:
            # If no drives, set defaults
            stats[t]["avg_plays_per_drive"] = 0.0
            stats[t]["avg_yards_per_drive"] = 0.0
            stats[t]["scoring_rate_per_drive"] = 0.0
            stats[t]["time_of_possession"] = 0.0
            stats[t]["avg_start_pos"] = 0.0
            stats[t]["field_position_advantage"] = 0.0
        
        # Clean up helper fields
        del stats[t]["epa_values"]
        del stats[t]["successful_plays"]
        del stats[t]["third_down_attempts"]
        del stats[t]["third_down_short_attempts"]
        del stats[t]["third_down_med_attempts"]
        del stats[t]["third_down_long_attempts"]
        del stats[t]["fourth_down_attempts"]
        del stats[t]["red_zone_attempts"]
        del stats[t]["red_zone_tds"]
        del stats[t]["drives"]
        del stats[t]["scoring_drives"]
        del stats[t]["possession_seconds"]
        del stats[t]["pace_seconds"]
        del stats[t]["epa_when_leading"]
        del stats[t]["epa_when_trailing"]
        del stats[t]["epa_when_close"]
        del stats[t]["plays_when_leading"]
        del stats[t]["plays_when_trailing"]
        del stats[t]["plays_when_close"]
        del stats[t]["goal_line_attempts"]
        del stats[t]["goal_line_tds"]
        del stats[t]["short_yardage_attempts"]
        del stats[t]["short_yardage_conv"]
        del stats[t]["red_zone_fgs"]
        del stats[t]["qb_dropbacks"]
        del stats[t]["fumble_opportunities"]
        del stats[t]["fumble_recoveries"]
        del stats[t]["punt_yards"]
        del stats[t]["punt_attempts"]
        del stats[t]["kickoff_return_yards"]
        del stats[t]["kickoff_returns"]
        del stats[t]["field_position_starts"]

    return stats
