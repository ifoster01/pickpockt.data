import pandas as pd
import json
from functions.general import condense_features
from datetime import timedelta

class Preprocessor:
    def __init__(self):
        self.json_data = self.get_all_fighter_histories()

    # gets the fighter's age at the time of a fight based on their DOB and the fight date
    def get_fighter_age(self, dob, fight_date):
        """
        Gets the fighter's age at the time of a fight based on their DOB and the fight date.
        Both dob and fight_date should be strings in a recognizable date format.
        Returns the age as an integer, or 'N/A' if parsing fails.
        """
        try:
            dob_parsed = pd.to_datetime(dob, errors='coerce')
            fight_date_parsed = pd.to_datetime(fight_date, errors='coerce')
            if pd.isna(dob_parsed) or pd.isna(fight_date_parsed):
                return 'N/A'
            # Calculate age
            age = fight_date_parsed.year - dob_parsed.year - (
                (fight_date_parsed.month, fight_date_parsed.day) < (dob_parsed.month, dob_parsed.day)
            )
            return age
        except Exception:
            return 'N/A'

    # create a json file with each fighter's fight history in date order as well as their age, height, reach, and stance
    def get_all_fighter_histories(self):
        # open fighter_stats csv
        fighter_stats = pd.read_csv('data/fighter_stats.csv')
        # open fight_stats csv
        fight_stats = pd.read_csv('data/fight_stats.csv')

        # initialize a list to store each fighter's fight history
        fighter_histories = []
        for index, row in fighter_stats.iterrows():
            fighter_name = row['name']
            fighter_history = fight_stats[(fight_stats['f1'] == fighter_name) | (fight_stats['f2'] == fighter_name)]
            fighter_history = fighter_history.sort_values(by='date', ascending=True)
            
            # Replace NaN values with empty strings
            fighter_history = fighter_history.fillna('')

            # if the fighter has no fight history, skip to the next fighter
            if len(fighter_history) == 0:
                continue
            
            fighter_history_list = fighter_history.to_dict(orient='index')
            # sort the fighter's fight history by date
            fighter_history_list = {i: fighter_history_list[i] for i in sorted(fighter_history_list)}

            fighter_histories.append({
                fighter_name: {
                    'dob': row['DOB'] if pd.notna(row['DOB']) else '',
                    'height': row['Height'] if pd.notna(row['Height']) else '',
                    'reach': row['Reach'] if pd.notna(row['Reach']) else '',
                    'stance': row['STANCE'] if pd.notna(row['STANCE']) else '',
                    'fight_history': fighter_history_list
                }
            })

        return fighter_histories
        
    def compare_dates(self, date1, date2):
        """
        Safely converts dates to pd datetime objects and then compares them.
        Handles common date format issues and returns False if parsing fails.
        """

        d1 = self.standardize_date(date1)
        d2 = self.standardize_date(date2)

        if d1 is None or d2 is None:
            # Optionally, you could log a warning here
            return False

        return d1 < d2

    def standardize_date(self, date):
        """
        Takes in dates in any format and returns them as a pd.Timestamp object with UTC timezone.
        Tries common date formats and falls back to pandas' parser.
        Returns None if parsing fails.
        """
        import pandas as pd

        if pd.isna(date):
            return None

        # Try common date formats
        date_formats = ['%m-%d-%Y', '%Y-%m-%d', '%d-%Y-%m']
        for fmt in date_formats:
            try:
                # create timezone aware timestamp in UTC
                return pd.to_datetime(date, format=fmt, utc=True)
            except Exception:
                continue
        # Try pandas default parser as a last resort
        try:
            dt = pd.to_datetime(date, errors='raise')
            if dt.tzinfo is None:
                # if naive, assume UTC and convert
                return dt.tz_localize('UTC')
            else:
                # if aware, just convert
                return dt.tz_convert('UTC')
        except Exception:
            return None

    def in_last_x_years(self, date1, date2, num_years):
        """
        Checks if date1 is within the last `num_years` years of date2.

        Args:
            date1: str, datetime, or pd.Timestamp - the earlier date (e.g., fight date)
            date2: str, datetime, or pd.Timestamp - the later date (e.g., reference date)
            num_years: int - number of years to check

        Returns:
            bool: True if date1 is within the last `num_years` years of date2, False otherwise.
        """
        # Standardize both dates to pandas Timestamp
        d1 = self.standardize_date(date1)
        d2 = self.standardize_date(date2)

        if d1 is None or d2 is None:
            return False

        # If date1 is after date2, it's not "within the last x years"
        if d1 > d2:
            return False

        # Calculate the threshold date: date2 minus num_years
        try:
            threshold_date = d2 - pd.DateOffset(years=num_years)
        except Exception:
            # Fallback for very old pandas versions
            threshold_date = d2 - pd.Timedelta(days=365 * num_years)

        # Return True if date1 is on or after the threshold date
        return d1 >= threshold_date
        

    def convert_to_seconds(self, time):
        if time == '' or time == '--':
            return 0

        try:
            time = time.split(':')
        except:
            return 0

        return int(time[0]) * 60 + int(time[1])


    def get_fight_in_seconds(self, rounds, time):
        if rounds == '':
            return 0

        rounds = int(rounds)

        if time == '5:00':
            return rounds * 300
        
        return (rounds - 1) * 300 + self.convert_to_seconds(time)


    def get_weight_class_change(self, previous_weight_class, current_weight_class):
        if previous_weight_class == '':
            return 0

        # create a dictionary to map each weight class to a number
        weight_classes = {
            'Women\'s Strawweight': 0,
            'Strawweight': 1,
            'Women\'s Flyweight': 2,
            'Flyweight': 3,
            'Women\'s Bantamweight': 4,
            'Bantamweight': 5,
            'Women\'s Featherweight': 6,
            'Featherweight': 7,
            'Women\'s Lightweight': 8,
            'Lightweight': 9,
            'Women\'s Welterweight': 10,
            'Welterweight': 11,
            'Women\'s Middleweight': 12,
            'Middleweight': 13,
            'Women\'s Light Heavyweight': 14,
            'Light Heavyweight': 15,
            'Women\'s Heavyweight': 16,
            'Heavyweight': 17
        }

        if previous_weight_class not in weight_classes or current_weight_class not in weight_classes:
            return 0
        
        # return 1 if the current weight class is higher than the previous weight class, 0 if they are the same, and -1 if the current weight class is lower than the previous weight class
        return 1 if weight_classes[current_weight_class] > weight_classes[previous_weight_class] else 0 if weight_classes[current_weight_class] == weight_classes[previous_weight_class] else -1

    def cast_weight_class_to_int(self, weight_class):
        # create a dictionary to map each weight class to a number
        weight_classes = {
            'Women\'s Strawweight': 0,
            'Strawweight': 1,
            'Women\'s Flyweight': 2,
            'Flyweight': 3,
            'Women\'s Bantamweight': 4,
            'Bantamweight': 5,
            'Women\'s Featherweight': 6,
            'Featherweight': 7,
            'Women\'s Lightweight': 8,
            'Lightweight': 9,
            'Women\'s Welterweight': 10,
            'Welterweight': 11,
            'Women\'s Middleweight': 12,
            'Middleweight': 13,
            'Women\'s Light Heavyweight': 14,
            'Light Heavyweight': 15,
            'Women\'s Heavyweight': 16,
            'Heavyweight': 17
        }

        if weight_class not in weight_classes:
            return -1
        
        return weight_classes[weight_class]

    def get_fighter_stats(self, fighter_name, fight_date, fighter_history):
        # get the fighter's stats at the time of the fight
        weight_class = ''
        idx = 0
        for x in fighter_history['fight_history'].values():
            weight_class = x['weight_class']
            break    
        weight_class = self.cast_weight_class_to_int(weight_class)

        # record stats
        wins, losses = 0, 0
        win_streak, lose_streak = 0, 0
        longest_win_streak, longest_lose_streak = 0, 0
        wins_by_ko, wins_by_sub, wins_by_dec = 0, 0, 0
        losses_by_ko, losses_by_sub, losses_by_dec = 0, 0, 0

        # MIGHT WANT TO ACCOUNT FOR FIGHTS THAT DON'T GO THE DISTANCE SINCE THIS WILL IMPACT THE NUMBERS -- Also might want to play with bundling features together
        last_yr_fight_stats = {
            'last_yr_fight_count': 0, 'last_yr_fight_time': 0, 'last_yr_fights_strikes_landed': 0, 'last_yr_fights_strikes_defended': 0, 'last_yr_fights_sig_strikes_landed': 0, 'last_yr_fights_sig_strikes_defended': 0,
            'last_yr_fights_takedowns_landed': 0, 'last_yr_fights_takedowns_defended': 0, 'last_yr_fights_submission_attempts': 0, 'last_yr_fights_passes': 0,
            'last_yr_fights_strike_accuracy': 0, 'last_yr_fights_strike_defense': 0, 'last_yr_fights_sig_strike_accuracy': 0, 'last_yr_fights_sig_strike_defense': 0,
            'last_yr_fights_takedown_accuracy': 0, 'last_yr_fights_takedown_defense': 0, 'last_yr_fights_submission_attempts': 0, 'last_yr_fights_passes': 0,
            'last_yr_fights_reversals': 0, 'last_yr_fights_control_time': 0,
            'last_yr_fights_head_strikes_landed': 0, 'last_yr_fights_head_strikes_defended': 0, 'last_yr_fights_body_strikes_landed': 0, 'last_yr_fights_body_strikes_defended': 0,
            'last_yr_fights_leg_strikes_landed': 0, 'last_yr_fights_leg_strikes_defended': 0, 'last_yr_fights_distance_strikes_landed': 0, 'last_yr_fights_distance_strikes_defended': 0,
            'last_yr_fights_clinc_strikes_landed': 0, 'last_yr_fights_clinc_strikes_defended': 0, 'last_yr_fights_ground_strikes_landed': 0, 'last_yr_fights_ground_strikes_defended': 0,
            'last_yr_fights_head_strikes_accuracy': 0, 'last_yr_fights_head_strikes_defense': 0, 'last_yr_fights_body_strikes_accuracy': 0, 'last_yr_fights_body_strikes_defense': 0,
            'last_yr_fights_leg_strikes_accuracy': 0, 'last_yr_fights_leg_strikes_defense': 0, 'last_yr_fights_distance_strikes_accuracy': 0, 'last_yr_fights_distance_strikes_defense': 0,
            'last_yr_fights_clinc_strikes_accuracy': 0, 'last_yr_fights_clinc_strikes_defense': 0, 'last_yr_fights_ground_strikes_accuracy': 0, 'last_yr_fights_ground_strikes_defense': 0
        }
        last_X_yr_fight_stats = {
            'last_X_yr_fight_count': 0, 'last_X_yr_fight_time': 0, 'last_X_yr_fights_strikes_landed': 0, 'last_X_yr_fights_strikes_defended': 0, 'last_X_yr_fights_sig_strikes_landed': 0, 'last_X_yr_fights_sig_strikes_defended': 0,
            'last_X_yr_fights_takedowns_landed': 0, 'last_X_yr_fights_takedowns_defended': 0, 'last_X_yr_fights_submission_attempts': 0, 'last_X_yr_fights_passes': 0,
            'last_X_yr_fights_strike_accuracy': 0, 'last_X_yr_fights_strike_defense': 0, 'last_X_yr_fights_sig_strike_accuracy': 0, 'last_X_yr_fights_sig_strike_defense': 0,
            'last_X_yr_fights_takedown_accuracy': 0, 'last_X_yr_fights_takedown_defense': 0, 'last_X_yr_fights_submission_attempts': 0, 'last_X_yr_fights_passes': 0,
            'last_X_yr_fights_reversals': 0, 'last_X_yr_fights_control_time': 0,
            'last_X_yr_fights_head_strikes_landed': 0, 'last_X_yr_fights_head_strikes_defended': 0, 'last_X_yr_fights_body_strikes_landed': 0, 'last_X_yr_fights_body_strikes_defended': 0,
            'last_X_yr_fights_leg_strikes_landed': 0, 'last_X_yr_fights_leg_strikes_defended': 0, 'last_X_yr_fights_distance_strikes_landed': 0, 'last_X_yr_fights_distance_strikes_defended': 0,
            'last_X_yr_fights_clinc_strikes_landed': 0, 'last_X_yr_fights_clinc_strikes_defended': 0, 'last_X_yr_fights_ground_strikes_landed': 0, 'last_X_yr_fights_ground_strikes_defended': 0,
            'last_X_yr_fights_head_strikes_accuracy': 0, 'last_X_yr_fights_head_strikes_defense': 0, 'last_X_yr_fights_body_strikes_accuracy': 0, 'last_X_yr_fights_body_strikes_defense': 0,
            'last_X_yr_fights_leg_strikes_accuracy': 0, 'last_X_yr_fights_leg_strikes_defense': 0, 'last_X_yr_fights_distance_strikes_accuracy': 0, 'last_X_yr_fights_distance_strikes_defense': 0,
            'last_X_yr_fights_clinc_strikes_accuracy': 0, 'last_X_yr_fights_clinc_strikes_defense': 0, 'last_X_yr_fights_ground_strikes_accuracy': 0, 'last_X_yr_fights_ground_strikes_defense': 0
        }
        total_fight_stats = {
            'total_fight_count': 0, 'total_fight_time': 0, 'total_fights_strikes_landed': 0, 'total_fights_strikes_defended': 0, 'total_fights_sig_strikes_landed': 0, 'total_fights_sig_strikes_defended': 0,
            'total_fights_takedowns_landed': 0, 'total_fights_takedowns_defended': 0, 'total_fights_submission_attempts': 0, 'total_fights_passes': 0,
            'total_fights_strike_accuracy': 0, 'total_fights_strike_defense': 0, 'total_fights_sig_strike_accuracy': 0, 'total_fights_sig_strike_defense': 0,
            'total_fights_takedown_accuracy': 0, 'total_fights_takedown_defense': 0, 'total_fights_submission_attempts': 0, 'total_fights_passes': 0,
            'total_fights_reversals': 0, 'total_fights_control_time': 0,
            'total_fights_head_strikes_landed': 0, 'total_fights_head_strikes_defended': 0, 'total_fights_body_strikes_landed': 0, 'total_fights_body_strikes_defended': 0,
            'total_fights_leg_strikes_landed': 0, 'total_fights_leg_strikes_defended': 0, 'total_fights_distance_strikes_landed': 0, 'total_fights_distance_strikes_defended': 0,
            'total_fights_clinc_strikes_landed': 0, 'total_fights_clinc_strikes_defended': 0, 'total_fights_ground_strikes_landed': 0, 'total_fights_ground_strikes_defended': 0,
            'total_fights_head_strikes_accuracy': 0, 'total_fights_head_strikes_defense': 0, 'total_fights_body_strikes_accuracy': 0, 'total_fights_body_strikes_defense': 0,
            'total_fights_leg_strikes_accuracy': 0, 'total_fights_leg_strikes_defense': 0, 'total_fights_distance_strikes_accuracy': 0, 'total_fights_distance_strikes_defense': 0,
            'total_fights_clinc_strikes_accuracy': 0, 'total_fights_clinc_strikes_defense': 0, 'total_fights_ground_strikes_accuracy': 0, 'total_fights_ground_strikes_defense': 0
        }

        if len(fighter_history['fight_history'].values()) == 0:
            return {
                'weight_class': weight_class,
                'weight_class_change': 0,
                'wins': wins,
                'losses': losses,
                'win_streak': win_streak,
                'lose_streak': lose_streak,
                'longest_win_streak': longest_win_streak,
                'longest_lose_streak': longest_lose_streak,
                'wins_by_ko': wins_by_ko,
                'wins_by_sub': wins_by_sub,
                'wins_by_dec': wins_by_dec,
                'losses_by_ko': losses_by_ko,
                'losses_by_sub': losses_by_sub,
                'losses_by_dec': losses_by_dec,            
                'last_fight_stats': last_yr_fight_stats,
                'last_3_fight_stats': last_X_yr_fight_stats,
                'total_fight_stats': total_fight_stats
            }

        idx = 0
        for fight in fighter_history['fight_history'].values():
            if idx == 0:
                weight_class = self.cast_weight_class_to_int(fight['weight_class'])

            if self.compare_dates(fight['date'], fight_date) == False:
                continue

            # getting the fighter and opponent
            fighter = 'f1' if fighter_name == fight['f1'] else 'f2'
            opponent = 'f2' if fighter == 'f1' else 'f1'

            # getting wins, losses, and draws
            if fight[f'{fighter}_res'] == 'W':
                wins += 1
                win_streak += 1
                lose_streak = 0
            elif fight[f'{fighter}_res'] == 'L':
                losses += 1
                lose_streak += 1
                win_streak = 0
            
            # getting longest streaks
            if win_streak > longest_win_streak:
                longest_win_streak = win_streak
            if lose_streak > longest_lose_streak:
                longest_lose_streak = lose_streak
            
            # getting wins by ko, sub, and dec
            if fight['method'] == 'KO/TKO':
                if fight[f'{fighter}_res'] == 'W':
                    wins_by_ko += 1
                else:
                    losses_by_ko += 1
            elif fight['method'] == 'Submission':
                if fight[f'{fighter}_res'] == 'W':
                    wins_by_sub += 1
                else:
                    losses_by_sub += 1
            elif fight['method'] == 'Decision-Unanimous' or fight['method'] == 'Decision-Split' or fight['method'] == 'Decision-Majority':
                if fight[f'{fighter}_res'] == 'W':
                    wins_by_dec += 1
                else:
                    losses_by_dec += 1

            # record the stats of the fight if it happened within the year prior to the current fight
            if self.in_last_x_years(fight['date'], fight_date, 1):
                last_yr_fight_stats['last_yr_fight_count'] += 1
                last_yr_fight_stats['last_yr_fight_time'] += self.get_fight_in_seconds(fight['round'], fight['time'])
                last_yr_fight_stats['last_yr_fights_strikes_landed'] += int(fight[f'total_str_hit_{fighter}']) if fight[f'total_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_strikes_defended'] += int(fight[f'total_str_hit_{opponent}']) if fight[f'total_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_sig_strikes_landed'] += int(fight[f'sig_str_hit_{fighter}']) if fight[f'sig_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_sig_strikes_defended'] += int(fight[f'sig_str_hit_{opponent}']) if fight[f'sig_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_takedowns_landed'] += int(fight[f'td_hit_{fighter}']) if fight[f'td_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_takedowns_defended'] += int(fight[f'td_hit_{opponent}']) if fight[f'td_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_submission_attempts'] += int(fight[f'sub_att_{fighter}']) if fight[f'sub_att_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_strike_accuracy'] += int(fight[f'total_str_hit_{fighter}']) / int(fight[f'total_str_tot_{fighter}']) if fight[f'total_str_tot_{fighter}'] != '' and int(fight[f'total_str_tot_{fighter}']) != 0 else 0
                last_yr_fight_stats['last_yr_fights_strike_defense'] += int(fight[f'total_str_hit_{opponent}']) / int(fight[f'total_str_tot_{opponent}']) if fight[f'total_str_tot_{opponent}'] != '' and int(fight[f'total_str_tot_{opponent}']) != 0 else 0
                last_yr_fight_stats['last_yr_fights_sig_strike_accuracy'] += int(fight[f'sig_str_perc_{fighter}'].replace('%', '') if fight[f'sig_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_sig_strike_defense'] += int(fight[f'sig_str_perc_{opponent}'].replace('%', '') if fight[f'sig_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_takedown_accuracy'] += int(fight[f'td_perc_{fighter}'].replace('%', '') if fight[f'td_perc_{fighter}'].replace('%', '') != '' and fight[f'td_perc_{fighter}'].replace('%', '') != '-1' else 0) / 100
                last_yr_fight_stats['last_yr_fights_takedown_defense'] += int(fight[f'td_perc_{opponent}'].replace('%', '') if fight[f'td_perc_{opponent}'].replace('%', '') != '' and fight[f'td_perc_{opponent}'].replace('%', '') != '-1' else 0) / 100
                last_yr_fight_stats['last_yr_fights_submission_attempts'] += fight[f'sub_att_{fighter}'] if fight[f'sub_att_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_reversals'] += int(fight[f'rev_{fighter}']) if fight[f'rev_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_control_time'] += self.convert_to_seconds(fight[f'ctrl_{fighter}'])
                last_yr_fight_stats['last_yr_fights_head_strikes_landed'] += int(fight[f'head_str_hit_{fighter}']) if fight[f'head_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_head_strikes_defended'] += int(fight[f'head_str_hit_{opponent}']) if fight[f'head_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_body_strikes_landed'] += int(fight[f'body_str_hit_{fighter}']) if fight[f'body_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_body_strikes_defended'] += int(fight[f'body_str_hit_{opponent}']) if fight[f'body_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_leg_strikes_landed'] += int(fight[f'leg_str_hit_{fighter}']) if fight[f'leg_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_leg_strikes_defended'] += int(fight[f'leg_str_hit_{opponent}']) if fight[f'leg_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_distance_strikes_landed'] += int(fight[f'dist_str_hit_{fighter}']) if fight[f'dist_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_distance_strikes_defended'] += int(fight[f'dist_str_hit_{opponent}']) if fight[f'dist_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_clinc_strikes_landed'] += int(fight[f'clinc_str_hit_{fighter}']) if fight[f'clinc_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_clinc_strikes_defended'] += int(fight[f'clinc_str_hit_{opponent}']) if fight[f'clinc_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_ground_strikes_landed'] += int(fight[f'ground_str_hit_{fighter}']) if fight[f'ground_str_hit_{fighter}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_ground_strikes_defended'] += int(fight[f'ground_str_hit_{opponent}']) if fight[f'ground_str_hit_{opponent}'] != '' else 0
                last_yr_fight_stats['last_yr_fights_head_strikes_accuracy'] += int(fight[f'head_str_perc_{fighter}'].replace('%', '') if fight[f'head_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_head_strikes_defense'] += int(fight[f'head_str_perc_{opponent}'].replace('%', '') if fight[f'head_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_body_strikes_accuracy'] += int(fight[f'body_str_perc_{fighter}'].replace('%', '') if fight[f'body_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_body_strikes_defense'] += int(fight[f'body_str_perc_{opponent}'].replace('%', '') if fight[f'body_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_leg_strikes_accuracy'] += int(fight[f'leg_str_perc_{fighter}'].replace('%', '') if fight[f'leg_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_leg_strikes_defense'] += int(fight[f'leg_str_perc_{opponent}'].replace('%', '') if fight[f'leg_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_distance_strikes_accuracy'] += int(fight[f'dist_str_perc_{fighter}'].replace('%', '') if fight[f'dist_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_distance_strikes_defense'] += int(fight[f'dist_str_perc_{opponent}'].replace('%', '') if fight[f'dist_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_clinc_strikes_accuracy'] += int(fight[f'clinc_str_perc_{fighter}'].replace('%', '') if fight[f'clinc_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_clinc_strikes_defense'] += int(fight[f'clinc_str_perc_{opponent}'].replace('%', '') if fight[f'clinc_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_ground_strikes_accuracy'] += int(fight[f'ground_str_perc_{fighter}'].replace('%', '') if fight[f'ground_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_yr_fight_stats['last_yr_fights_ground_strikes_defense'] += int(fight[f'ground_str_perc_{opponent}'].replace('%', '') if fight[f'ground_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            
            # record the stats of the fight if it happened within the X years prior to the current fight
            if self.in_last_x_years(fight['date'], fight_date, 5):
                last_X_yr_fight_stats['last_X_yr_fight_count'] += 1
                last_X_yr_fight_stats['last_X_yr_fight_time'] += self.get_fight_in_seconds(fight['round'], fight['time'])
                last_X_yr_fight_stats['last_X_yr_fights_strikes_landed'] += int(fight[f'total_str_hit_{fighter}']) if fight[f'total_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_strikes_defended'] += int(fight[f'total_str_hit_{opponent}']) if fight[f'total_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_sig_strikes_landed'] += int(fight[f'sig_str_hit_{fighter}']) if fight[f'sig_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_sig_strikes_defended'] += int(fight[f'sig_str_hit_{opponent}']) if fight[f'sig_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_takedowns_landed'] += int(fight[f'td_hit_{fighter}']) if fight[f'td_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_takedowns_defended'] += int(fight[f'td_hit_{opponent}']) if fight[f'td_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_submission_attempts'] += int(fight[f'sub_att_{fighter}']) if fight[f'sub_att_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_strike_accuracy'] += int(fight[f'total_str_hit_{fighter}']) / int(fight[f'total_str_tot_{fighter}']) if fight[f'total_str_tot_{fighter}'] != '' and int(fight[f'total_str_tot_{fighter}']) != 0 else 0
                last_X_yr_fight_stats['last_X_yr_fights_strike_defense'] += int(fight[f'total_str_hit_{opponent}']) / int(fight[f'total_str_tot_{opponent}']) if fight[f'total_str_tot_{opponent}'] != '' and int(fight[f'total_str_tot_{opponent}']) != 0 else 0
                last_X_yr_fight_stats['last_X_yr_fights_sig_strike_accuracy'] += int(fight[f'sig_str_perc_{fighter}'].replace('%', '') if fight[f'sig_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_sig_strike_defense'] += int(fight[f'sig_str_perc_{opponent}'].replace('%', '') if fight[f'sig_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_takedown_accuracy'] += int(fight[f'td_perc_{fighter}'].replace('%', '') if fight[f'td_perc_{fighter}'].replace('%', '') != '' and fight[f'td_perc_{fighter}'].replace('%', '') != '-1' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_takedown_defense'] += int(fight[f'td_perc_{opponent}'].replace('%', '') if fight[f'td_perc_{opponent}'].replace('%', '') != '' and fight[f'td_perc_{opponent}'].replace('%', '') != '-1' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_submission_attempts'] += fight[f'sub_att_{fighter}'] if fight[f'sub_att_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_reversals'] += int(fight[f'rev_{fighter}']) if fight[f'rev_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_control_time'] += self.convert_to_seconds(fight[f'ctrl_{fighter}'])
                last_X_yr_fight_stats['last_X_yr_fights_head_strikes_landed'] += int(fight[f'head_str_hit_{fighter}']) if fight[f'head_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_head_strikes_defended'] += int(fight[f'head_str_hit_{opponent}']) if fight[f'head_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_body_strikes_landed'] += int(fight[f'body_str_hit_{fighter}']) if fight[f'body_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_body_strikes_defended'] += int(fight[f'body_str_hit_{opponent}']) if fight[f'body_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_leg_strikes_landed'] += int(fight[f'leg_str_hit_{fighter}']) if fight[f'leg_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_leg_strikes_defended'] += int(fight[f'leg_str_hit_{opponent}']) if fight[f'leg_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_distance_strikes_landed'] += int(fight[f'dist_str_hit_{fighter}']) if fight[f'dist_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_distance_strikes_defended'] += int(fight[f'dist_str_hit_{opponent}']) if fight[f'dist_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_clinc_strikes_landed'] += int(fight[f'clinc_str_hit_{fighter}']) if fight[f'clinc_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_clinc_strikes_defended'] += int(fight[f'clinc_str_hit_{opponent}']) if fight[f'clinc_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_ground_strikes_landed'] += int(fight[f'ground_str_hit_{fighter}']) if fight[f'ground_str_hit_{fighter}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_ground_strikes_defended'] += int(fight[f'ground_str_hit_{opponent}']) if fight[f'ground_str_hit_{opponent}'] != '' else 0
                last_X_yr_fight_stats['last_X_yr_fights_head_strikes_accuracy'] += int(fight[f'head_str_perc_{fighter}'].replace('%', '') if fight[f'head_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_head_strikes_defense'] += int(fight[f'head_str_perc_{opponent}'].replace('%', '') if fight[f'head_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_body_strikes_accuracy'] += int(fight[f'body_str_perc_{fighter}'].replace('%', '') if fight[f'body_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_body_strikes_defense'] += int(fight[f'body_str_perc_{opponent}'].replace('%', '') if fight[f'body_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_leg_strikes_accuracy'] += int(fight[f'leg_str_perc_{fighter}'].replace('%', '') if fight[f'leg_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_leg_strikes_defense'] += int(fight[f'leg_str_perc_{opponent}'].replace('%', '') if fight[f'leg_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_distance_strikes_accuracy'] += int(fight[f'dist_str_perc_{fighter}'].replace('%', '') if fight[f'dist_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_distance_strikes_defense'] += int(fight[f'dist_str_perc_{opponent}'].replace('%', '') if fight[f'dist_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_clinc_strikes_accuracy'] += int(fight[f'clinc_str_perc_{fighter}'].replace('%', '') if fight[f'clinc_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_clinc_strikes_defense'] += int(fight[f'clinc_str_perc_{opponent}'].replace('%', '') if fight[f'clinc_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_ground_strikes_accuracy'] += int(fight[f'ground_str_perc_{fighter}'].replace('%', '') if fight[f'ground_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
                last_X_yr_fight_stats['last_X_yr_fights_ground_strikes_defense'] += int(fight[f'ground_str_perc_{opponent}'].replace('%', '') if fight[f'ground_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            
            # record the stats of the fight if it happened before the current fight
            total_fight_stats['total_fight_count'] += 1
            total_fight_stats['total_fight_time'] += self.get_fight_in_seconds(fight['round'], fight['time'])
            total_fight_stats['total_fights_strikes_landed'] += int(fight[f'total_str_hit_{fighter}']) if fight[f'total_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_strikes_defended'] += int(fight[f'total_str_hit_{opponent}']) if fight[f'total_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_sig_strikes_landed'] += int(fight[f'sig_str_hit_{fighter}']) if fight[f'sig_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_sig_strikes_defended'] += int(fight[f'sig_str_hit_{opponent}']) if fight[f'sig_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_takedowns_landed'] += int(fight[f'td_hit_{fighter}']) if fight[f'td_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_takedowns_defended'] += int(fight[f'td_hit_{opponent}']) if fight[f'td_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_submission_attempts'] += int(fight[f'sub_att_{fighter}']) if fight[f'sub_att_{fighter}'] != '' else 0
            total_fight_stats['total_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
            total_fight_stats['total_fights_strike_accuracy'] += int(fight[f'total_str_hit_{fighter}']) / int(fight[f'total_str_tot_{fighter}']) if fight[f'total_str_tot_{fighter}'] != '' and int(fight[f'total_str_tot_{fighter}']) != 0 else 0
            total_fight_stats['total_fights_strike_defense'] += int(fight[f'total_str_hit_{opponent}']) / int(fight[f'total_str_tot_{opponent}']) if fight[f'total_str_tot_{opponent}'] != '' and int(fight[f'total_str_tot_{opponent}']) != 0 else 0
            total_fight_stats['total_fights_sig_strike_accuracy'] += int(fight[f'sig_str_perc_{fighter}'].replace('%', '') if fight[f'sig_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_sig_strike_defense'] += int(fight[f'sig_str_perc_{opponent}'].replace('%', '') if fight[f'sig_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_takedown_accuracy'] += int(fight[f'td_perc_{fighter}'].replace('%', '') if fight[f'td_perc_{fighter}'].replace('%', '') != '' and fight[f'td_perc_{fighter}'].replace('%', '') != '-1' else 0) / 100
            total_fight_stats['total_fights_takedown_defense'] += int(fight[f'td_perc_{opponent}'].replace('%', '') if fight[f'td_perc_{opponent}'].replace('%', '') != '' and fight[f'td_perc_{opponent}'].replace('%', '') != '-1' else 0) / 100
            total_fight_stats['total_fights_submission_attempts'] += fight[f'sub_att_{fighter}'] if fight[f'sub_att_{fighter}'] != '' else 0
            total_fight_stats['total_fights_passes'] += int(fight[f'sub_att_{opponent}']) if fight[f'sub_att_{opponent}'] != '' else 0
            total_fight_stats['total_fights_reversals'] += int(fight[f'rev_{fighter}']) if fight[f'rev_{fighter}'] != '' else 0
            total_fight_stats['total_fights_control_time'] += self.convert_to_seconds(fight[f'ctrl_{fighter}'])
            total_fight_stats['total_fights_head_strikes_landed'] += int(fight[f'head_str_hit_{fighter}']) if fight[f'head_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_head_strikes_defended'] += int(fight[f'head_str_hit_{opponent}']) if fight[f'head_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_body_strikes_landed'] += int(fight[f'body_str_hit_{fighter}']) if fight[f'body_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_body_strikes_defended'] += int(fight[f'body_str_hit_{opponent}']) if fight[f'body_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_leg_strikes_landed'] += int(fight[f'leg_str_hit_{fighter}']) if fight[f'leg_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_leg_strikes_defended'] += int(fight[f'leg_str_hit_{opponent}']) if fight[f'leg_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_distance_strikes_landed'] += int(fight[f'dist_str_hit_{fighter}']) if fight[f'dist_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_distance_strikes_defended'] += int(fight[f'dist_str_hit_{opponent}']) if fight[f'dist_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_clinc_strikes_landed'] += int(fight[f'clinc_str_hit_{fighter}']) if fight[f'clinc_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_clinc_strikes_defended'] += int(fight[f'clinc_str_hit_{opponent}']) if fight[f'clinc_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_ground_strikes_landed'] += int(fight[f'ground_str_hit_{fighter}']) if fight[f'ground_str_hit_{fighter}'] != '' else 0
            total_fight_stats['total_fights_ground_strikes_defended'] += int(fight[f'ground_str_hit_{opponent}']) if fight[f'ground_str_hit_{opponent}'] != '' else 0
            total_fight_stats['total_fights_head_strikes_accuracy'] += int(fight[f'head_str_perc_{fighter}'].replace('%', '') if fight[f'head_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_head_strikes_defense'] += int(fight[f'head_str_perc_{opponent}'].replace('%', '') if fight[f'head_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_body_strikes_accuracy'] += int(fight[f'body_str_perc_{fighter}'].replace('%', '') if fight[f'body_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_body_strikes_defense'] += int(fight[f'body_str_perc_{opponent}'].replace('%', '') if fight[f'body_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_leg_strikes_accuracy'] += int(fight[f'leg_str_perc_{fighter}'].replace('%', '') if fight[f'leg_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_leg_strikes_defense'] += int(fight[f'leg_str_perc_{opponent}'].replace('%', '') if fight[f'leg_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_distance_strikes_accuracy'] += int(fight[f'dist_str_perc_{fighter}'].replace('%', '') if fight[f'dist_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_distance_strikes_defense'] += int(fight[f'dist_str_perc_{opponent}'].replace('%', '') if fight[f'dist_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_clinc_strikes_accuracy'] += int(fight[f'clinc_str_perc_{fighter}'].replace('%', '') if fight[f'clinc_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_clinc_strikes_defense'] += int(fight[f'clinc_str_perc_{opponent}'].replace('%', '') if fight[f'clinc_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_ground_strikes_accuracy'] += int(fight[f'ground_str_perc_{fighter}'].replace('%', '') if fight[f'ground_str_perc_{fighter}'].replace('%', '') != '' else 0) / 100
            total_fight_stats['total_fights_ground_strikes_defense'] += int(fight[f'ground_str_perc_{opponent}'].replace('%', '') if fight[f'ground_str_perc_{opponent}'].replace('%', '') != '' else 0) / 100

            idx += 1
        
        # updating all accuracy stats to be a percentage by looping through the stats and dividing by the total number of fights
        for key in last_yr_fight_stats.keys():
            if 'accuracy' in key or 'defense' in key:
                last_yr_fight_stats[key] /= last_yr_fight_stats['last_yr_fight_count'] if last_yr_fight_stats['last_yr_fight_count'] != 0 else 1
        for key in last_X_yr_fight_stats.keys():
            if 'accuracy' in key or 'defense' in key:
                last_X_yr_fight_stats[key] /= last_X_yr_fight_stats['last_X_yr_fight_count'] if last_X_yr_fight_stats['last_X_yr_fight_count'] != 0 else 1
        for key in total_fight_stats.keys():
            if 'accuracy' in key or 'defense' in key:
                total_fight_stats[key] /= total_fight_stats['total_fight_count'] if total_fight_stats['total_fight_count'] != 0 else 1

        return {
            'weight_class': weight_class,
            'weight_class_change': 0,
            'wins': wins,
            'losses': losses,
            'win_streak': win_streak,
            'lose_streak': lose_streak,
            'longest_win_streak': longest_win_streak,
            'longest_lose_streak': longest_lose_streak,
            'wins_by_ko': wins_by_ko,
            'wins_by_sub': wins_by_sub,
            'wins_by_dec': wins_by_dec,
            'losses_by_ko': losses_by_ko,
            'losses_by_sub': losses_by_sub,
            'losses_by_dec': losses_by_dec,
            'last_fight_stats': last_yr_fight_stats,
            'last_3_fight_stats': last_X_yr_fight_stats,
            'total_fight_stats': total_fight_stats
        }
            

    def convert_to_inches(self, height):
        if "'" not in height:
            return 0

        height = height.replace('"', '').split("'")
        feet = int(height[0])
        inches = int(height[1])
        return feet * 12 + inches

    def cast_stance_to_int(self,stance):
        if stance == 'Orthodox':
            return 0
        if stance == 'Southpaw':
            return 1
        if stance == 'Switch':
            return 2
        return -1
        

    # generating list of stats based on the specified features for each fighter at the time of each fight and writing to a csv
    def generate_training_data(self):
        # loop through every fight and generate the stats of each fighter in the fight at the time of the fight (don't include the fight stats themselves)
        
        # open fight_stats csv
        fight_stats = pd.read_csv('data/fight_stats.csv')

        # open fighter_stats csv
        fighter_stats = pd.read_csv('data/fighter_stats.csv')

        fighter_histories = self.json_data
        
        # create pandas dataframe to store the training data
        training_data = []    

        ctr = 0
        for index, row in fight_stats.iterrows():
            fight_date = self.standardize_date(row['date'])
            f1 = row['f1']
            f2 = row['f2']
            fight_weight_class = row['weight_class']

            try:
                f1_dob = fighter_stats[fighter_stats['name'] == f1]['DOB'].values[0]
                f2_dob = fighter_stats[fighter_stats['name'] == f2]['DOB'].values[0]
            except:
                print(f"F1: {f1} or F2: {f2} has no DOB")
                continue

            # get the fighter's height, reach, and stance from the fighter_stats dataset
            f1_dob = fighter_stats[fighter_stats['name'] == f1]['DOB'].values[0]
            f1_height = fighter_stats[fighter_stats['name'] == f1]['Height'].values[0]
            f1_reach = fighter_stats[fighter_stats['name'] == f1]['Reach'].values[0]
            f1_stance = fighter_stats[fighter_stats['name'] == f1]['STANCE'].values[0]
            f2_dob = fighter_stats[fighter_stats['name'] == f2]['DOB'].values[0]
            f2_height = fighter_stats[fighter_stats['name'] == f2]['Height'].values[0]
            f2_reach = fighter_stats[fighter_stats['name'] == f2]['Reach'].values[0]
            f2_stance = fighter_stats[fighter_stats['name'] == f2]['STANCE'].values[0]

            win = row['f1_res']
            method = row['method']
            round_count = row['round']

            # find the fighter's stats at the time of the fight
            f1_stats = {}
            f2_stats = {}
            for fighter in fighter_histories:
                weight_class_cng = 0
                if f1 in fighter:
                    f1_stats = self.get_fighter_stats(f1, fight_date, fighter[f1])

                    if len(fighter[f1]['fight_history'].values()) > 0:
                        for fight in fighter[f1]['fight_history'].values():
                            if self.compare_dates(self.standardize_date(fight['date']), fight_date):
                                weight_class_cng = self.get_weight_class_change(fight['weight_class'], fight_weight_class)
                                break
                    f1_stats['weight_class_change'] = weight_class_cng

                if f2 in fighter:
                    f2_stats = self.get_fighter_stats(f2, fight_date, fighter[f2])

                    if len(fighter[f2]['fight_history'].values()) > 0:
                        for fight in fighter[f2]['fight_history'].values():
                            if self.compare_dates(self.standardize_date(fight['date']), fight_date):
                                weight_class_cng = self.get_weight_class_change(fight['weight_class'], fight_weight_class)
                                break
                    f2_stats['weight_class_change'] = weight_class_cng
            
            # if either fighter's stats are empty, skip to the next fight
            if not f1_stats or not f2_stats:
                continue
        
            # flatten the stats dictionaries
            f1_training = {}
            for key in f1_stats.keys():
                if key != 'last_fight_stats' and key != 'last_3_fight_stats' and key != 'total_fight_stats':
                    f1_training[f'f1_{key}'] = f1_stats[key]
            for key in f1_stats['last_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['last_fight_stats'][key]
            for key in f1_stats['last_3_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['last_3_fight_stats'][key]
            for key in f1_stats['total_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['total_fight_stats'][key]
            f2_training = {}
            for key in f2_stats.keys():
                if key != 'last_fight_stats' and key != 'last_3_fight_stats' and key != 'total_fight_stats':
                    f2_training[f'f2_{key}'] = f2_stats[key]
            for key in f2_stats['last_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['last_fight_stats'][key]
            for key in f2_stats['last_3_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['last_3_fight_stats'][key]
            for key in f2_stats['total_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['total_fight_stats'][key]

            # get the fighter's age at the time of the fight and convert to float
            f1_age = float(self.get_fighter_age(f1_dob, fight_date)) if self.get_fighter_age(f1_dob, fight_date) != 'N/A' else 0
            f2_age = float(self.get_fighter_age(f2_dob, fight_date)) if self.get_fighter_age(f2_dob, fight_date) != 'N/A' else 0

            # add the fighter's age, height, reach, and stance to the training_data list
            f1_training['f1_name'] = f1
            f1_training['f1_age'] = f1_age
            f1_training['f1_height'] = self.convert_to_inches(f1_height)
            f1_training['f1_reach'] = int(f1_reach.replace('"', '')) if f1_reach != '--' else 0
            f1_training['f1_stance'] = self.cast_stance_to_int(f1_stance)
            f2_training['f2_name'] = f2
            f2_training['f2_age'] = f2_age
            f2_training['f2_height'] = self.convert_to_inches(f2_height)
            f2_training['f2_reach'] = int(f2_reach.replace('"', '')) if f2_reach != '--' else 0
            f2_training['f2_stance'] = self.cast_stance_to_int(f2_stance)

            # converting the features to the condensed form
            f1_training = condense_features(f1_training, 'f1')
            f2_training = condense_features(f2_training, 'f2')

            try:
                f1_training['f1_age_diff'] = int(f1_age) - int(f2_age)
                f2_training['f2_age_diff'] = int(f2_age) - int(f1_age)
            except:
                f1_training['f1_age_diff'] = 0
                f2_training['f2_age_diff'] = 0
            
            f1_training['f1_last_yr_fight_diff'] = f1_training['f1_last_yr_fight_count'] - f2_training['f2_last_yr_fight_count']
            f2_training['f2_last_yr_fight_diff'] = f2_training['f2_last_yr_fight_count'] - f1_training['f1_last_yr_fight_count']
            f1_training['f1_last_X_yr_fight_diff'] = f1_training['f1_last_X_yr_fight_count'] - f2_training['f2_last_X_yr_fight_count']
            f2_training['f2_last_X_yr_fight_diff'] = f2_training['f2_last_X_yr_fight_count'] - f1_training['f1_last_X_yr_fight_count']
            f1_training['f1_total_fight_diff'] = f1_training['f1_total_fight_count'] - f2_training['f2_total_fight_count']
            f2_training['f2_total_fight_diff'] = f2_training['f2_total_fight_count'] - f1_training['f1_total_fight_count']

            # switch the f1 and f2 columns so the fighters are listed in alphabetical order
            if f1 > f2:
                # f2 comes before f1 alphabetically, so swap them to ensure consistent ordering.
                # The new f1 will be the original f2, and the new f2 will be the original f1.
                
                # Create new dictionaries with swapped data and corrected prefixes.
                new_f1_training = {key.replace('f2_', 'f1_'): value for key, value in f2_training.items()}
                new_f2_training = {key.replace('f1_', 'f2_'): value for key, value in f1_training.items()}
                
                # Update the original dictionaries for the merge.
                f1_training = new_f1_training
                f2_training = new_f2_training
                
                # The 'win' variable is from the original f1's perspective, so it must be flipped.
                win = 'L' if win == 'W' else 'W'

            # merging the two dictionaries to become one
            fight_training = {**f1_training, **f2_training}
            
            fight_training['win'] = win = 1 if win == 'W' else 0
            fight_training['goes_the_distance'] = 1 if method == 'Decision-Unanimous' or method == 'Decision-Split' or method == 'Decision-Majority' else 0
            fight_training['rounds'] = round_count
            fight_training['date'] = fight_date

            # add the fighter's stats to the training_data df
            training_data.append(fight_training)
            
            ctr += 1

            # if ctr == 5:
            #     break
        
        print('getting composite stats...')
        # loop through the training_data
        for row in training_data:
            # get all the fights the current f1 has been in that happened before the current fight by looping through the training_data dataframe
            f1_fights = []
            f2_fights = []
            for fight in training_data:
                if fight['f1_name'] == row['f1_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f1_fights.append(fight)
                elif fight['f2_name'] == row['f1_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f1_fights.append(fight)

                if fight['f1_name'] == row['f2_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f2_fights.append(fight)
                elif fight['f2_name'] == row['f2_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f2_fights.append(fight)

            # looping through all the fights f1 has been in and adding the stats to the row
            f1_composite_losses, f1_composite_wins = 0, 0
            f1_composite_record = 0
            for fight in f1_fights:
                if fight['f1_name'] == row['f1_name']:
                    f1_composite_record += fight['f2_wins'] / fight['f2_losses'] if fight['f2_losses'] != 0 else fight['f2_wins']
                    f1_composite_losses += fight['f2_losses']
                    f1_composite_wins += fight['f2_wins']
                else:
                    f1_composite_record += fight['f1_wins'] / fight['f1_losses'] if fight['f1_losses'] != 0 else fight['f1_wins']
                    f1_composite_losses += fight['f1_losses']
                    f1_composite_wins += fight['f1_wins']

            # looping through all the fights f2 has been in and adding the stats to the row
            f2_composite_losses, f2_composite_wins = 0, 0
            f2_composite_record = 0
            for fight in f2_fights:
                if fight['f1_name'] == row['f2_name']:
                    f2_composite_record += fight['f2_wins'] / fight['f2_losses'] if fight['f2_losses'] != 0 else fight['f2_wins']
                    f2_composite_losses += fight['f2_losses']
                    f2_composite_wins += fight['f2_wins']
                else:
                    f2_composite_record += fight['f1_wins'] / fight['f1_losses'] if fight['f1_losses'] != 0 else fight['f1_wins']
                    f2_composite_losses += fight['f1_losses']
                    f2_composite_wins += fight['f1_wins']
            
            row['f1_composite_losses'] = f1_composite_losses / row['f1_losses'] if row['f1_losses'] != 0 else f1_composite_losses
            row['f1_composite_wins'] = f1_composite_wins / row['f1_wins'] if row['f1_wins'] != 0 else f1_composite_wins
            row['f1_composite_record'] = f1_composite_record
            row['f2_composite_losses'] = f2_composite_losses / row['f2_losses'] if row['f2_losses'] != 0 else f2_composite_losses
            row['f2_composite_wins'] = f2_composite_wins / row['f2_wins'] if row['f2_wins'] != 0 else f2_composite_wins
            row['f2_composite_record'] = f2_composite_record
            row['f1_height_reach_interaction'] = row['f1_height'] * row['f1_reach']
            row['f2_height_reach_interaction'] = row['f2_height'] * row['f2_reach']

        # create a pandas dataframe from the training_data list
        training_data = pd.DataFrame(training_data)

        training_data = self.add_moneyline_odds_to_training_data(training_data)

        # fix date column to be a datetime object with a timezone in UTC
        training_data['date'] = pd.to_datetime(training_data['date'], utc=True)
        
        # drop any duplicates from the training_data dataframe by date and f1_name and f2_name
        training_data = training_data.drop_duplicates(subset=['date', 'f1_name', 'f2_name'])

        # order by date descending
        training_data = training_data.sort_values(by='date', ascending=False)

        # write the training_data dataframe to a csv file
        training_data.to_csv('data/training_data.csv', index=False)


    def generate_new_fight_rows(self):
        # loop through every fight and generate the stats of each fighter in the fight at the time of the fight (don't include the fight stats themselves)
        
        # open fight_stats csv
        fight_stats = pd.read_csv('data/fights.csv')

        # open fighter_stats csv
        fighter_stats = pd.read_csv('data/fighter_stats.csv')

        # open fighter_histories json
        fighter_histories = self.json_data

        # create pandas dataframe to store the training data
        training_data = []    

        for index, row in fight_stats.iterrows():
            fight_date = self.standardize_date(row['date'])
            if row['f1'] > row['f2']:
                f1 = row['f1']
                f2 = row['f2']
            else:
                f1 = row['f2']
                f2 = row['f1']
            fight_weight_class = row['weight_class']

            # get the fighter's height, reach, and stance from the fighter_stats dataset
            f1_dob = fighter_stats[fighter_stats['name'] == f1]['DOB'].values[0]
            f1_height = fighter_stats[fighter_stats['name'] == f1]['Height'].values[0]
            f1_reach = fighter_stats[fighter_stats['name'] == f1]['Reach'].values[0]
            f1_stance = fighter_stats[fighter_stats['name'] == f1]['STANCE'].values[0]
            f2_dob = fighter_stats[fighter_stats['name'] == f2]['DOB'].values[0]
            f2_height = fighter_stats[fighter_stats['name'] == f2]['Height'].values[0]
            f2_reach = fighter_stats[fighter_stats['name'] == f2]['Reach'].values[0]
            f2_stance = fighter_stats[fighter_stats['name'] == f2]['STANCE'].values[0]

            # find the fighter's stats at the time of the fight
            f1_stats = {}
            f2_stats = {}
            for fighter in fighter_histories:
                weight_class_cng = 0
                if f1 in fighter:
                    f1_stats = self.get_fighter_stats(f1, fight_date, fighter[f1])

                    if len(fighter[f1]['fight_history'].values()) > 0:
                        for fight in fighter[f1]['fight_history'].values():
                            if self.compare_dates(self.standardize_date(fight['date']), fight_date):
                                weight_class_cng = self.get_weight_class_change(fight['weight_class'], fight_weight_class)
                                break
                    f1_stats['weight_class_change'] = weight_class_cng

                if f2 in fighter:
                    f2_stats = self.get_fighter_stats(f2, fight_date, fighter[f2])

                    if len(fighter[f2]['fight_history'].values()) > 0:
                        for fight in fighter[f2]['fight_history'].values():
                            if self.compare_dates(self.standardize_date(fight['date']), fight_date):
                                weight_class_cng = self.get_weight_class_change(fight['weight_class'], fight_weight_class)
                                break
                    f2_stats['weight_class_change'] = weight_class_cng

            # if either fighter's stats are empty, skip to the next fight
            if not f1_stats or not f2_stats:
                print(f1, f2)
                print(f1_stats and True, f2_stats and True)
                continue

            # flatten the stats dictionaries
            f1_training = {}
            for key in f1_stats.keys():
                if key != 'last_fight_stats' and key != 'last_3_fight_stats' and key != 'total_fight_stats':
                    f1_training[f'f1_{key}'] = f1_stats[key]
            for key in f1_stats['last_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['last_fight_stats'][key]
            for key in f1_stats['last_3_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['last_3_fight_stats'][key]
            for key in f1_stats['total_fight_stats'].keys():
                f1_training[f'f1_{key}'] = f1_stats['total_fight_stats'][key]
            f2_training = {}
            for key in f2_stats.keys():
                if key != 'last_fight_stats' and key != 'last_3_fight_stats' and key != 'total_fight_stats':
                    f2_training[f'f2_{key}'] = f2_stats[key]
            for key in f2_stats['last_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['last_fight_stats'][key]
            for key in f2_stats['last_3_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['last_3_fight_stats'][key]
            for key in f2_stats['total_fight_stats'].keys():
                f2_training[f'f2_{key}'] = f2_stats['total_fight_stats'][key]
            
            print(f1, 'vs', f2)
            print()

            # get the fighter's age at the time of the fight and convert to float
            f1_age = float(self.get_fighter_age(f1_dob, fight_date)) if self.get_fighter_age(f1_dob, fight_date) != 'N/A' else 0
            f2_age = float(self.get_fighter_age(f2_dob, fight_date)) if self.get_fighter_age(f2_dob, fight_date) != 'N/A' else 0

            # add the fighter's age, height, reach, and stance to the training_data list
            f1_training['f1_name'] = f1
            f1_training['f1_age'] = f1_age
            f1_training['f1_height'] = self.convert_to_inches(f1_height)
            f1_training['f1_reach'] = int(f1_reach.replace('"', '')) if f1_reach != '--' else 0
            f1_training['f1_stance'] = self.cast_stance_to_int(f1_stance)
            f2_training['f2_name'] = f2
            f2_training['f2_age'] = f2_age
            f2_training['f2_height'] = self.convert_to_inches(f2_height)
            f2_training['f2_reach'] = int(f2_reach.replace('"', '')) if f2_reach != '--' else 0
            f2_training['f2_stance'] = self.cast_stance_to_int(f2_stance)

            # converting the features to the condensed form
            f1_training = condense_features(f1_training, 'f1')
            f2_training = condense_features(f2_training, 'f2')

            try:
                f1_training['f1_age_diff'] = int(f1_age) - int(f2_age)
                f2_training['f2_age_diff'] = int(f2_age) - int(f1_age)
            except:
                f1_training['f1_age_diff'] = 0
                f2_training['f2_age_diff'] = 0
            
            f1_training['f1_last_yr_fight_diff'] = f1_training['f1_last_yr_fight_count'] - f2_training['f2_last_yr_fight_count']
            f2_training['f2_last_yr_fight_diff'] = f2_training['f2_last_yr_fight_count'] - f1_training['f1_last_yr_fight_count']
            f1_training['f1_last_X_yr_fight_diff'] = f1_training['f1_last_X_yr_fight_count'] - f2_training['f2_last_X_yr_fight_count']
            f2_training['f2_last_X_yr_fight_diff'] = f2_training['f2_last_X_yr_fight_count'] - f1_training['f1_last_X_yr_fight_count']
            f1_training['f1_total_fight_diff'] = f1_training['f1_total_fight_count'] - f2_training['f2_total_fight_count']
            f2_training['f2_total_fight_diff'] = f2_training['f2_total_fight_count'] - f1_training['f1_total_fight_count']

            # switch the f1 and f2 columns so the fighters are listed in alphabetical order
            if f1 > f2:
                # f2 comes before f1 alphabetically, so swap them to ensure consistent ordering.
                # The new f1 will be the original f2, and the new f2 will be the original f1.
                
                # Create new dictionaries with swapped data and corrected prefixes.
                new_f1_training = {key.replace('f2_', 'f1_'): value for key, value in f2_training.items()}
                new_f2_training = {key.replace('f1_', 'f2_'): value for key, value in f1_training.items()}
                
                # Update the original dictionaries for the merge.
                f1_training = new_f1_training
                f2_training = new_f2_training

            # merging the two dictionaries to become one
            fight_training = {**f1_training, **f2_training}

            fight_training['date'] = fight_date

            # add the fighter's stats to the training_data df
            training_data.append(fight_training)

        # opening the training_data csv file
        full_data = pd.read_csv('data/training_data.csv')
        # loop through the training_data
        for row in training_data:
            # get all the fights the current f1 and f2 have been in that happened before the current fight by looping through the training_data dataframe
            f1_fights = []
            f2_fights = []
            for idx, fight in full_data.iterrows():
                if fight['f1_name'] == row['f1_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f1_fights.append(fight)
                elif fight['f2_name'] == row['f1_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f1_fights.append(fight)

                if fight['f1_name'] == row['f2_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f2_fights.append(fight)
                elif fight['f2_name'] == row['f2_name'] and self.compare_dates(self.standardize_date(fight['date']), self.standardize_date(row['date'])) == True:
                    f2_fights.append(fight)

            # looping through all the fights f1 has been in and adding the stats to the row
            f1_composite_losses, f1_composite_wins = 0, 0
            f1_composite_record = 0
            for fight in f1_fights:
                if fight['f1_name'] == row['f1_name']:
                    f1_composite_record += fight['f2_wins'] / fight['f2_losses'] if fight['f2_losses'] != 0 else fight['f2_wins']
                    f1_composite_losses += fight['f2_losses']
                    f1_composite_wins += fight['f2_wins']
                else:
                    f1_composite_record += fight['f1_wins'] / fight['f1_losses'] if fight['f1_losses'] != 0 else fight['f1_wins']
                    f1_composite_losses += fight['f1_losses']
                    f1_composite_wins += fight['f1_wins']

            # looping through all the fights f2 has been in and adding the stats to the row
            f2_composite_losses, f2_composite_wins = 0, 0
            f2_composite_record = 0
            for fight in f2_fights:
                if fight['f1_name'] == row['f2_name']:
                    f2_composite_record += fight['f2_wins'] / fight['f2_losses'] if fight['f2_losses'] != 0 else fight['f2_wins']
                    f2_composite_losses += fight['f2_losses']
                    f2_composite_wins += fight['f2_wins']
                else:
                    f2_composite_record += fight['f1_wins'] / fight['f1_losses'] if fight['f1_losses'] != 0 else fight['f1_wins']
                    f2_composite_losses += fight['f1_losses']
                    f2_composite_wins += fight['f1_wins']

            row['f1_composite_losses'] = f1_composite_losses / row['f1_losses'] if row['f1_losses'] != 0 else f1_composite_losses
            row['f1_composite_wins'] = f1_composite_wins / row['f1_wins'] if row['f1_wins'] != 0 else f1_composite_wins
            row['f1_composite_record'] = f1_composite_record
            row['f2_composite_losses'] = f2_composite_losses / row['f2_losses'] if row['f2_losses'] != 0 else f2_composite_losses
            row['f2_composite_wins'] = f2_composite_wins / row['f2_wins'] if row['f2_wins'] != 0 else f2_composite_wins
            row['f2_composite_record'] = f2_composite_record
            row['f1_height_reach_interaction'] = row['f1_height'] * row['f1_reach']
            row['f2_height_reach_interaction'] = row['f2_height'] * row['f2_reach']
        
        # create a pandas dataframe from the training_data list
        training_data = pd.DataFrame(training_data)

        # fix date column to be a datetime object with a timezone in UTC
        training_data['date'] = pd.to_datetime(training_data['date'], utc=True)

        # fill the NaN values with 0.0
        training_data = training_data.fillna(0.0)

        # write the training_data dataframe to a csv file
        training_data.to_csv('data/new_fights_ready.csv', index=False)


    def add_moneyline_odds_to_training_data(self, training_data):
        # open the ufc_odds file
        ufc_odds = pd.read_csv('data/ufc_odds.csv')

        for index, row in training_data.iterrows():
            # find the row in odds_data where the player and opponent are the same as the row in processed_data
            player_match = (ufc_odds['player1_name'] == row['f1_name']) & (ufc_odds['player2_name'] == row['f2_name'])
            opponent_match = (ufc_odds['player1_name'] == row['f2_name']) & (ufc_odds['player2_name'] == row['f1_name'])
            odds_row = ufc_odds[player_match | opponent_match]

            if odds_row.empty:
                training_data.loc[index, 'f1_odds'] = 0
                training_data.loc[index, 'f2_odds'] = 0
                continue

            # find the odds for the match winner
            odds_row = odds_row[odds_row['market_name'] == 'Moneyline']

            # date values are already normalized to 'YYYY-MM-DD' strings above
            date1 = row['date']
            # Ensure date1 is a datetime object, then add one day
            if not isinstance(date1, pd.Timestamp):
                date1_dt = pd.to_datetime(date1, utc=True, errors='coerce')
            else:
                date1_dt = date1
            date2 = (date1_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            date1 = date1_dt.strftime('%Y-%m-%d')

            # filter for matching event date
            odds_row = odds_row[(odds_row['start_date'] == date1) | (odds_row['start_date'] == date2)]

            if odds_row.empty:
                training_data.loc[index, 'f1_odds'] = 0
                training_data.loc[index, 'f2_odds'] = 0
                continue

            if len(odds_row) > 1:
                print(f'Multiple odds rows found for {row["f1_name"]} vs {row["f2_name"]} on {date1} or {date2}')
                continue

            # convert the odds and add them to the row in processed_data
            if odds_row['player1_name'].values[0] == row['f1_name']:
                training_data.loc[index, 'f1_odds'] = odds_row['player1_american_odds'].values[0]
                training_data.loc[index, 'f2_odds'] = odds_row['player2_american_odds'].values[0]
            else:
                training_data.loc[index, 'f1_odds'] = odds_row['player2_american_odds'].values[0]
                training_data.loc[index, 'f2_odds'] = odds_row['player1_american_odds'].values[0]
            # replace the match date in the processed_data with the match date in the odds_data
            date_val = self.standardize_date(odds_row['start_date'].values[0])
            training_data.loc[index, 'date'] = date_val

        return training_data


if __name__ == '__main__':
    import sys

    preprocessor = Preprocessor()

    if sys.argv[1] == 'train':
        preprocessor.generate_training_data()
    elif sys.argv[1] == 'new':
        preprocessor.generate_new_fight_rows()