from bs4 import BeautifulSoup
import pandas as pd
import requests, re
from functions.general import *
from datetime import datetime, timezone, timedelta

# database imports
import os, json, requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(supabase_url, supabase_key)

def extractDate(dateStr):
    dateStr = dateStr.split(' ')
    month = dateStr[0]
    if month == 'January':
        month = '01'
    elif month == 'February':
        month = '02'
    elif month == 'March':
        month = '03'
    elif month == 'April':
        month = '04'
    elif month == 'May':
        month = '05'
    elif month == 'June':
        month = '06'
    elif month == 'July':
        month = '07'
    elif month == 'August':
        month = '08'
    elif month == 'September':
        month = '09'
    elif month == 'October':
        month = '10'
    elif month == 'November':
        month = '11'
    elif month == 'December':
        month = '12'

    day = dateStr[1].replace(',', '')
    if len(day) == 1:
        day = '0' + day

    year = dateStr[2]
    return f"{month}-{day}-{year}"

def getNextFights():
    """
    1. Finds the upcoming fights
    2. Stores the [F1, F2, Date, Weight Class] for the upcoming fights in the fights.csv file
    """

    upcoming_fight_link = get_next_fight_link()
    wc_list = get_weight_classes(upcoming_fight_link)
    date = get_fight_date(upcoming_fight_link)

    df = pd.DataFrame(columns=['f1', 'f2', 'date', 'weight_class'])
    
    html = requests.get(upcoming_fight_link).text
    soup = BeautifulSoup(html, 'html.parser')
    # get all the fighter links for fighters in the next fight
    fighter_links = soup.find_all('a', class_='b-link b-link_style_black')
    
    # getting the names of the fighters
    i, x = 0, 0
    while i < len(fighter_links) - 1:
        if fighter_links[i].text.strip() == 'View Matchup' or fighter_links[i + 1].text.strip() == 'View Matchup':
            i += 1
            continue
        if x > len(wc_list) - 1:
            break
        print(fighter_links[i].text.strip(), fighter_links[i + 1].text.strip(), date, wc_list[x])
        new_row = {'f1': fighter_links[i].text.strip(), 'f2': fighter_links[i + 1].text.strip(), 'date': date, 'weight_class': wc_list[x]}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        i += 2
        x += 1
    
    # add the new fighters data to the fighters.csv file
    df.to_csv('data/fights.csv', index=False)

def getFightStats():
    fight_links = get_all_fight_links()

    # initialize a dataframe to store all fight data
    columns = ['date', 'f1', 'f2', 'f1_res', 'f2_res', 'weight_class', 'title_fight', 'method', 'round', 'time', 'time_format', 'referee', 'judge1', 'score1', 'judge2', 'score2', 'judge3', 'score3', 'kd_f1', 'kd_f2', 'sig_str_hit_f1', 'sig_str_tot_f1', 'sig_str_hit_f2', 'sig_str_tot_f2', 'sig_str_perc_f1', 'sig_str_perc_f2', 'total_str_hit_f1', 'total_str_tot_f1', 'total_str_hit_f2', 'total_str_tot_f2', 'td_hit_f1', 'td_tot_f1', 'td_hit_f2', 'td_tot_f2', 'td_perc_f1', 'td_perc_f2', 'sub_att_f1', 'sub_att_f2', 'rev_f1', 'rev_f2', 'ctrl_f1', 'ctrl_f2', 'head_str_hit_f1', 'head_str_tot_f1', 'head_str_hit_f2', 'head_str_tot_f2', 'body_str_hit_f1', 'body_str_tot_f1', 'body_str_hit_f2', 'body_str_tot_f2', 'leg_str_hit_f1', 'leg_str_tot_f1', 'leg_str_hit_f2', 'leg_str_tot_f2', 'dist_str_hit_f1', 'dist_str_tot_f1', 'dist_str_hit_f2', 'dist_str_tot_f2', 'clinc_str_hit_f1', 'clinc_str_tot_f1', 'clinc_str_hit_f2', 'clinc_str_tot_f2', 'ground_str_hit_f1', 'ground_str_tot_f1', 'ground_str_hit_f2', 'ground_str_tot_f2', 'head_str_perc_f1', 'head_str_perc_f2', 'body_str_perc_f1', 'body_str_perc_f2', 'leg_str_perc_f1', 'leg_str_perc_f2', 'dist_str_perc_f1', 'dist_str_perc_f2', 'clinc_str_perc_f1', 'clinc_str_perc_f2', 'ground_str_perc_f1', 'ground_str_perc_f2']
    df = pd.DataFrame(columns=columns)

    i = 0
    for link in fight_links:
        print(f'scraping fight: {i + 1} of {len(fight_links)}')

        # Total Stat Variables
        f1, f2, f1_res, f2_res, weight_class, title_fight, method, round, time, time_format, referee = '', '', '', '', '', '', '', '', '', '', ''
        judge1, score1, judge2, score2, judge3, score3, kd_f1, kd_f2, sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
        sig_str_perc_f1, sig_str_perc_f2, total_str_hit_f1, total_str_tot_f1, total_str_hit_f2, total_str_tot_f2, td_hit_f1, td_tot_f1, td_hit_f2, td_tot_f2, td_perc_f1, td_perc_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
        sub_att_f1, sub_att_f2, rev_f1, rev_f2, ctrl_f1, ctrl_f2 = '', '', '', '', '', ''
        # Significant Strike Stat Variables
        sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2, sig_str_perc_f1, sig_str_perc_f2, head_str_hit_f1, head_str_tot_f1, head_str_hit_f2, head_str_tot_f2 = '', '', '', '', '', '', '', '', '', ''
        body_str_hit_f1, body_str_tot_f1, body_str_hit_f2, body_str_tot_f2, leg_str_hit_f1, leg_str_tot_f1, leg_str_hit_f2, leg_str_tot_f2, dist_str_hit_f1, dist_str_tot_f1, dist_str_hit_f2, dist_str_tot_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
        clinc_str_hit_f1, clinc_str_tot_f1, clinc_str_hit_f2, clinc_str_tot_f2, ground_str_hit_f1, ground_str_tot_f1, ground_str_hit_f2, ground_str_tot_f2, head_str_perc_f1, head_str_perc_f2 = '', '', '', '', '', '', '', '', '', ''
        body_str_perc_f1, body_str_perc_f2, leg_str_perc_f1, leg_str_perc_f2, dist_str_perc_f1, dist_str_perc_f2 = '', '', '', '', '', ''
        clinc_str_perc_f1, clinc_str_perc_f2, ground_str_perc_f1, ground_str_perc_f2 = '', '', '', ''

        date = link.split(',')[1]
        html = requests.get(link.split(',')[0]).text
        soup = BeautifulSoup(html, 'html.parser')

        fighters = soup.find_all('a', class_='b-link')
        f1 = fighters[1].text.strip()
        f2 = fighters[2].text.strip()
        # print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        # print(f'{f1} vs {f2}')
        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        # print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

        winner = soup.find_all('i', class_='b-fight-details__person-status')
        f1_res = winner[0].text.strip()
        f2_res = winner[1].text.strip()
        # print('Red Fighter Res:', f1_res)
        # print('Blue Fighter Res:', f2_res)

        # create a list of all the possible weight classes
        weight_classes = ['Women\'s Strawweight', 'Women\'s Flyweight', 'Women\'s Bantamweight', 'Women\'s Featherweight', 'Women\'s Lightweight', 'Women\'s Welterweight', 'Women\'s Middleweight', 'Women\'s Light Heavyweight', 'Women\'s Heavyweight', 'Strawweight', 'Flyweight', 'Bantamweight', 'Featherweight', 'Lightweight', 'Welterweight', 'Middleweight', 'Light Heavyweight', 'Heavyweight']

        weight_class = soup.find('i', class_='b-fight-details__fight-title').text.strip().lower()
        for wc in weight_classes:
            if wc.lower() in weight_class:
                weight_class = wc
                break
        title_fight = 1 if 'title bout' in soup.find('i', class_='b-fight-details__fight-title').text.strip().lower() else 0

        method = soup.find_all('i', class_='b-fight-details__text-item_first')
        method = ''.join(method[0].text.strip().split(' ')[1:]).strip()
        # print('Method:', method)

        details = soup.find_all('i', class_='b-fight-details__text-item')
        round = ''
        z = 0
        for x in details:
            if z == 0:
                round = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                # print(f"{round[0]}: {round[1]}")
            elif z == 1:
                time = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                # print(f"{time[0]}: {time[1]}")
            elif z == 2:
                time_format = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                # print(f"{time_format[0]}: {time_format[1]}")
            elif z == 3:
                referee = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                # print(f"{referee[0]}: {referee[1]}")
            elif z == 4:
                str1 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                m = re.search(r'\d', str1)
                judge1 = str1[:m.start()].strip()
                score1 = str1[m.start():].strip()
                # print(f'{judge1}: {score1}')
            elif z == 5:
                str2 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                m = re.search(r'\d', str2)
                judge2 = str2[:m.start()].strip()
                score2 = str2[m.start():].strip()
                # print(f'{judge2}: {score2}')
            elif z == 6:
                str3 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                m = re.search(r'\d', str3)
                judge3 = str3[:m.start()].strip()
                score3 = str3[m.start():].strip()
                # print(f'{judge3}: {score3}')
            z += 1

        stats = soup.find_all('p', class_='b-fight-details__table-text')
        z = 0
        for x in stats:
            if z % 20 <= 1:
                z += 1
                if z % 20 == 1:
                    if z // 20 > 0:
                        break # don't need all round data rn but can add in the future if model still sucks lol
                        if z // 20 > int(round[1]):
                            break
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        # print(f"Round {z // 20} Stats ({z}):")
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    else:
                        temp = ''
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        # print("Total Stats:")
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                continue
            elif z % 20 == 2:
                kd_f1 = x.text.strip().replace('  ', '')
                # print('KD f1:', kd_f1)
            elif z % 20 == 3:
                kd_f2 = x.text.strip().replace('  ', '')
                # print('KD f2:', kd_f2)
            elif z % 20 == 4:
                sig_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                sig_str_hit_f1 = sig_str_f1[0]
                sig_str_tot_f1 = sig_str_f1[1]
                # print('Sig Str Hit f1:', sig_str_hit_f1)
                # print('Sig Str Tot f1:', sig_str_tot_f1)
            elif z % 20 == 5:
                sig_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                sig_str_hit_f2 = sig_str_f2[0]
                sig_str_tot_f2 = sig_str_f2[1]
                # print('Sig Str Hit f2:', sig_str_hit_f2)
                # print('Sig Str Tot f2:', sig_str_tot_f2)
            elif z % 20 == 6:
                sig_str_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('Sig Str Perc f1:', sig_str_perc_f1)
            elif z % 20 == 7:
                sig_str_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('Sig Str Perc f2:', sig_str_perc_f2)
            elif z % 20 == 8:
                total_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                total_str_hit_f1 = total_str_f1[0]
                total_str_tot_f1 = total_str_f1[1]
                # print('Total Str Hit f1:', total_str_hit_f1)
                # print('Total Str Tot f1:', total_str_tot_f1)
            elif z % 20 == 9:
                total_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                total_str_hit_f2 = total_str_f2[0]
                total_str_tot_f2 = total_str_f2[1]
                # print('Total Str Hit f2:', total_str_hit_f2)
                # print('Total Str Tot f2:', total_str_tot_f2)
            elif z % 20 == 10:
                td_f1 = x.text.strip().replace('  ', '').split(' of ')
                td_hit_f1 = td_f1[0]
                td_tot_f1 = td_f1[1]
                # print('TD Hit f1:', td_hit_f1)
                # print('TD Tot f1:', td_tot_f1)
            elif z % 20 == 11:
                td_f2 = x.text.strip().replace('  ', '').split(' of ')
                td_hit_f2 = td_f2[0]
                td_tot_f2 = td_f2[1]
                # print('TD Hit f2:', td_hit_f2)
                # print('TD Tot f2:', td_tot_f2)
            elif z % 20 == 12:
                td_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('TD Perc f1:', td_perc_f1)
            elif z % 20 == 13:
                td_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('TD Perc f2:', td_perc_f2)
            elif z % 20 == 14:
                sub_att_f1 = x.text.strip().replace('  ', '')
                # print('Sub Att f1:', sub_att_f1)
            elif z % 20 == 15:
                sub_att_f2 = x.text.strip().replace('  ', '')
                # print('Sub Att f2:', sub_att_f2)
            elif z % 20 == 16:
                rev_f1 = x.text.strip().replace('  ', '')
                # print('Rev f1:', rev_f1)
            elif z % 20 == 17:
                rev_f2 = x.text.strip().replace('  ', '')
                # print('Rev f2:', rev_f2)
            elif z % 20 == 18:
                ctrl_f1 = x.text.strip().replace('  ', '')
                # print('Ctrl f1:', ctrl_f1)
            elif z % 20 == 19:
                ctrl_f2 = x.text.strip().replace('  ', '')
                # print('Ctrl f2:', ctrl_f2)
            elif z % 20 == 20 or z % 20 == 21:
                z += 1
                continue
            elif z % 20 == 22:
                temp = ''
                # print('Round 1')
            else:
                temp = ''
                # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
            z += 1

        z = 1 + 20 * (int(round[1]) + 1) # start at the significant strike stats since we're currently skipping the round stats
        y = 0
        for x in stats:
            if y < z:
                y += 1
                continue
            elif y == z:
                y = 2
                z = -1
                # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                # print("Significant Strike Stats:")
                # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                continue
            if y % 18 <= 1:
                y += 1
                if y % 18 == 1:
                    if y // 18 > 0:
                        break # don't need all round data rn but can add in the future if model still sucks lol
                        if y // 18 > int(round[1]):
                            break
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        # print(f"Round {y // 18} Significant Strike Stats ({y}):")
                        # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                continue
            elif y % 18 == 2:
                sig_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                sig_str_hit_f1 = sig_str_f1[0]
                sig_str_tot_f1 = sig_str_f1[1]
                # print('Sig Str Hit f1:', sig_str_hit_f1)
                # print('Sig Str Tot f1:', sig_str_tot_f1)
            elif y % 18 == 3:
                sig_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                sig_str_hit_f2 = sig_str_f2[0]
                sig_str_tot_f2 = sig_str_f2[1]
                # print('Sig Str Hit f2:', sig_str_hit_f2)
                # print('Sig Str Tot f2:', sig_str_tot_f2)
            elif y % 18 == 4:
                sig_str_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('Sig Str Perc f1:', sig_str_perc_f1)
            elif y % 18 == 5:
                sig_str_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                # print('Sig Str Perc f2:', sig_str_perc_f2)
            elif y % 18 == 6:
                head_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                head_str_hit_f1 = head_str_f1[0]
                head_str_tot_f1 = head_str_f1[1]
                # print('Head Str Hit f1:', head_str_hit_f1)
                # print('Head Str Tot f1:', head_str_tot_f1)
            elif y % 18 == 7:
                head_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                head_str_hit_f2 = head_str_f2[0]
                head_str_tot_f2 = head_str_f2[1]
                # print('Head Str Hit f2:', head_str_hit_f2)
                # print('Head Str Tot f2:', head_str_tot_f2)
            elif y % 18 == 8:
                body_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                body_str_hit_f1 = body_str_f1[0]
                body_str_tot_f1 = body_str_f1[1]
                # print('Body Str Hit f1:', body_str_hit_f1)
                # print('Body Str Tot f1:', body_str_tot_f1)
            elif y % 18 == 9:
                body_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                body_str_hit_f2 = body_str_f2[0]
                body_str_tot_f2 = body_str_f2[1]
                # print('Body Str Hit f2:', body_str_hit_f2)
                # print('Body Str Tot f2:', body_str_tot_f2)
            elif y % 18 == 10:
                leg_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                leg_str_hit_f1 = leg_str_f1[0]
                leg_str_tot_f1 = leg_str_f1[1]
                # print('Leg Str Hit f1:', leg_str_hit_f1)
                # print('Leg Str Tot f1:', leg_str_tot_f1)
            elif y % 18 == 11:
                leg_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                leg_str_hit_f2 = leg_str_f2[0]
                leg_str_tot_f2 = leg_str_f2[1]
                # print('Leg Str Hit f2:', leg_str_hit_f2)
                # print('Leg Str Tot f2:', leg_str_tot_f2)
            elif y % 18 == 12:
                dist_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                dist_str_hit_f1 = dist_str_f1[0]
                dist_str_tot_f1 = dist_str_f1[1]
                # print('Dist Str Hit f1:', dist_str_hit_f1)
                # print('Dist Str Tot f1:', dist_str_tot_f1)
            elif y % 18 == 13:
                dist_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                dist_str_hit_f2 = dist_str_f2[0]
                dist_str_tot_f2 = dist_str_f2[1]
                # print('Dist Str Hit f2:', dist_str_hit_f2)
                # print('Dist Str Tot f2:', dist_str_tot_f2)
            elif y % 18 == 14:
                clinc_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                clinc_str_hit_f1 = clinc_str_f1[0]
                clinc_str_tot_f1 = clinc_str_f1[1]
                # print('Clinc Str Hit f1:', clinc_str_hit_f1)
                # print('Clinc Str Tot f1:', clinc_str_tot_f1)
            elif y % 18 == 15:
                clinc_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                clinc_str_hit_f2 = clinc_str_f2[0]
                clinc_str_tot_f2 = clinc_str_f2[1]
                # print('Clinc Str Hit f2:', clinc_str_hit_f2)
                # print('Clinc Str Tot f2:', clinc_str_tot_f2)
            elif y % 18 == 16:
                ground_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                ground_str_hit_f1 = ground_str_f1[0]
                ground_str_tot_f1 = ground_str_f1[1]
                # print('Ground Str Hit f1:', ground_str_hit_f1)
                # print('Ground Str Tot f1:', ground_str_tot_f1)
            elif y % 18 == 17:
                ground_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                ground_str_hit_f2 = ground_str_f2[0]
                ground_str_tot_f2 = ground_str_f2[1]
                # print('Ground Str Hit f2:', ground_str_hit_f2)
                # print('Ground Str Tot f2:', ground_str_tot_f2)
            else:
                temp = ''
                # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
            y += 1
        
        # print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        strike_stats = soup.find_all('i', class_='b-fight-details__charts-num')
        q = 0
        for x in strike_stats:
            if q == 0:
                head_str_perc_f1 = x.text.strip()
                # print('Head Str Perc f1:', head_str_perc_f1)
            elif q == 1:
                head_str_perc_f2 = x.text.strip()
                # print('Head Str Perc f2:', head_str_perc_f2)
            elif q == 2:
                body_str_perc_f1 = x.text.strip()
                # print('Body Str Perc f1:', body_str_perc_f1)
            elif q == 3:
                body_str_perc_f2 = x.text.strip()
                # print('Body Str Perc f2:', body_str_perc_f2)
            elif q == 4:
                leg_str_perc_f1 = x.text.strip()
                # print('Leg Str Perc f1:', leg_str_perc_f1)
            elif q == 5:
                leg_str_perc_f2 = x.text.strip()
                # print('Leg Str Perc f2:', leg_str_perc_f2)
            elif q == 6:
                dist_str_perc_f1 = x.text.strip()
                # print('Dist Str Perc f1:', dist_str_perc_f1)
            elif q == 7:
                dist_str_perc_f2 = x.text.strip()
                # print('Dist Str Perc f2:', dist_str_perc_f2)
            elif q == 8:
                clinc_str_perc_f1 = x.text.strip()
                # print('Clinc Str Perc f1:', clinc_str_perc_f1)
            elif q == 9:
                clinc_str_perc_f2 = x.text.strip()
                # print('Clinc Str Perc f2:', clinc_str_perc_f2)
            elif q == 10:
                ground_str_perc_f1 = x.text.strip()
                # print('Ground Str Perc f1:', ground_str_perc_f1)
            elif q == 11:
                ground_str_perc_f2 = x.text.strip()
                # print('Ground Str Perc f2:', ground_str_perc_f2)
            else:
                temp = ''
                # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
            q += 1
        
        # adding to the dataframe with concat
        data = [[date, f1, f2, f1_res, f2_res, weight_class, title_fight, method, round[1], f'{time[1]}:{time[2]}', time_format[1], referee[1], judge1, score1, judge2, score2, judge3, score3, kd_f1, kd_f2, sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2, sig_str_perc_f1, sig_str_perc_f2, total_str_hit_f1, total_str_tot_f1, total_str_hit_f2, total_str_tot_f2, td_hit_f1, td_tot_f1, td_hit_f2, td_tot_f2, td_perc_f1, td_perc_f2, sub_att_f1, sub_att_f2, rev_f1, rev_f2, ctrl_f1, ctrl_f2, head_str_hit_f1, head_str_tot_f1, head_str_hit_f2, head_str_tot_f2, body_str_hit_f1, body_str_tot_f1, body_str_hit_f2, body_str_tot_f2, leg_str_hit_f1, leg_str_tot_f1, leg_str_hit_f2, leg_str_tot_f2, dist_str_hit_f1, dist_str_tot_f1, dist_str_hit_f2, dist_str_tot_f2, clinc_str_hit_f1, clinc_str_tot_f1, clinc_str_hit_f2, clinc_str_tot_f2, ground_str_hit_f1, ground_str_tot_f1, ground_str_hit_f2, ground_str_tot_f2, head_str_perc_f1, head_str_perc_f2, body_str_perc_f1, body_str_perc_f2, leg_str_perc_f1, leg_str_perc_f2, dist_str_perc_f1, dist_str_perc_f2, clinc_str_perc_f1, clinc_str_perc_f2, ground_str_perc_f1, ground_str_perc_f2]]
        df = pd.concat([df, pd.DataFrame(data, columns=columns)], ignore_index=True)

        # if i == 10: # training wheels
        #     break
        i += 1
        # print()

        # order the dataframe by date
        df = df.sort_values(by='date', ascending=False)
    
        df.to_csv('data/fight_stats.csv', index=False)


def getNewFightStats():
    """
    This functions takes the most recent fight and extracts the now available
    fight data so it can be used to train the model

    1. Gets the links to the previous fights
    2. Iterates through each previous fight link and extracts the fight data
    """

    # get the links to the previous fights
    fight_links = get_fight_links(link_type='previous')

    # initialize a dataframe to store all fight data
    columns = ['date', 'f1', 'f2', 'f1_res', 'f2_res', 'weight_class', 'title_fight', 'method', 'round', 'time', 'time_format', 'referee', 'judge1', 'score1', 'judge2', 'score2', 'judge3', 'score3', 'kd_f1', 'kd_f2', 'sig_str_hit_f1', 'sig_str_tot_f1', 'sig_str_hit_f2', 'sig_str_tot_f2', 'sig_str_perc_f1', 'sig_str_perc_f2', 'total_str_hit_f1', 'total_str_tot_f1', 'total_str_hit_f2', 'total_str_tot_f2', 'td_hit_f1', 'td_tot_f1', 'td_hit_f2', 'td_tot_f2', 'td_perc_f1', 'td_perc_f2', 'sub_att_f1', 'sub_att_f2', 'rev_f1', 'rev_f2', 'ctrl_f1', 'ctrl_f2', 'head_str_hit_f1', 'head_str_tot_f1', 'head_str_hit_f2', 'head_str_tot_f2', 'body_str_hit_f1', 'body_str_tot_f1', 'body_str_hit_f2', 'body_str_tot_f2', 'leg_str_hit_f1', 'leg_str_tot_f1', 'leg_str_hit_f2', 'leg_str_tot_f2', 'dist_str_hit_f1', 'dist_str_tot_f1', 'dist_str_hit_f2', 'dist_str_tot_f2', 'clinc_str_hit_f1', 'clinc_str_tot_f1', 'clinc_str_hit_f2', 'clinc_str_tot_f2', 'ground_str_hit_f1', 'ground_str_tot_f1', 'ground_str_hit_f2', 'ground_str_tot_f2', 'head_str_perc_f1', 'head_str_perc_f2', 'body_str_perc_f1', 'body_str_perc_f2', 'leg_str_perc_f1', 'leg_str_perc_f2', 'dist_str_perc_f1', 'dist_str_perc_f2', 'clinc_str_perc_f1', 'clinc_str_perc_f2', 'ground_str_perc_f1', 'ground_str_perc_f2']
    df = pd.DataFrame(columns=columns)

    i = 0
    for link_details in fight_links:
        try:
            # Total Stat Variables
            f1, f2, f1_res, f2_res, weight_class, title_fight, method, round, time, time_format, referee = '', '', '', '', '', '', '', '', '', '', ''
            judge1, score1, judge2, score2, judge3, score3, kd_f1, kd_f2, sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
            sig_str_perc_f1, sig_str_perc_f2, total_str_hit_f1, total_str_tot_f1, total_str_hit_f2, total_str_tot_f2, td_hit_f1, td_tot_f1, td_hit_f2, td_tot_f2, td_perc_f1, td_perc_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
            sub_att_f1, sub_att_f2, rev_f1, rev_f2, ctrl_f1, ctrl_f2 = '', '', '', '', '', ''
            # Significant Strike Stat Variables
            sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2, sig_str_perc_f1, sig_str_perc_f2, head_str_hit_f1, head_str_tot_f1, head_str_hit_f2, head_str_tot_f2 = '', '', '', '', '', '', '', '', '', ''
            body_str_hit_f1, body_str_tot_f1, body_str_hit_f2, body_str_tot_f2, leg_str_hit_f1, leg_str_tot_f1, leg_str_hit_f2, leg_str_tot_f2, dist_str_hit_f1, dist_str_tot_f1, dist_str_hit_f2, dist_str_tot_f2 = '', '', '', '', '', '', '', '', '', '', '', ''
            clinc_str_hit_f1, clinc_str_tot_f1, clinc_str_hit_f2, clinc_str_tot_f2, ground_str_hit_f1, ground_str_tot_f1, ground_str_hit_f2, ground_str_tot_f2, head_str_perc_f1, head_str_perc_f2 = '', '', '', '', '', '', '', '', '', ''
            body_str_perc_f1, body_str_perc_f2, leg_str_perc_f1, leg_str_perc_f2, dist_str_perc_f1, dist_str_perc_f2 = '', '', '', '', '', ''
            clinc_str_perc_f1, clinc_str_perc_f2, ground_str_perc_f1, ground_str_perc_f2 = '', '', '', ''

            date = link_details[1]
            html = requests.get(link_details[0]).text
            soup = BeautifulSoup(html, 'html.parser')

            fighters = soup.find_all('a', class_='b-link')
            f1 = fighters[1].text.strip()
            f2 = fighters[2].text.strip()
            # print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            # print(f'{f1} vs {f2}')
            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            # print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

            winner = soup.find_all('i', class_='b-fight-details__person-status')
            f1_res = winner[0].text.strip()
            f2_res = winner[1].text.strip()
            # print('Red Fighter Res:', f1_res)
            # print('Blue Fighter Res:', f2_res)

            # create a list of all the possible weight classes
            weight_classes = ['Women\'s Strawweight', 'Women\'s Flyweight', 'Women\'s Bantamweight', 'Women\'s Featherweight', 'Women\'s Lightweight', 'Women\'s Welterweight', 'Women\'s Middleweight', 'Women\'s Light Heavyweight', 'Women\'s Heavyweight', 'Strawweight', 'Flyweight', 'Bantamweight', 'Featherweight', 'Lightweight', 'Welterweight', 'Middleweight', 'Light Heavyweight', 'Heavyweight']

            weight_class = soup.find('i', class_='b-fight-details__fight-title').text.strip().lower()
            for wc in weight_classes:
                if wc.lower() in weight_class:
                    weight_class = wc
                    break
            title_fight = 1 if 'title bout' in soup.find('i', class_='b-fight-details__fight-title').text.strip().lower() else 0

            method = soup.find_all('i', class_='b-fight-details__text-item_first')
            method = ''.join(method[0].text.strip().split(' ')[1:]).strip()
            # print('Method:', method)

            details = soup.find_all('i', class_='b-fight-details__text-item')
            round = ''
            z = 0
            for x in details:
                if z == 0:
                    round = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                    # print(f"{round[0]}: {round[1]}")
                elif z == 1:
                    time = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                    # print(f"{time[0]}: {time[1]}")
                elif z == 2:
                    time_format = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                    # print(f"{time_format[0]}: {time_format[1]}")
                elif z == 3:
                    referee = x.text.strip().replace('\n', '').replace('  ', '').split(':')
                    # print(f"{referee[0]}: {referee[1]}")
                elif z == 4:
                    str1 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                    m = re.search(r'\d', str1)
                    judge1 = str1[:m.start()].strip()
                    score1 = str1[m.start():].strip()
                    # print(f'{judge1}: {score1}')
                elif z == 5:
                    str2 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                    m = re.search(r'\d', str2)
                    judge2 = str2[:m.start()].strip()
                    score2 = str2[m.start():].strip()
                    # print(f'{judge2}: {score2}')
                elif z == 6:
                    str3 = x.text.strip().replace('\n', '').replace('  ', '').replace('.', '')
                    m = re.search(r'\d', str3)
                    judge3 = str3[:m.start()].strip()
                    score3 = str3[m.start():].strip()
                    # print(f'{judge3}: {score3}')
                z += 1

            stats = soup.find_all('p', class_='b-fight-details__table-text')
            z = 0
            for x in stats:
                if z % 20 <= 1:
                    z += 1
                    if z % 20 == 1:
                        if z // 20 > 0:
                            break # don't need all round data rn but can add in the future if model still sucks lol
                            if z // 20 > int(round[1]):
                                break
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                            # print(f"Round {z // 20} Stats ({z}):")
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                        else:
                            temp = ''
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                            # print("Total Stats:")
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    continue
                elif z % 20 == 2:
                    kd_f1 = x.text.strip().replace('  ', '')
                    # print('KD f1:', kd_f1)
                elif z % 20 == 3:
                    kd_f2 = x.text.strip().replace('  ', '')
                    # print('KD f2:', kd_f2)
                elif z % 20 == 4:
                    sig_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    sig_str_hit_f1 = sig_str_f1[0]
                    sig_str_tot_f1 = sig_str_f1[1]
                    # print('Sig Str Hit f1:', sig_str_hit_f1)
                    # print('Sig Str Tot f1:', sig_str_tot_f1)
                elif z % 20 == 5:
                    sig_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    sig_str_hit_f2 = sig_str_f2[0]
                    sig_str_tot_f2 = sig_str_f2[1]
                    # print('Sig Str Hit f2:', sig_str_hit_f2)
                    # print('Sig Str Tot f2:', sig_str_tot_f2)
                elif z % 20 == 6:
                    sig_str_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('Sig Str Perc f1:', sig_str_perc_f1)
                elif z % 20 == 7:
                    sig_str_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('Sig Str Perc f2:', sig_str_perc_f2)
                elif z % 20 == 8:
                    total_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    total_str_hit_f1 = total_str_f1[0]
                    total_str_tot_f1 = total_str_f1[1]
                    # print('Total Str Hit f1:', total_str_hit_f1)
                    # print('Total Str Tot f1:', total_str_tot_f1)
                elif z % 20 == 9:
                    total_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    total_str_hit_f2 = total_str_f2[0]
                    total_str_tot_f2 = total_str_f2[1]
                    # print('Total Str Hit f2:', total_str_hit_f2)
                    # print('Total Str Tot f2:', total_str_tot_f2)
                elif z % 20 == 10:
                    td_f1 = x.text.strip().replace('  ', '').split(' of ')
                    td_hit_f1 = td_f1[0]
                    td_tot_f1 = td_f1[1]
                    # print('TD Hit f1:', td_hit_f1)
                    # print('TD Tot f1:', td_tot_f1)
                elif z % 20 == 11:
                    td_f2 = x.text.strip().replace('  ', '').split(' of ')
                    td_hit_f2 = td_f2[0]
                    td_tot_f2 = td_f2[1]
                    # print('TD Hit f2:', td_hit_f2)
                    # print('TD Tot f2:', td_tot_f2)
                elif z % 20 == 12:
                    td_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('TD Perc f1:', td_perc_f1)
                elif z % 20 == 13:
                    td_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('TD Perc f2:', td_perc_f2)
                elif z % 20 == 14:
                    sub_att_f1 = x.text.strip().replace('  ', '')
                    # print('Sub Att f1:', sub_att_f1)
                elif z % 20 == 15:
                    sub_att_f2 = x.text.strip().replace('  ', '')
                    # print('Sub Att f2:', sub_att_f2)
                elif z % 20 == 16:
                    rev_f1 = x.text.strip().replace('  ', '')
                    # print('Rev f1:', rev_f1)
                elif z % 20 == 17:
                    rev_f2 = x.text.strip().replace('  ', '')
                    # print('Rev f2:', rev_f2)
                elif z % 20 == 18:
                    ctrl_f1 = x.text.strip().replace('  ', '')
                    # print('Ctrl f1:', ctrl_f1)
                elif z % 20 == 19:
                    ctrl_f2 = x.text.strip().replace('  ', '')
                    # print('Ctrl f2:', ctrl_f2)
                elif z % 20 == 20 or z % 20 == 21:
                    z += 1
                    continue
                elif z % 20 == 22:
                    temp = ''
                    # print('Round 1')
                else:
                    temp = ''
                    # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
                z += 1

            z = 1 + 20 * (int(round[1]) + 1) # start at the significant strike stats since we're currently skipping the round stats
            y = 0
            for x in stats:
                if y < z:
                    y += 1
                    continue
                elif y == z:
                    y = 2
                    z = -1
                    # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    # print("Significant Strike Stats:")
                    # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    continue
                if y % 18 <= 1:
                    y += 1
                    if y % 18 == 1:
                        if y // 18 > 0:
                            break # don't need all round data rn but can add in the future if model still sucks lol
                            if y // 18 > int(round[1]):
                                break
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                            # print(f"Round {y // 18} Significant Strike Stats ({y}):")
                            # print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                    continue
                elif y % 18 == 2:
                    sig_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    sig_str_hit_f1 = sig_str_f1[0]
                    sig_str_tot_f1 = sig_str_f1[1]
                    # print('Sig Str Hit f1:', sig_str_hit_f1)
                    # print('Sig Str Tot f1:', sig_str_tot_f1)
                elif y % 18 == 3:
                    sig_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    sig_str_hit_f2 = sig_str_f2[0]
                    sig_str_tot_f2 = sig_str_f2[1]
                    # print('Sig Str Hit f2:', sig_str_hit_f2)
                    # print('Sig Str Tot f2:', sig_str_tot_f2)
                elif y % 18 == 4:
                    sig_str_perc_f1 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('Sig Str Perc f1:', sig_str_perc_f1)
                elif y % 18 == 5:
                    sig_str_perc_f2 = x.text.strip().replace('  ', '') if x.text.strip().replace(' ', '') != '---' else '-1%'
                    # print('Sig Str Perc f2:', sig_str_perc_f2)
                elif y % 18 == 6:
                    head_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    head_str_hit_f1 = head_str_f1[0]
                    head_str_tot_f1 = head_str_f1[1]
                    # print('Head Str Hit f1:', head_str_hit_f1)
                    # print('Head Str Tot f1:', head_str_tot_f1)
                elif y % 18 == 7:
                    head_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    head_str_hit_f2 = head_str_f2[0]
                    head_str_tot_f2 = head_str_f2[1]
                    # print('Head Str Hit f2:', head_str_hit_f2)
                    # print('Head Str Tot f2:', head_str_tot_f2)
                elif y % 18 == 8:
                    body_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    body_str_hit_f1 = body_str_f1[0]
                    body_str_tot_f1 = body_str_f1[1]
                    # print('Body Str Hit f1:', body_str_hit_f1)
                    # print('Body Str Tot f1:', body_str_tot_f1)
                elif y % 18 == 9:
                    body_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    body_str_hit_f2 = body_str_f2[0]
                    body_str_tot_f2 = body_str_f2[1]
                    # print('Body Str Hit f2:', body_str_hit_f2)
                    # print('Body Str Tot f2:', body_str_tot_f2)
                elif y % 18 == 10:
                    leg_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    leg_str_hit_f1 = leg_str_f1[0]
                    leg_str_tot_f1 = leg_str_f1[1]
                    # print('Leg Str Hit f1:', leg_str_hit_f1)
                    # print('Leg Str Tot f1:', leg_str_tot_f1)
                elif y % 18 == 11:
                    leg_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    leg_str_hit_f2 = leg_str_f2[0]
                    leg_str_tot_f2 = leg_str_f2[1]
                    # print('Leg Str Hit f2:', leg_str_hit_f2)
                    # print('Leg Str Tot f2:', leg_str_tot_f2)
                elif y % 18 == 12:
                    dist_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    dist_str_hit_f1 = dist_str_f1[0]
                    dist_str_tot_f1 = dist_str_f1[1]
                    # print('Dist Str Hit f1:', dist_str_hit_f1)
                    # print('Dist Str Tot f1:', dist_str_tot_f1)
                elif y % 18 == 13:
                    dist_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    dist_str_hit_f2 = dist_str_f2[0]
                    dist_str_tot_f2 = dist_str_f2[1]
                    # print('Dist Str Hit f2:', dist_str_hit_f2)
                    # print('Dist Str Tot f2:', dist_str_tot_f2)
                elif y % 18 == 14:
                    clinc_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    clinc_str_hit_f1 = clinc_str_f1[0]
                    clinc_str_tot_f1 = clinc_str_f1[1]
                    # print('Clinc Str Hit f1:', clinc_str_hit_f1)
                    # print('Clinc Str Tot f1:', clinc_str_tot_f1)
                elif y % 18 == 15:
                    clinc_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    clinc_str_hit_f2 = clinc_str_f2[0]
                    clinc_str_tot_f2 = clinc_str_f2[1]
                    # print('Clinc Str Hit f2:', clinc_str_hit_f2)
                    # print('Clinc Str Tot f2:', clinc_str_tot_f2)
                elif y % 18 == 16:
                    ground_str_f1 = x.text.strip().replace('  ', '').split(' of ')
                    ground_str_hit_f1 = ground_str_f1[0]
                    ground_str_tot_f1 = ground_str_f1[1]
                    # print('Ground Str Hit f1:', ground_str_hit_f1)
                    # print('Ground Str Tot f1:', ground_str_tot_f1)
                elif y % 18 == 17:
                    ground_str_f2 = x.text.strip().replace('  ', '').split(' of ')
                    ground_str_hit_f2 = ground_str_f2[0]
                    ground_str_tot_f2 = ground_str_f2[1]
                    # print('Ground Str Hit f2:', ground_str_hit_f2)
                    # print('Ground Str Tot f2:', ground_str_tot_f2)
                else:
                    temp = ''
                    # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
                y += 1
            
            # print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

            strike_stats = soup.find_all('i', class_='b-fight-details__charts-num')
            q = 0
            for x in strike_stats:
                if q == 0:
                    head_str_perc_f1 = x.text.strip()
                    # print('Head Str Perc f1:', head_str_perc_f1)
                elif q == 1:
                    head_str_perc_f2 = x.text.strip()
                    # print('Head Str Perc f2:', head_str_perc_f2)
                elif q == 2:
                    body_str_perc_f1 = x.text.strip()
                    # print('Body Str Perc f1:', body_str_perc_f1)
                elif q == 3:
                    body_str_perc_f2 = x.text.strip()
                    # print('Body Str Perc f2:', body_str_perc_f2)
                elif q == 4:
                    leg_str_perc_f1 = x.text.strip()
                    # print('Leg Str Perc f1:', leg_str_perc_f1)
                elif q == 5:
                    leg_str_perc_f2 = x.text.strip()
                    # print('Leg Str Perc f2:', leg_str_perc_f2)
                elif q == 6:
                    dist_str_perc_f1 = x.text.strip()
                    # print('Dist Str Perc f1:', dist_str_perc_f1)
                elif q == 7:
                    dist_str_perc_f2 = x.text.strip()
                    # print('Dist Str Perc f2:', dist_str_perc_f2)
                elif q == 8:
                    clinc_str_perc_f1 = x.text.strip()
                    # print('Clinc Str Perc f1:', clinc_str_perc_f1)
                elif q == 9:
                    clinc_str_perc_f2 = x.text.strip()
                    # print('Clinc Str Perc f2:', clinc_str_perc_f2)
                elif q == 10:
                    ground_str_perc_f1 = x.text.strip()
                    # print('Ground Str Perc f1:', ground_str_perc_f1)
                elif q == 11:
                    ground_str_perc_f2 = x.text.strip()
                    # print('Ground Str Perc f2:', ground_str_perc_f2)
                else:
                    temp = ''
                    # print("WHAT AM I DOING HERE!?!?!?!?!? lol")
                q += 1
            
            # construct the row_id where the id is the fighter names in alphabetical order and the date in the format YYYY-MM-DD
            if f1 > f2:
                row_id = f'{f2}{f1}{date.strftime("%Y-%m-%d")}'
            else:
                row_id = f'{f1}{f2}{date.strftime("%Y-%m-%d")}'

            if f1 > f2:
                result = (f2_res == 'W')
            else:
                result = (f1_res == 'W')

            print(f'row_id: {row_id}, result: {result}')

            # adding the fight result to the db
            response = (
                supabase.table('events')
                .update({
                    'result': result,
                    'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                })
                .eq('id', row_id)
                .execute()
            )

            if response.data == [] and response.count == None:
                # reconstruct the row_id and shift the day forward by 1 day
                if f1 > f2:
                    row_id = f'{f2}{f1}{(date + timedelta(days=1)).strftime("%Y-%m-%d")}'
                else:
                    row_id = f'{f1}{f2}{(date + timedelta(days=1)).strftime("%Y-%m-%d")}'
                
                # adding the fight result to the db
                response = (
                    supabase.table('events')
                    .update({
                        'result': result,
                        'updated_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    })
                    .eq('id', row_id)
                    .execute()
                )

                if response.data == [] and response.count == None:
                    print(f'Failed to add fight result to the db for {row_id}')
                    continue
            
            # adding to the beginning of the dataframe with concat
            data = [[date, f1, f2, f1_res, f2_res, weight_class, title_fight, method, round[1], f'{time[1]}:{time[2]}', time_format[1], referee[1], judge1, score1, judge2, score2, judge3, score3, kd_f1, kd_f2, sig_str_hit_f1, sig_str_tot_f1, sig_str_hit_f2, sig_str_tot_f2, sig_str_perc_f1, sig_str_perc_f2, total_str_hit_f1, total_str_tot_f1, total_str_hit_f2, total_str_tot_f2, td_hit_f1, td_tot_f1, td_hit_f2, td_tot_f2, td_perc_f1, td_perc_f2, sub_att_f1, sub_att_f2, rev_f1, rev_f2, ctrl_f1, ctrl_f2, head_str_hit_f1, head_str_tot_f1, head_str_hit_f2, head_str_tot_f2, body_str_hit_f1, body_str_tot_f1, body_str_hit_f2, body_str_tot_f2, leg_str_hit_f1, leg_str_tot_f1, leg_str_hit_f2, leg_str_tot_f2, dist_str_hit_f1, dist_str_tot_f1, dist_str_hit_f2, dist_str_tot_f2, clinc_str_hit_f1, clinc_str_tot_f1, clinc_str_hit_f2, clinc_str_tot_f2, ground_str_hit_f1, ground_str_tot_f1, ground_str_hit_f2, ground_str_tot_f2, head_str_perc_f1, head_str_perc_f2, body_str_perc_f1, body_str_perc_f2, leg_str_perc_f1, leg_str_perc_f2, dist_str_perc_f1, dist_str_perc_f2, clinc_str_perc_f1, clinc_str_perc_f2, ground_str_perc_f1, ground_str_perc_f2]]
            df = pd.concat([df, pd.DataFrame(data, columns=columns)], ignore_index=True)

            i += 1
            # print()
        except Exception as e:
            print(f"Error processing fight {i}: {e}")
            continue

    # opening the fight_stats.csv file and adding the new fight data
    old_df = pd.read_csv('data/fight_stats.csv')
    old_df['date'] = pd.to_datetime(old_df['date'])

    # add the new fight data to the old_df
    df = pd.concat([old_df, df], ignore_index=True)
    # remove any duplicates from the dataframe by checking the f1 name and f2 name and date
    df = df.drop_duplicates(subset=['f1', 'f2', 'date'], keep='last', ignore_index=True)

    # order the dataframe by date
    df = df.sort_values(by='date', ascending=False)

    # saving the new fight data to the fight_stats.csv file
    df.to_csv('data/fight_stats.csv', index=False)


if __name__ == '__main__':
    # extracting the command line args
    import sys
    
    if sys.argv[1] == 'next':
        getNextFights()
    elif sys.argv[1] == 'stats':
        getFightStats()
    elif sys.argv[1] == 'new':
        getNewFightStats()