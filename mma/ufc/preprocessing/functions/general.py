import pandas as pd
import requests
from bs4 import BeautifulSoup

def extractDate(dateStr):
    dateStr = dateStr.split(' ')
    month = dateStr[0]
    if month == 'January' or month == 'Jan':
        month = '01'
    elif month == 'February' or month == 'Feb':
        month = '02'
    elif month == 'March' or month == 'Mar':
        month = '03'
    elif month == 'April' or month == 'Apr':
        month = '04'
    elif month == 'May' or month == 'May':
        month = '05'
    elif month == 'June' or month == 'Jun':
        month = '06'
    elif month == 'July' or month == 'Jul':
        month = '07'
    elif month == 'August' or month == 'Aug':
        month = '08'
    elif month == 'September' or month == 'Sep':
        month = '09'
    elif month == 'October' or month == 'Oct':
        month = '10'
    elif month == 'November' or month == 'Nov':
        month = '11'
    elif month == 'December' or month == 'Dec':
        month = '12'

    day = dateStr[1].replace(',', '')
    if len(day) == 1:
        day = '0' + day

    year = dateStr[2]

    date_str = f"{year}-{month}-{day}"

    # convert to pd datetime object
    date = pd.to_datetime(date_str)

    return date

def get_all_fight_links():
    """
    This function gets the fight links from the ufc website
    """
    
    # get all of the completed fight links from ufcstats.com
    url = f'http://www.ufcstats.com/statistics/events/completed?page=all'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')

    event_links = soup.find_all('a', class_='b-link b-link_style_black')
    
    fight_links = []
    count = 0
    for event_link in event_links:
        event_soup = BeautifulSoup(requests.get(event_link['href']).text, 'html.parser')
        date = event_soup.find('li', class_='b-list__box-list-item').text.strip().replace('Date:', '').strip()
        date = extractDate(date)
        print(f'scraped event: {count + 1} on {date}')

        event_fight_links = event_soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')

        for event_fight_link in event_fight_links:
            fight_links.append(f'{event_fight_link['data-link']},{date}')
        count += 1

    return fight_links

def get_fight_date(url):
    """
    This function gets the date of the upcoming fight from the ufc website
    """
    
    if url is None:
        url = get_next_fight_link()
    
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('li', class_='b-list__box-list-item').text.strip().replace('Date:', '').strip()
    date = extractDate(date)

    return date

def get_weight_classes(url):
    """
    This function gets the weight classes from the ufc website
    """
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    
    # get all the weight classes for each fight
    weight_classes_data = soup.find_all('p', class_='b-fight-details__table-text')
    weight_classes = ['Women\'s Strawweight', 'Strawweight', 'Women\'s Flyweight', 'Flyweight', 'Women\'s Bantamweight', 'Bantamweight', 'Women\'s Featherweight', 'Featherweight', 'Women\'s Lightweight', 'Lightweight', 'Women\'s Welterweight', 'Welterweight', 'Women\'s Middleweight', 'Middleweight', 'Women\'s Light Heavyweight', 'Light Heavyweight', 'Women\'s Heavyweight', 'Heavyweight']
    wc_list = []
    for wc in weight_classes_data:
        temp = wc.text.replace('\n', '').strip()
        if temp in weight_classes:
            wc_list.append(temp)

    return wc_list

def get_next_fight_link():
    """
    This function gets the link to the next fight from the ufc website
    """
    
    # get the next fight link from ufcstats.com
    url = f'http://www.ufcstats.com/statistics/events/completed?page=all'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')

    link = soup.find('a', class_='b-link b-link_style_white')
    return link['href']

def get_previous_fight_link():
    """
    This function gets the link to the previous fight from the ufc website
    """
    
    # get the next fight link from ufcstats.com
    url = f'http://www.ufcstats.com/statistics/events/completed?page=all'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')

    link = soup.find('a', class_='b-link b-link_style_black')
    return link['href']


def get_fight_links(link_type='upcoming'):
    """
    This function gets the links to the upcoming fights from the ufc website
    """

    fight_night_link = None
    if link_type == 'upcoming':
        fight_night_link = get_next_fight_link()
    elif link_type == 'previous': 
        fight_night_link = get_previous_fight_link()
    
    if fight_night_link is None:
        return []

    # getting all the fight links from the fight night links
    fight_links = []
    html = requests.get(fight_night_link).text
    soup = BeautifulSoup(html, 'html.parser')
    date = soup.find('li', class_='b-list__box-list-item').text.strip().replace('Date:', '').strip()
    date = extractDate(date)
    fights = soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')
    
    for fight in fights:
        fight_links.append([fight['data-link'], date])

    return fight_links

def get_fighter_links(link_type='upcoming'):
    """
    This function gets the links to the fighters from the ufc website
    """

    if link_type == 'upcoming':
        fight_night_link = get_next_fight_link()
    elif link_type == 'previous':
        fight_night_link = get_previous_fight_link()
    
    if fight_night_link is None:
        return []
    
    # getting all the fighter links from the fight night links
    html = requests.get(fight_night_link).text
    soup = BeautifulSoup(html, 'html.parser')

    fighter_links_soup = soup.find_all('a', class_='b-link b-link_style_black')

    fighter_links = []
    for link in fighter_links_soup:
        try:
            fighter_links.append(link['href'])
        except:
            pass

    # remove duplicates
    fighter_links = list(set(fighter_links))

    return fighter_links

# return true if date1 is before date2, false otherwise
def compare_dates(date1, date2):
    """
    date1: str, date in the format 'MM-DD-YYYY'
    date2: str, date in the format 'MM-DD-YYYY'

    return: bool, true if date1 is before date2, false otherwise
    """

    date1 = date1.split('-')
    date2 = date2.split('-')

    if '' in date1 or '' in date2:
        return 'N/A'

    date1 = [int(i) for i in date1]
    date2 = [int(i) for i in date2]

    if date1[2] < date2[2]:
        return True
    elif date1[2] == date2[2] and date1[0] < date2[0]:
        return True
    elif date1[2] == date2[2] and date1[0] == date2[0] and date1[1] < date2[1]:
        return True
    else:
        return False

def in_last_x_years(date1, date2, num_years):
    """
    date1: str, date in the format 'MM-DD-YYYY'
    date2: str, date in the format 'MM-DD-YYYY'
    num_years: int, number of years to check if date1 is within

    return: bool, true if date1 is within the last x years of date2, false otherwise
    """

    # if date1 is after date2, return false
    if compare_dates(date1, date2) == False:
        return False

    date1 = date1.split('-')
    date2 = date2.split('-')

    if '' in date1 or '' in date2:
        return 'N/A'
    
    date1 = [int(i) for i in date1]
    date2 = [int(i) for i in date2]

    # if date1 is more than x years before date2, return false
    if date2[2] - date1[2] > num_years:
        return False
    elif date2[2] - date1[2] == num_years:
        if date1[0] < date2[0]:
            return False
        elif date1[0] == date2[0] and date1[1] < date2[1]:
            return False
    
    return True

def calculate_strike_math(row, fighter):
    head_weight = 0.1
    body_weight = 0.2
    leg_weight = 0.2
    distance_weight = 0.2
    clinc_weight = 0.1
    ground_weight = 0.2

    fight_time = row[f'{fighter}_last_yr_fight_time']

    head_strikes = (row[f'{fighter}_last_yr_fights_head_strikes_landed'] * head_weight) / fight_time if fight_time else 0
    body_strikes = (row[f'{fighter}_last_yr_fights_body_strikes_landed'] * body_weight) / fight_time if fight_time else 0
    leg_strikes = (row[f'{fighter}_last_yr_fights_leg_strikes_landed'] * leg_weight) / fight_time if fight_time else 0
    distance_strikes = (row[f'{fighter}_last_yr_fights_distance_strikes_landed'] * distance_weight) / fight_time if fight_time else 0
    clinc_strikes = (row[f'{fighter}_last_yr_fights_clinc_strikes_landed'] * clinc_weight) / fight_time if fight_time else 0
    ground_strikes = (row[f'{fighter}_last_yr_fights_ground_strikes_landed'] * ground_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_yr_strike_math'] = head_strikes + body_strikes + leg_strikes + distance_strikes + clinc_strikes + ground_strikes

    fight_time = row[f'{fighter}_last_X_yr_fight_time']
    
    head_strikes = (row[f'{fighter}_last_X_yr_fights_head_strikes_landed'] * head_weight) / fight_time if fight_time else 0
    body_strikes = (row[f'{fighter}_last_X_yr_fights_body_strikes_landed'] * body_weight) / fight_time if fight_time else 0
    leg_strikes = (row[f'{fighter}_last_X_yr_fights_leg_strikes_landed'] * leg_weight) / fight_time if fight_time else 0
    distance_strikes = (row[f'{fighter}_last_X_yr_fights_distance_strikes_landed'] * distance_weight) / fight_time if fight_time else 0
    clinc_strikes = (row[f'{fighter}_last_X_yr_fights_clinc_strikes_landed'] * clinc_weight) / fight_time if fight_time else 0
    ground_strikes = (row[f'{fighter}_last_X_yr_fights_ground_strikes_landed'] * ground_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_X_yr_strike_math'] = head_strikes + body_strikes + leg_strikes + distance_strikes + clinc_strikes + ground_strikes

    return row

def calculate_strike_def(row, fighter):
    head_weight = 0.1
    body_weight = 0.2
    leg_weight = 0.2
    distance_weight = 0.2
    clinc_weight = 0.1
    ground_weight = 0.2

    fight_time = row[f'{fighter}_last_yr_fight_time']

    head_strikes = (row[f'{fighter}_last_yr_fights_head_strikes_defended'] * head_weight) / fight_time if fight_time else 0
    body_strikes = (row[f'{fighter}_last_yr_fights_body_strikes_defended'] * body_weight) / fight_time if fight_time else 0
    leg_strikes = (row[f'{fighter}_last_yr_fights_leg_strikes_defended'] * leg_weight) / fight_time if fight_time else 0
    distance_strikes = (row[f'{fighter}_last_yr_fights_distance_strikes_defended'] * distance_weight) / fight_time if fight_time else 0
    clinc_strikes = (row[f'{fighter}_last_yr_fights_clinc_strikes_defended'] * clinc_weight) / fight_time if fight_time else 0
    ground_strikes = (row[f'{fighter}_last_yr_fights_ground_strikes_defended'] * ground_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_yr_strike_def'] = head_strikes + body_strikes + leg_strikes + distance_strikes + clinc_strikes + ground_strikes

    fight_time = row[f'{fighter}_last_X_yr_fight_time']
    
    head_strikes = (row[f'{fighter}_last_X_yr_fights_head_strikes_defended'] * head_weight) / fight_time if fight_time else 0
    body_strikes = (row[f'{fighter}_last_X_yr_fights_body_strikes_defended'] * body_weight) / fight_time if fight_time else 0
    leg_strikes = (row[f'{fighter}_last_X_yr_fights_leg_strikes_defended'] * leg_weight) / fight_time if fight_time else 0
    distance_strikes = (row[f'{fighter}_last_X_yr_fights_distance_strikes_defended'] * distance_weight) / fight_time if fight_time else 0
    clinc_strikes = (row[f'{fighter}_last_X_yr_fights_clinc_strikes_defended'] * clinc_weight) / fight_time if fight_time else 0
    ground_strikes = (row[f'{fighter}_last_X_yr_fights_ground_strikes_defended'] * ground_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_X_yr_strike_def'] = head_strikes + body_strikes + leg_strikes + distance_strikes + clinc_strikes + ground_strikes

    return row

def calculate_grapple_stats(row, fighter):
    takedown_weight = 0.4
    submission_weight = 0.3
    control_weight = 0.3

    fight_time = row[f'{fighter}_last_yr_fight_time']

    takedowns = (row[f'{fighter}_last_yr_fights_takedowns_landed'] * takedown_weight) / fight_time if fight_time else 0
    submissions = (row[f'{fighter}_last_yr_fights_submission_attempts'] * submission_weight) / fight_time if fight_time else 0
    control = (row[f'{fighter}_last_yr_fights_control_time'] * control_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_yr_grapple_stats'] = takedowns + submissions + control

    fight_time = row[f'{fighter}_last_X_yr_fight_time']

    takedowns = (row[f'{fighter}_last_X_yr_fights_takedowns_landed'] * takedown_weight) / fight_time if fight_time else 0
    submissions = (row[f'{fighter}_last_X_yr_fights_submission_attempts'] * submission_weight) / fight_time if fight_time else 0
    control = (row[f'{fighter}_last_X_yr_fights_control_time'] * control_weight) / fight_time if fight_time else 0

    row[f'{fighter}_last_X_yr_grapple_stats'] = takedowns + submissions + control

    return row


def condense_features(data, fighter):
    """
    Takes a pandas df row and condenses the features into a smaller set of features
    """

    # desired features:
    # last_yr_strike_math: combination of all the striking stats (head, body, leg, distance, clinc, ground) for the last year but weighted by the number of fights and the different type of strike
    # last_X_yr_strike_math: combination of all the striking stats (head, body, leg, distance, clinc, ground) for the last year but weighted by the number of fights and the different type of strike
    # last_yr_strike_def: combination of all the striking defense stats (head, body, leg, distance, clinc, ground) for the last year but weighted by the number of fights and the different type of strike
    # last_X_yr_strike_def: combination of all the striking defense stats (head, body, leg, distance, clinc, ground) for the last year but weighted by the number of fights and the different type of strike
    # last_yr_grapple_stats: combination of all the grappling stats (takedown, submission, control) for the last year but weighted by the number of fights and the different type of grapple
    # last_X_yr_grapple_stats: combination of all the grappling stats (takedown, submission, control) for the last year but weighted by the number of fights and the different type of grapple

    # print(json.dumps(data, indent=4))
    
    calculate_strike_math(data, fighter)
    calculate_strike_def(data, fighter)
    calculate_grapple_stats(data, fighter)

    return data

if __name__ == '__main__':
    # test the functions
    # fighter_links = get_fighter_links(link_type='previous')

    exit(1)