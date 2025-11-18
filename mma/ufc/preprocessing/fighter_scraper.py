from bs4 import BeautifulSoup
import pandas as pd
import requests
from functions.general import extractDate, get_fighter_links

def getFighters():
    # Initialize an empty DataFrame with the expected columns
    columns = [
        'name', 'nickname', 'record', 'Height', 'Weight', 'Reach', 'STANCE', 'DOB',
        'SLpM', 'Str. Acc.', 'SApM', 'Str. Def.', 'TD Avg.', 'TD Acc.', 'TD Def.', 'Sub. Avg.'
    ]
    fighter_stats = pd.DataFrame(columns=columns)
    pages = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    count = 0
    for page in pages:
        print(f'scraping page {count + 1} of {len(pages)}')
        count += 1

        html = requests.get(f'http://ufcstats.com/statistics/fighters?char={page}&page=all').text
        soup = BeautifulSoup(html, 'html.parser')

        fighter_links = soup.find_all('a', class_='b-link b-link_style_black')
        fighter_links = [link['href'] for link in fighter_links if link['href'].startswith('http://ufcstats.com/fighter-details/')]
        # remove duplicates
        fighter_links = list(set(fighter_links))

        # Fetching the stats for each fighter
        for link in fighter_links:
            fighter_html = requests.get(link).text
            fighter_soup = BeautifulSoup(fighter_html, 'html.parser')

            # Fetching the name
            name = fighter_soup.find('span', class_='b-content__title-highlight').text.strip()
            # Fetching the nickname
            nickname = fighter_soup.find('p', class_='b-content__Nickname').text.strip()
            # Fetching the record
            record = fighter_soup.find('span', class_='b-content__title-record').text.strip()
            
            # Creating a new row with initial values
            new_row = {'name': name, 'nickname': nickname, 'record': record}

            # Fetching the stats
            stats = fighter_soup.find_all('li', class_='b-list__box-list-item b-list__box-list-item_type_block')
            for stat in stats:
                stat_name = stat.find('i').text.strip().replace(':', '')  # Remove trailing colons
                stat_value = stat.text.replace(stat.find('i').text, '').strip()
                if stat_name in columns:  # Add only if the stat is in the predefined columns
                    new_row[stat_name] = stat_value
            
            # format the dob
            if (new_row['DOB'] == '--'):
                continue

            new_row['DOB'] = extractDate(new_row['DOB'])

            # Convert new_row to DataFrame and append
            fighter_stats = pd.concat([fighter_stats, pd.DataFrame(new_row, index=[0])], ignore_index=True)

    # writing the data to a csv file
    fighter_stats.to_csv('data/fighter_stats.csv', index=False)

def getNewFighters():
    # Initialize an empty DataFrame with the expected columns
    columns = [
        'name', 'nickname', 'record', 'Height', 'Weight', 'Reach', 'STANCE', 'DOB',
        'SLpM', 'Str. Acc.', 'SApM', 'Str. Def.', 'TD Avg.', 'TD Acc.', 'TD Def.', 'Sub. Avg.'
    ]
    fighter_stats = pd.DataFrame(columns=columns)
    
    # get the links to the new fighters
    fighter_links = get_fighter_links(link_type='upcoming')

    # Fetching the stats for each fighter
    for link in fighter_links:
        fighter_html = requests.get(link).text
        fighter_soup = BeautifulSoup(fighter_html, 'html.parser')

        # Fetching the name
        name = fighter_soup.find('span', class_='b-content__title-highlight').text.strip()
        # Fetching the nickname
        nickname = fighter_soup.find('p', class_='b-content__Nickname').text.strip()
        # Fetching the record
        record = fighter_soup.find('span', class_='b-content__title-record').text.strip()
        
        # Creating a new row with initial values
        new_row = {'name': name, 'nickname': nickname, 'record': record}

        # Fetching the stats
        stats = fighter_soup.find_all('li', class_='b-list__box-list-item b-list__box-list-item_type_block')
        for stat in stats:
            stat_name = stat.find('i').text.strip().replace(':', '')  # Remove trailing colons
            stat_value = stat.text.replace(stat.find('i').text, '').strip()
            if stat_name in columns:  # Add only if the stat is in the predefined columns
                new_row[stat_name] = stat_value

        # format the dob
        if (new_row['DOB'] == '--'):
            continue

        new_row['DOB'] = extractDate(new_row['DOB'])

        # Convert new_row to DataFrame and append
        fighter_stats = pd.concat([fighter_stats, pd.DataFrame(new_row, index=[0])], ignore_index=True)

    # reading the existing fighter_stats data and updating it if the fighter already exists or appending it if it is a new fighter
    existing_fighter_stats = pd.read_csv('data/fighter_stats.csv')
    existing_fighter_stats['DOB'] = pd.to_datetime(existing_fighter_stats['DOB'])
    fighter_stats = pd.concat([existing_fighter_stats, fighter_stats], ignore_index=True)
    fighter_stats.drop_duplicates(subset=['name'], keep='last', inplace=True)

    # writing the data to a csv file
    fighter_stats.to_csv('data/fighter_stats.csv', index=False)


if __name__ == '__main__':
    # extracting the command line arguments
    import sys

    if sys.argv[1] == 'fighters':
        getFighters()
    elif sys.argv[1] == 'new':
        getNewFighters()