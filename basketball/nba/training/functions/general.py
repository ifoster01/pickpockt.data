def get_team_from_name(team):
    team_names = {
        'bos': ['Boston Celtics', 'Celtics'],
        'nyk': ['New York Knicks', 'Knicks'],
        'phi': ['Philadelphia 76ers', '76ers'],
        'brk': ['Brooklyn Nets', 'New Jersey Nets', 'Nets'],
        'tor': ['Toronto Raptors', 'Raptors'],
        'cle': ['Cleveland Cavaliers', 'Cavaliers'],
        'mil': ['Milwaukee Bucks', 'Bucks'],
        'ind': ['Indiana Pacers', 'Pacers'],
        'det': ['Detroit Pistons', 'Pistons'],
        'chi': ['Chicago Bulls', 'Bulls'],
        'atl': ['Atlanta Hawks', 'Hawks'],
        'orl': ['Orlando Magic', 'Magic'],
        'mia': ['Miami Heat', 'Heat'],
        'cho': ['Charlotte Hornets', 'Charlotte Bobcats', 'Hornets'],
        'was': ['Washington Wizards', 'Wizards'],
        'okc': ['Oklahoma City Thunder', 'Seattle SuperSonics', 'Thunder'],
        'den': ['Denver Nuggets', 'Nuggets'],
        'min': ['Minnesota Timberwolves', 'Timberwolves'],
        'por': ['Portland Trail Blazers', 'Trail Blazers'],
        'uta': ['Utah Jazz', 'Jazz'],
        'lac': ['Los Angeles Clippers', 'Clippers'],
        'lal': ['Los Angeles Lakers', 'Lakers'],
        'sac': ['Sacramento Kings', 'Kings'],
        'gsw': ['Golden State Warriors', 'Warriors'],
        'pho': ['Phoenix Suns', 'Suns'],
        'hou': ['Houston Rockets', 'Rockets'],
        'mem': ['Memphis Grizzlies', 'Vancouver Grizzlies', 'Grizzlies'],
        'dal': ['Dallas Mavericks', 'Mavericks'],
        'sas': ['San Antonio Spurs', 'Spurs'],
        'nop': ['New Orleans Pelicans', 'New Orleans Hornets', 'Pelicans']
    }

    for key, value in team_names.items():
        if team in value:
            return key

def get_name_from_team(team):
    team_names = {
        'bos': ['Boston Celtics', 'Celtics'],
        'nyk': ['New York Knicks', 'Knicks'],
        'phi': ['Philadelphia 76ers', '76ers'],
        'brk': ['Brooklyn Nets', 'Nets'],
        'tor': ['Toronto Raptors', 'Raptors'],
        'cle': ['Cleveland Cavaliers', 'Cavaliers'],
        'mil': ['Milwaukee Bucks', 'Bucks'],
        'ind': ['Indiana Pacers', 'Pacers'],
        'det': ['Detroit Pistons', 'Pistons'],
        'chi': ['Chicago Bulls', 'Bulls'],
        'atl': ['Atlanta Hawks', 'Hawks'],
        'orl': ['Orlando Magic', 'Magic'],
        'mia': ['Miami Heat', 'Heat'],
        'cho': ['Charlotte Hornets', 'Hornets'],
        'was': ['Washington Wizards', 'Wizards'],
        'okc': ['Oklahoma City Thunder', 'Thunder'],
        'den': ['Denver Nuggets', 'Nuggets'],
        'min': ['Minnesota Timberwolves', 'Timberwolves'],
        'por': ['Portland Trail Blazers', 'Trail Blazers'],
        'uta': ['Utah Jazz', 'Jazz'],
        'lac': ['Los Angeles Clippers', 'Clippers'],
        'lal': ['Los Angeles Lakers', 'Lakers'],
        'sac': ['Sacramento Kings', 'Kings'],
        'gsw': ['Golden State Warriors', 'Warriors'],
        'pho': ['Phoenix Suns', 'Suns'],
        'hou': ['Houston Rockets', 'Rockets'],
        'mem': ['Memphis Grizzlies', 'Grizzlies'],
        'dal': ['Dallas Mavericks', 'Mavericks'],
        'sas': ['San Antonio Spurs', 'Spurs'],
        'nop': ['New Orleans Pelicans', 'Pelicans']
    }

    return team_names[team][0]

def get_image_url(team):
    team_images = {
        'bos': 'https://cdn.nba.com/logos/nba/1610612738/primary/L/logo.svg',
        'nyk': 'https://cdn.nba.com/logos/nba/1610612752/primary/L/logo.svg',
        'phi': 'https://cdn.nba.com/logos/nba/1610612755/primary/L/logo.svg',
        'brk': 'https://cdn.nba.com/logos/nba/1610612751/primary/L/logo.svg',
        'tor': 'https://cdn.nba.com/logos/nba/1610612761/primary/L/logo.svg',
        'cle': 'https://cdn.nba.com/logos/nba/1610612739/primary/L/logo.svg',
        'mil': 'https://cdn.nba.com/logos/nba/1610612749/primary/L/logo.svg',
        'ind': 'https://cdn.nba.com/logos/nba/1610612754/primary/L/logo.svg',
        'det': 'https://cdn.nba.com/logos/nba/1610612765/primary/L/logo.svg',
        'chi': 'https://cdn.nba.com/logos/nba/1610612741/primary/L/logo.svg',
        'atl': 'https://cdn.nba.com/logos/nba/1610612737/primary/L/logo.svg',
        'orl': 'https://cdn.nba.com/logos/nba/1610612753/primary/L/logo.svg',
        'mia': 'https://cdn.nba.com/logos/nba/1610612748/primary/L/logo.svg',
        'cho': 'https://cdn.nba.com/logos/nba/1610612766/primary/L/logo.svg',
        'was': 'https://cdn.nba.com/logos/nba/1610612764/primary/L/logo.svg',
        'okc': 'https://cdn.nba.com/logos/nba/1610612760/primary/L/logo.svg',
        'den': 'https://cdn.nba.com/logos/nba/1610612743/primary/L/logo.svg',
        'min': 'https://cdn.nba.com/logos/nba/1610612750/primary/L/logo.svg',
        'por': 'https://cdn.nba.com/logos/nba/1610612757/primary/L/logo.svg',
        'uta': 'https://cdn.nba.com/logos/nba/1610612762/primary/L/logo.svg',
        'lac': 'https://cdn.nba.com/logos/nba/1610612746/primary/L/logo.svg',
        'lal': 'https://cdn.nba.com/logos/nba/1610612747/primary/L/logo.svg',
        'sac': 'https://cdn.nba.com/logos/nba/1610612758/primary/L/logo.svg',
        'gsw': 'https://cdn.nba.com/logos/nba/1610612744/primary/L/logo.svg',
        'pho': 'https://cdn.nba.com/logos/nba/1610612756/primary/L/logo.svg',
        'hou': 'https://cdn.nba.com/logos/nba/1610612745/primary/L/logo.svg',
        'mem': 'https://cdn.nba.com/logos/nba/1610612763/primary/L/logo.svg',
        'dal': 'https://cdn.nba.com/logos/nba/1610612742/primary/L/logo.svg',
        'sas': 'https://cdn.nba.com/logos/nba/1610612759/primary/L/logo.svg',
        'nop': 'https://cdn.nba.com/logos/nba/1610612740/primary/L/logo.svg'
    }

    return team_images[team]

def get_png_url(team):
    team_images = {
        'bos': 'https://loodibee.com/wp-content/uploads/nba-boston-celtics-logo-300x300.png',
        'nyk': 'https://loodibee.com/wp-content/uploads/nba-new-york-knicks-logo-300x300.png',
        'phi': 'https://loodibee.com/wp-content/uploads/nba-philadelphia-76ers-logo-300x300.png',
        'brk': 'https://loodibee.com/wp-content/uploads/nba-brooklyn-nets-logo-300x300.png',
        'tor': 'https://loodibee.com/wp-content/uploads/nba-toronto-raptors-logo-2020-300x300.png',
        'cle': 'https://loodibee.com/wp-content/uploads/Clevelan-Cavaliers-logo-2022-300x300.png',
        'mil': 'https://loodibee.com/wp-content/uploads/nba-milwaukee-bucks-logo-300x300.png',
        'ind': 'https://loodibee.com/wp-content/uploads/nba-indiana-pacers-logo-300x300.png',
        'det': 'https://loodibee.com/wp-content/uploads/nba-detroit-pistons-logo-300x300.png',
        'chi': 'https://loodibee.com/wp-content/uploads/nba-chicago-bulls-logo-300x300.png',
        'atl': 'https://loodibee.com/wp-content/uploads/nba-atlanta-hawks-logo-300x300.png',
        'orl': 'https://loodibee.com/wp-content/uploads/nba-orlando-magic-logo-300x300.png',
        'mia': 'https://loodibee.com/wp-content/uploads/nba-miami-heat-logo-300x300.png',
        'cho': 'https://loodibee.com/wp-content/uploads/nba-charlotte-hornets-logo-300x300.png',
        'was': 'https://loodibee.com/wp-content/uploads/nba-washington-wizards-logo-300x300.png',
        'okc': 'https://loodibee.com/wp-content/uploads/nba-oklahoma-city-thunder-logo-300x300.png',
        'den': 'https://loodibee.com/wp-content/uploads/nba-denver-nuggets-logo-2018-300x300.png',
        'min': 'https://loodibee.com/wp-content/uploads/nba-minnesota-timberwolves-logo-300x300.png',
        'por': 'https://loodibee.com/wp-content/uploads/nba-portland-trail-blazers-logo-300x300.png',
        'uta': 'https://loodibee.com/wp-content/uploads/utah-jazz-logo-2022-300x300.png',
        'lac': 'https://loodibee.com/wp-content/uploads/NBA-LA-Clippers-logo-2024-300x300.png',
        'lal': 'https://loodibee.com/wp-content/uploads/nba-los-angeles-lakers-logo-300x300.png',
        'sac': 'https://loodibee.com/wp-content/uploads/nba-sacramento-kings-logo-300x300.png',
        'gsw': 'https://loodibee.com/wp-content/uploads/nba-golden-state-warriors-logo-2020-300x300.png',
        'pho': 'https://loodibee.com/wp-content/uploads/nba-phoenix-suns-logo-300x300.png',
        'hou': 'https://loodibee.com/wp-content/uploads/nba-houston-rockets-logo-2020-300x300.png',
        'mem': 'https://loodibee.com/wp-content/uploads/nba-memphis-grizzlies-logo-300x300.png',
        'dal': 'https://loodibee.com/wp-content/uploads/nba-dallas-mavericks-logo-300x300.png',
        'sas': 'https://loodibee.com/wp-content/uploads/nba-san-antonio-spurs-logo-300x300.png',
        'nop': 'https://loodibee.com/wp-content/uploads/nba-new-orleans-pelicans-logo-300x300.png'
    }

    return team_images[team]