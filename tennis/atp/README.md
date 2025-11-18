# SCRAPING, PROCESSING, AND TRAINING THE ATP MODEL

## Instructions

## Navigate to /preprocessing
1. First you need to scrape all the recent and upcoming ATP matches and odds then process the data
    - run: python3 atp_match_data.py new && python3 odds_scraper.py && python3 processing.py new

## Navigate to /training
4. Now you need to train the model on your now up-to-date data
    - run python3 train_xgb.py

## SUPERSTEP: (navigate to /preprocessing)
python3 odds_scraper.py && python3 atp_match_data.py new && python3 processing.py new && cd .. && cd events && python3 upload_events.py && cd .. && cd training && python3 train_xgb.py

## Total Games Processing
run python3 total_games_processing.py after processing.py
    --> creates player_data_games_processed.csv
    --> creates opponent_data_games_processed.csv
run ptg_xgb.py (player total games) to train the processing file
run otg_xgb.py (opponent total games) to train the processing file

### Files
- atp_match_data.py: scrapes all the match history for all players in currently ranked in the ATP
- hist_odds_scraper.py: scrapes all the odds for all the historical ATP matches -- needs to be updated so the dates align with the scraped match dates
- old_odds_scraper.py: fetches the odds data from tennis-data.co
- odds_scraper.py: scrapes all the odds for all the upcoming ATP matches
- processing.py: processes all the match data and add the odds data