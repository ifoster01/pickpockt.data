# SCRAPING, PROCESSING, AND TRAINING THE NFL MODEL

## Instructions

## Navigate to /preprocessing
1. First you need to rescrape and preprocess all the nfl data
    - run: python3 game_scraper.py && python3 processing.py && python3 processing.py b

## Navigate to /training
4. Now you need to train the model on your now up-to-date data
    - run python3 train_xgb.py

## Super command:
python3 game_scraper.py && python3 processing.py && python3 processing.py b && cd .. && cd training && python3 train_xgb.py

## NEW SUPER COMMAND:
python3 odds_scraper.py && python3 odds_scraper_bkup.py && python3 game_scraper.py && python3 processing.py && cd .. && cd events && python3 upload_events.py && python3 update_completed_events.py && cd .. && cd training && python3 train_xgb.py