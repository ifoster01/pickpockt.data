# SCRAPING, PROCESSING, AND TRAINING THE UFC MODEL

## Instructions

## Navigate to /preprocessing
1. First you need to update the fight stats data with the most recently completed fight (assuming it hasn't been processed yet)
    - run: python3 fight_scraper.py new

2. Next you need to scrape the upcoming fights and any new fighters from the ufc website... (START HERE IF UPDATING DATA PRE FIGHT)
    - run: python3 fight_scraper.py next
    - run: python3 fighter_scraper.py new

3. Next you need to update the historical data with the new fighters and the new fighter's data and/or any new fights and generate the new data for training and predicting
    - run: python3 processing.py generate
    - run: python3 processing.py train
    - run: python3 processing.py new

## Navigate to /training
4. Now you need to train the model on your now up-to-date data
    - run python3 train_xgb_model.py

## SUPERSTEP:
python3 fight_scraper.py next && python3 fighter_scraper.py new && python3 processing.py generate && python3 processing.py train && python3 processing.py new && cd .. && cd training && python3 train_xgb_model.py

## NEW SUPERSTEP:
python3 fight_scraper.py new && python3 fight_scraper.py next && python3 fighter_scraper.py new && python3 odds_scraper.py && python3 processing.py new && cd .. && cd events && python3 upload_events.py && cd .. && cd training && python3 train_xgb_model.py