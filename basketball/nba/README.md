## Full Step
1. Navigate to nba/preprocessing
2. run `python3 game_scraper.py && python3 odds_scraper.py && python3 processing.py && cd ../events && python3 upload_events.py && python3 update_completed_events.py && cd ../training && python3 train_xgb_model.py`