def convert_probability_to_american(win_prob):
    if win_prob<.5:
        fighterOdds=round(100/win_prob - 100)
        opponentOdds = round(1 / (1 / (1 - win_prob) - 1) * 100)
        return [fighterOdds,-1*opponentOdds]
    else:
        fighterOdds = round(1 / (1 / win_prob - 1) * 100)
        opponentOdds = round(100 / (1 - win_prob) - 100)
        return [-1*fighterOdds,opponentOdds]

def convert_american_to_probability(odds):
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    else:
        return 100 / (abs(odds) + 100)