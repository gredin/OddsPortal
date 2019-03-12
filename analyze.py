import datetime
import decimal
import json

short_scores = set([])
tournament_names = set([])

allowed_tournament_names = {'2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019',
                            '2009/2010', '2010/2011', '2011/2012', '2012/2013', '2013/2014', '2014/2015', '2015/2016', '2016/2017', '2017/2018', '2018/2019'}

right = 0
wrong = 0

delta = 1  # à diviser par 100

net_winnings = []

probas = {}

with open("scraping/oddsportal_57618.jl") as f:
    for line_number, line in enumerate(f):
        scraped_item = json.loads(line)

        if 'from_alternative_structure' in scraped_item['match']:
            continue

        tournament_name = scraped_item['tournament']['name']
        tournament_names.add(tournament_name)

        if tournament_name not in allowed_tournament_names:
            continue

        short_score = scraped_item['match']['score_short']
        short_scores.add(short_score)

        try:
            home_score, away_score = short_score.split('\xa0')[0].split(':')
            home_score = int(home_score)
            away_score = int(away_score)

            outcome_from_score = "1" if home_score > away_score else ("X" if home_score == away_score else "2")
        except:
            continue

        for outcome, odds_per_bookie in scraped_item['match']['odds_data'].items():
            if outcome != '1':
                continue

            odds = []
            for bookie, odds_per_time in odds_per_bookie.items():
                last_odd = 0
                last_odd_time = datetime.datetime.strptime('1900-01-01T00:00:00+00:00', "%Y-%m-%dT%H:%M:%S+00:00")

                for odd_time_iso, odd in odds_per_time.items():
                    odd_time = datetime.datetime.strptime(odd_time_iso, "%Y-%m-%dT%H:%M:%S+00:00")
                    if odd_time > last_odd_time:
                        last_odd_time = odd_time
                        last_odd = decimal.Decimal(odd)

                if last_odd != 0:
                    odds.append(last_odd)

            if not odds:
                continue

            mean_odd = sum(odds) / len(odds)
            mean_proba = 1 / mean_odd

            prediction_is_correct = outcome_from_score == '1'

            delta_proba = decimal.Decimal("0.05")
            real_proba = mean_proba - delta_proba
            highest_odd = max(odds)
            if 1 / highest_odd < real_proba:
                if prediction_is_correct:
                    net_winnings.append(1/(1/mean_odd - delta_proba) - 1)
                else:
                    net_winnings.append(-1)

            """
            for i in range(-delta, delta + 1):
                proba = round(mean_proba, 2) + decimal.Decimal(i) / 100

                proba_100 = int(100 * proba)
                if proba_100 not in probas:
                    probas[proba_100] = []
                probas[proba_100].append(prediction_is_correct)
            """

#print(sorted(list(short_scores)))
#print(sorted(list(tournament_names)))


print(net_winnings)
print(len(net_winnings), sum(net_winnings))
exit()

for mean_proba in sorted(probas.keys()):
    outcomes_correct = probas[mean_proba]

    real_proba = len([v for v in outcomes_correct if v]) / len(outcomes_correct)

    print(len(outcomes_correct), "%.2f" % (mean_proba / 100), "%.2f" % real_proba, "%.2f" % (mean_proba / 100 - real_proba))

"""
odds_per_outcome = {"1": [], "X": [], "2": []}

for outcome, odds_per_bookie in scraped_item['match']['odds_data'].items():
    for bookie, odds_per_time in odds_per_bookie.items():
        for time, odd in odds_per_time.items():
            odds_per_outcome[outcome].append(decimal.Decimal(odd))



average_odd_1 = sum(odds_per_outcome["1"]) / len(odds_per_outcome["1"])
average_odd_X = sum(odds_per_outcome["X"]) / len(odds_per_outcome["X"])
average_odd_2 = sum(odds_per_outcome["2"]) / len(odds_per_outcome["2"])

if average_odd_2 < average_odd_1 and average_odd_2 < average_odd_X:
    if outcome_from_score == "2":
        right += 1
    else:
        wrong += 1
"""


# print(sorted(list(short_scores)))

# print(right, wrong)
