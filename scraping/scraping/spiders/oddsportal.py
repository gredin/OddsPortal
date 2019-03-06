# -*- coding: utf-8 -*-
import datetime
import decimal
import json
import re
import urllib
from os.path import abspath, dirname, join

import pytz
import scrapy
from bs4 import BeautifulSoup

REGEX_BOOKIES = re.compile("var bookmakersData=(\{.*?\});", re.DOTALL)  # .*? = non-greedy
REGEX_TOURNAMENT_ARCHIVE = re.compile("globals.jsonpCallback\('.+', (\{.+\})\);", re.DOTALL)
REGEX_PAGE_TOURNAMENT = re.compile("new PageTournament\((\{.*?\})\);")  # .*? = non-greedy
REGEX_PAGE_EVENT = re.compile("new PageEvent\((\{.+\})\);")
REGEX_MATCH = re.compile("globals.jsonpCallback\('.+.dat', (\{.+\})\);", re.DOTALL)
REGEX_POSTMATCHSCORE = re.compile("globals.jsonpCallback\('.+.dat', (\{.+\})\);", re.DOTALL)

ALLOWED_TOURNAMENT_NAMES = {'2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019',
                            '2009/2010', '2010/2011', '2011/2012', '2012/2013', '2013/2014', '2014/2015', '2015/2016', '2016/2017', '2017/2018', '2018/2019'}

with open(join(dirname(dirname(dirname(dirname(abspath(__file__))))), "bookies-190305102620-1551837467.js"), 'r') as f:
    bookies_content = f.read()
    result = re.match(REGEX_BOOKIES, bookies_content)
    bookies = json.loads(result.group(1))

    bookies_names = {bookie_id: bookie_details['WebName'] for bookie_id, bookie_details in bookies.items()}


class OddsportalSpider(scrapy.Spider):
    name = 'oddsportal'
    allowed_domains = ['oddsportal.com']

    custom_settings = {
        # 'CLOSESPIDER_ITEMCOUNT': 5,
        'FEED_URI': 'oddsportal.jl',  # TODO: edit
        'LOG_LEVEL': 'INFO',
        # 'DOWNLOAD_DELAY': 0.2,
        'CONCURRENT_REQUESTS': 12,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 6,
        'DOWNLOAD_TIMEOUT': 20,
        'ROBOTSTXT_OBEY': False,
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
    }

    def start_requests(self):
        # TODO: for testing
        for url in [
            "https://www.oddsportal.com/soccer/france/ligue-1/results/",
            "https://www.oddsportal.com/soccer/france/ligue-2/results/",
            "https://www.oddsportal.com/soccer/germany/bundesliga/results/",
            "https://www.oddsportal.com/soccer/germany/2-bundesliga/results/",
            "https://www.oddsportal.com/soccer/italy/serie-a/results/",
            "https://www.oddsportal.com/soccer/italy/serie-b/results/",
            "https://www.oddsportal.com/soccer/spain/laliga/results/",
            "https://www.oddsportal.com/soccer/spain/laliga2/results/",
            "https://www.oddsportal.com/soccer/england/premier-league/results/",
            "https://www.oddsportal.com/soccer/england/championship/results/",
            "https://www.oddsportal.com/soccer/brazil/serie-a/results/",
            "https://www.oddsportal.com/soccer/brazil/serie-b/results/",
            "https://www.oddsportal.com/soccer/argentina/superliga/results/",
            "https://www.oddsportal.com/soccer/colombia/liga-aguila/results/",
            "https://www.oddsportal.com/soccer/netherlands/eredivisie/results/",
        ]:
            yield scrapy.Request(
                url,
                self.parse_tournament(first=True),
                meta={
                    "sport": {
                        "id": 1,
                        "slug": "soccer",
                        "url": "https://www.oddsportal.com/soccer/results/"
                    },
                    "tournament_group": {
                        "url": "url",
                        "name": "%s/%s" % (url.split("/")[-4], url.split("/")[-3])
                    }
                }
            )

        return
        # TODO: for testing

        # TODO: for testing
        url = "https://www.oddsportal.com/soccer/france/ligue-1-2017-2018/results/"
        yield scrapy.Request(
            url,
            self.parse_tournament(first=False),
            meta={
                "sport": {
                    "id": 1,
                    "slug": "soccer",
                    "url": "https://www.oddsportal.com/soccer/results/"
                },
                "tournament_group": {
                    "url": "https://www.oddsportal.com/soccer/france/ligue-1/results/",
                    "name": "Ligue 1"
                },
                'tournament': {
                    'url': url,
                    'name': "2017/2018",
                }
            }
        )
        return
        # TODO: for testing

        sport_id = 1
        sport_slug = "soccer"
        sport_url = "https://www.oddsportal.com/%s/results/" % sport_slug

        yield scrapy.Request(url, self.parse_sport, meta={
            "sport": {
                "id": sport_id,
                "slug": sport_slug,
                "url": sport_url,
            }
        })

    def parse_sport(self, response):
        meta = response.meta

        for a in response.css(".table-main.sport .odd a"):
            url = a.css('::attr(href)').get()
            name = a.css('::text').get()

            meta['tournament_group'] = {
                "url": response.urljoin(url),
                "name": name,
            }

            yield response.follow(url, self.parse_tournament(first=True), meta=meta)

    def parse_tournament(self, first):
        def func(response):
            meta = response.meta

            result = re.search(REGEX_PAGE_TOURNAMENT, response.text)
            js_data = json.loads(result.group(1))

            sid = js_data['sid']
            tournament_id = js_data['id']
            use_premium = 1
            timezone_offset = 0
            page_nr = 1
            url = build_tournament_ajax_url(sid, tournament_id, use_premium, timezone_offset, page_nr)

            if first:
                name = response.css('.main-filter')[1].css('.active a::text').get()

                if name not in ALLOWED_TOURNAMENT_NAMES:
                    return

                meta['tournament'] = {
                    'url': meta['tournament_group']['url'],
                    'name': name,
                }

            meta['tournament']['js_data'] = js_data

            yield response.follow(url, self.parse_tournament_ajax(first=True), meta=meta)

            if first:
                for a in response.css('.main-filter')[1].css('a')[1:]:  # skip the first tournament
                    meta = response.meta

                    url = a.css('::attr(href)').get()
                    name = a.css('::text').get()

                    if name not in ALLOWED_TOURNAMENT_NAMES:
                        continue

                    meta['tournament'] = {
                        'url': response.urljoin(url),
                        'name': name,
                    }

                    yield response.follow(url, self.parse_tournament(first=False), meta=meta)

        return func

    def parse_tournament_ajax(self, first):
        def func(response):
            meta = response.meta

            result = re.match(REGEX_TOURNAMENT_ARCHIVE, response.text)
            html = json.loads(result.group(1))["d"]["html"]
            soup = BeautifulSoup(html, features="lxml")

            for tr in soup.select("table tr"):
                participant = tr.select('.table-participant a')
                score = tr.select('.table-score')

                if not (participant and score):
                    continue

                url = "https://www.oddsportal.com" + participant[0]['href']

                meta['match'] = {
                    'url': url,
                    'score_short': score[0].text.strip()
                }

                yield response.follow(url, self.parse_match, meta=meta)

            if first:
                page_nr_max = 0
                for a in soup.select("#pagination a"):
                    page_nr_max = max(page_nr_max, int(a['x-page']))

                for page_nr in range(2, page_nr_max + 1):
                    meta = response.meta

                    sid = meta['tournament']['js_data']['sid']
                    tournament_id = meta['tournament']['js_data']['id']
                    use_premium = 1
                    timezone_offset = 0

                    url = build_tournament_ajax_url(sid, tournament_id, use_premium, timezone_offset, page_nr)

                    yield response.follow(url, self.parse_tournament_ajax(first=False), meta=meta)

        return func

    def parse_match(self, response):
        meta = response.meta

        result = re.search(REGEX_PAGE_EVENT, response.text)
        js_data = json.loads(result.group(1))

        for key in ["xhash", "xhashf"]:
            js_data[key] = urllib.parse.unquote(js_data["xhash"])

        meta['match']['js_data'] = js_data

        version_id = 1
        sport_id = 1  # soccer
        scope_id = 2  # full time (other possible values: 1st hald, 2nd half)
        match_id = js_data["id"]
        betting_type = 1  # 1X2
        xhash = js_data['xhash']

        url = "https://fb.oddsportal.com/feed/match/%d-%d-%s-%d-%d-%s.dat" % (
            version_id,
            sport_id,
            match_id,
            betting_type,
            scope_id,
            xhash
        )

        meta['match']['url_ajax_match'] = url
        meta['match']['url_ajax_postmatchscore'] = "https://fb.oddsportal.com/feed/postmatchscore/%d-%s-%s.dat" % (sport_id, match_id, xhash)

        yield response.follow(url, self.parse_match_ajax, meta=meta)

    def parse_match_ajax(self, response):
        meta = response.meta

        result = re.match(REGEX_MATCH, response.text)
        match = json.loads(result.group(1))

        outcomes_names = {
            "0": "1",
            "1": "X",
            "2": "2",
        }

        outcome_ids = match["d"]["oddsdata"]["back"]["E-1-2-0-0-0"]["OutcomeID"]
        if isinstance(outcome_ids, list):
            meta['match']['from_alternative_structure'] = 1
            outcome_ids = {str(outcome_number): outcome_id for outcome_number, outcome_id in enumerate(outcome_ids)}
        outcomes = {outcome_id: outcome_number for outcome_number, outcome_id in outcome_ids.items()}

        odds_data = {outcome_name: {} for _, outcome_name in outcomes_names.items()}

        history = match["d"]["history"]["back"]
        if history:
            for outcome_id, odds_per_bookie in history.items():
                outcome_number = outcomes[outcome_id]
                outcome_name = outcomes_names[outcome_number]

                for bookie_id, odds in odds_per_bookie.items():
                    bookie_id_name = '%s-%s' % (bookie_id, bookies_names[bookie_id])
                    odds_data[outcome_name][bookie_id_name] = {}

                    for odd, _, timestamp in odds:
                        time_iso = timestamp_to_iso_utc(timestamp)
                        odds_data[outcome_name][bookie_id_name][time_iso] = round(decimal.Decimal(odd), 2)

        last_odds = match["d"]["oddsdata"]["back"]["E-1-2-0-0-0"]["odds"]
        last_odd_times = match["d"]["oddsdata"]["back"]["E-1-2-0-0-0"]["change_time"]
        for bookie_id in last_odds.keys():
            last_odds_of_bookie = last_odds[bookie_id]
            if isinstance(last_odds_of_bookie, list):
                meta['match']['from_alternative_structure'] = 1
                last_odds_of_bookie = {str(outcome_number): last_odd for outcome_number, last_odd in enumerate(last_odds_of_bookie)}

            bookie_id_name = '%s-%s' % (bookie_id, bookies_names[bookie_id])

            for outcome_number in last_odds_of_bookie:
                outcome_name = outcomes_names[outcome_number]

                if bookie_id_name not in odds_data[outcome_name]:
                    odds_data[outcome_name][bookie_id_name] = {}

                last_odd_times_of_bookie = last_odd_times[bookie_id]
                if isinstance(last_odd_times_of_bookie, list):
                    meta['match']['from_alternative_structure'] = 1
                    last_odd_times_of_bookie = {str(outcome_number): last_odd_time for outcome_number, last_odd_time in enumerate(last_odd_times_of_bookie)}

                odd = decimal.Decimal(last_odds_of_bookie[outcome_number])
                timestamp = last_odd_times_of_bookie[outcome_number]
                time_iso = timestamp_to_iso_utc(timestamp)

                odds_data[outcome_name][bookie_id_name][time_iso] = round(decimal.Decimal(odd), 2)

        meta['match']['odds_data'] = odds_data

        url = meta['match']['url_ajax_postmatchscore']  # TODO dirty hack for calling another url afterwards

        yield response.follow(url, self.parse_postmatchscore_ajax, meta=meta)

    def parse_postmatchscore_ajax(self, response):
        meta = response.meta

        result = re.match(REGEX_POSTMATCHSCORE, response.text)
        postmatchscore = json.loads(result.group(1))

        postmatchscore['d']['startTime'] = timestamp_to_iso_utc(int(postmatchscore['d']['startTime']))

        meta['match']['postmatchscore_data'] = postmatchscore

        scraped_item = ScrapedItem(sport=meta['sport'],
                                   tournament_group=meta['tournament_group'],
                                   tournament=meta['tournament'],
                                   match=meta['match'])

        yield scraped_item


def timestamp_to_iso_utc(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, pytz.UTC).isoformat()


def build_tournament_ajax_url(sid, tournament_id, use_premium, timezone_offset, page_nr):
    return "https://fb.oddsportal.com/ajax-sport-country-tournament-archive/%d/%s/%s/%d/%d/%d/" % (
        sid,
        tournament_id,
        getBookieHash(),
        use_premium,
        timezone_offset,
        page_nr
    )


def getBookieHash():
    max_bookmaker_id = 531
    is_my_bookmaker_array = [True] * (max_bookmaker_id + 1)

    k = 0
    batch_cnt = 30

    bookiehash = ""
    index = 0
    while index <= len(is_my_bookmaker_array):
        bookiehash_int = 0
        for j in range(batch_cnt):
            index = k * batch_cnt + j

            if index >= len(is_my_bookmaker_array):
                break

            if is_my_bookmaker_array[index]:
                bookiehash_int += 2 ** j

        k += 1
        bookiehash += 'X' + str(bookiehash_int)

    return bookiehash


class ScrapedItem(scrapy.Item):
    sport = scrapy.Field()
    tournament_group = scrapy.Field()
    tournament = scrapy.Field()
    match = scrapy.Field()

    def __str__(self):
        return "<ScrapedItem>"
