import json
import pandas as pd
import re
import requests
import time

from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from sqlalchemy import create_engine

# Settings for selenium
WINDOW_SIZE = "1920,1080"
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)

# Request headers
HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9,pl;q=0.8",
    "referer": "https://www.oddsportal.com/",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.99 Safari/537.36"
}

def get_response(url):    
    return requests.get(url, headers=HEADERS).text


### Scraper functions

# Fetch countries and leagues
def get_leagues():
    """
    Fetches all countries and leagues/cups that oddsportal has historical odds for
    """

    url = 'https://www.oddsportal.com/soccer/results/'
    response = get_response(url)
    
    soup = BeautifulSoup(response, "lxml")
    tds = soup.find('table', attrs={'class': 'table-main sport'}).find('tbody').find_all('td')
    
    league_links = defaultdict(lambda: defaultdict(dict))

    for td in tds:
        if td.find('a'):
            link = td.find('a').attrs['href']
            comp_name = td.find('a').text
            country, competition = link.split('/')[2:4]
            league_links[country][competition]['name'] = comp_name
    
    return league_links


# Fetch all seasons for competition
def get_seasons_per_comp(country, competition, name):
    """
    For a given competition find all seasons that oddsportal has historical odds for
    """
    url = f'https://www.oddsportal.com/soccer/{country}/{competition}/results/'
    
    soup = BeautifulSoup(get_response(url), "lxml")
    
    try:
        season_links = soup.find('div', attrs={'class': 'main-menu2 main-menu-gray'}).find_all('a')
    except:
        print(f'Fail: {country} - {competition}')
        return
    
    seasons = {}
    if season_links:   
        for season in season_links:
            try:
                start_year = int(season.text[:4])
                if start_year >= 2013:
                    seasons[season.text] = season.attrs['href']
            except:
                continue
                
    if seasons:
        seasons_df = pd.DataFrame({"extension": seasons}).reset_index()
        seasons_df['country'] = country
        seasons_df['competition'] = name
        return seasons_df
    
    else:
        print(f'Fail: {country} - {competition}')
        return


# Fetch match links
def get_url(extension, page):
    """
    Helper to flick through the results pages
    """
    return f'https://www.oddsportal.com{extension}#/page/{page}/'

def get_links_from_page(soup):
    """
    Read game links from an individual results page like this:
    https://www.oddsportal.com/soccer/england/premier-league/results/
    """
    table = soup.find('table', attrs={'id':'tournamentTable'})
    rows = table.find_all('tr', attrs={'class': 'deactivate'})
    
    links = []

    for row in rows:
        link_element = row.find('td', attrs={'class': 'table-participant'}).find('a')
        link = link_element.attrs['href']
        links.append(link)
        
    return links

def get_game_links_by_season(url_extension):
    """
    Scrape all game links for one season
    """
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

    game_links = []

    page = 1

    while page:
        url = get_url(url_extension, page)

        driver.get(url)
        time.sleep(3)

        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")

        links = get_links_from_page(soup)
        game_links += links

        if links:
            page += 1
            
        else:
            page = 0

    driver.quit()
    
    return list(set(game_links))


# Fetch bookie data
def get_bookies():
    """
    Fetches bookie ids and names
    """
    bookies_js = f"https://www.oddsportal.com/res/x/bookies-201014103652-{int(time.time())}.js"
    bookies = json.loads(re.findall(r'bookmakersData=({.*});var', get_response(bookies_js))[0])
    bookies_mapping = {key: value['WebName'] for key, value in bookies.items()}
    return bookies_mapping


# Get individual game info
def find_request_id(network_requests, match_id):
    """
    Function to find the network request that populates the odds data, it has the following form:
    https://fb.oddsportal.com/feed/match/1-1-2JDks1o7-1-2-yjd15.dat?_=1618320499284
    Where 'yjd15' is the id needed to make calls to the endpoint.
    '2JDks1o7' is the game_id and '-1-2-' is the market_id
    """
    
    url_start = 'https://fb.oddsportal.com/feed/match/'
    regex_pattern = '(?<=-)(.*?)(?=\.dat)'
    
    request_urls = [req for req in network_requests if (req.url.startswith(url_start)) & (match_id in req.url)]
    if not request_urls:
        return 'no id found'
    request_url = request_urls[0].url
    request_id = re.findall(regex_pattern, request_url)[0].split('-')[-1]
    
    return request_id


def get_teams(soup):
    """
    Helper to extract the team names
    """
    home, away = soup.find('h1').text.split(' - ')
    return home, away


def get_match_date(soup):
    """
    Helper to extract the match date
    """
    epoch_time_raw = soup.find('div', attrs={'id': 'col-content'}).find('p', attrs={'class': 'date'}).attrs['class'][-1]
    epoch_time = int(epoch_time_raw.split('-')[0][1:])
    return datetime.fromtimestamp(epoch_time)


def get_game_info(game_link_row, driver, sleep_time=1):
    """
    Fetch team names, dates, and request_id
    """
    game_info = {}
    game_info['match_id'] = game_link_row.match_id
    game_info['season_id'] = game_link_row.season_id


    url = 'https://www.oddsportal.com' + game_link_row.game_link

    try:
        driver.get(url)
        time.sleep(sleep_time)

        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")
        
        home, away = get_teams(soup)
        kickoff = get_match_date(soup)

        game_info['kickoff'] = kickoff
        game_info['home'] = home
        game_info['away'] = away
        
        request_id = find_request_id(driver.requests, game_link_row.match_id)

    except:
        print('Failed to fetch game_info for game: ', game_link_row.match_id)
        return None, None
        
    return game_info, request_id


def get_odds(game_id, request_id, market):
    """
    Use request_id to make a request to their odds endpoint
    """
    
    MARKET_MAPPING = {
        '1X2': '1-2',
        'CS': '8-2',
        'AHC': '5-2',
        'TG': '2-2'
    }
    
    time_now_ms = int(round(time.time() * 1000))
    market_abbr = MARKET_MAPPING[market]
    
    odds_data_js = f"https://fb.oddsportal.com/feed/match/1-1-{game_id}-{market_abbr}-{request_id}.dat?_={time_now_ms}"
    response = get_response(odds_data_js)
    odds_data = json.loads(re.findall(r"\.dat',\s({.*})", response)[0])
    return odds_data


def rename_outcomes(odds_dict):
    """
    Helper to give events more meaningful names
    """
    odds_dict['home'] = odds_dict.pop('0')
    odds_dict['draw'] = odds_dict.pop('1')
    odds_dict['away'] = odds_dict.pop('2')
    return odds_dict


def get_odds_from_list(odds_data_short, bookie_id):
    """
    Helper to extract opening and closing odds from odds_data if it's a list
    """
    cl_home, cl_draw, cl_away = odds_data_short['odds'][bookie_id]
    closing = {
        'home': cl_home,
        'draw': cl_draw,
        'away': cl_away
    }
    closing['time'] = datetime.fromtimestamp(max(odds_data_short['changeTime'][bookie_id]))
    closing['timing'] = 'closing'

    op_home, op_draw, op_away = odds_data_short['openingOdd'][bookie_id]
    opening = {
        'home': op_home,
        'draw': op_draw,
        'away': op_away
    }
    opening['time'] = datetime.fromtimestamp(max(odds_data_short['openingChangeTime'][bookie_id]))
    opening['timing'] = 'opening'
    
    return opening, closing


def get_odds_from_dict(odds_data_short, bookie_id):
    """
    Helper to extract opening and closing odds from odds_data if it's a dict
    """
    closing = {k: v for k, v in odds_data_short['odds'][bookie_id].items()}
    closing = rename_outcomes(closing)
    closing['time'] = datetime.fromtimestamp(max(odds_data_short['changeTime'][bookie_id].values()))
    closing['timing'] = 'closing'

    opening = {k: v for k, v in odds_data_short['openingOdd'][bookie_id].items()}
    opening = rename_outcomes(opening)
    opening['time'] = datetime.fromtimestamp(max(odds_data_short['openingChangeTime'][bookie_id].values()))
    opening['timing'] = 'opening'
    
    return opening, closing


def extract_1X2_odds(odds_data, bookie_id, match_id):
    """
    Extract 1x2 odds from a certain bookie from the odds_data response. 
    (The format of the response varies sometimes between a dict and a list)
    """
    odds_data_short = odds_data['d']['oddsdata']['back']['E-1-2-0-0-0']
    
    if type(odds_data_short['odds'][bookie_id]) == list:
        opening, closing = get_odds_from_list(odds_data_short, bookie_id)
        
    elif type(odds_data_short['odds'][bookie_id]) == dict:
        opening, closing = get_odds_from_dict(odds_data_short, bookie_id)
        
    else:
        opening = {}
        closing = {}
        print('no 1X2 odds found')
        
    opening['match_id'] = match_id
    closing['match_id'] = match_id
    
    return opening, closing
    

def extract_cs_odds(cs_data, bookie_id):
    """
    Extract correct score odds from a certain bookie from the odds_data response.
    """
    cs_data = cs_data['d']['oddsdata']['back']
    cs_odds = {}
    for key, value in cs_data.items():
        try:
            cs_odds[value['mixedParameterName']] = value['odds'][bookie_id][0]
        except:
            continue
    return cs_odds


def extract_odds(odds_data, bookie_id, outcome1, outcome2, match_id):

    all_odds = []
    
    for odds in odds_data['d']['oddsdata']['back'].values():
        for odds_type in [{'value': 'odds', 'timing': 'closing'}, {'value': 'openingOdd', 'timing': 'opening'}]:
            if bookie_id in odds[odds_type['value']].keys():
                odds_entry = {'match_id': match_id, 'timing': odds_type['timing']}
                odds_entry['line'] = odds['handicapValue']
                bookie_odds = odds[odds_type['value']][bookie_id]
                if type(bookie_odds) == list:
                    odds_entry[outcome1], odds_entry[outcome2] = bookie_odds
                    all_odds.append(odds_entry)
                elif type(bookie_odds) == dict:
                    odds_entry[outcome1] = bookie_odds['0']
                    odds_entry[outcome2] = bookie_odds['1']
                    all_odds.append(odds_entry)
    
    return all_odds


def get_odds_data_1x2(match_id, request_id, bookie_id='18'):
    odds_data_1x2 = get_odds(match_id, request_id, '1X2')
    opening, closing = extract_1X2_odds(odds_data_1x2, bookie_id, match_id)   
    return [opening, closing]

def get_odds_data_cs(match_id, request_id, bookie_id='16'):
    odds_data_cs = get_odds(match_id, request_id, 'CS')
    cs_odds = extract_cs_odds(odds_data_cs, bookie_id) 
    cs_odds_full = [{'match_id': match_id, 'home': int(key.split(':')[0]), 'away': int(key.split(':')[1]), 'odds': value} for key, value in cs_odds.items()]
    return cs_odds_full

def get_odds_data_ahc(match_id, request_id, bookie_id='18'):
    odds_data_ahc = get_odds(match_id, request_id, 'AHC')
    ahc_odds = extract_odds(odds_data_ahc, bookie_id, 'home', 'away', match_id)
    return ahc_odds

def get_odds_data_tg(match_id, request_id, bookie_id='18'):
    odds_data_tg = get_odds(match_id, request_id, 'TG')
    tg_odds = extract_odds(odds_data_tg, bookie_id, 'over', 'under', match_id)
    return tg_odds


def collect_data_by_season_id(game_links):
    """
    Given a dataframe with game links fetch all match information for those games
    """
    driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)
    
    game_infos = []
    
    odds_infos = {
    "1X2": [],
    "CS": [],
    "AHC": [],
    "TG": []
    }
    
    fetching_functions = {
    "1X2": get_odds_data_1x2,
    "CS": get_odds_data_cs,
    "AHC": get_odds_data_ahc,
    "TG": get_odds_data_tg
    }
    
    failed_games = []
    
    current_time = time.time()
    retry = 2
    
    while retry:
    
        for i, (idx, row) in enumerate(game_links.iterrows()):
            if i % 25 == 0 and i != 0:
                print(f"Last 25 matches took {int(time.time() - current_time)} seconds")
                current_time = time.time()
            try:
                game_info, request_id = get_game_info(row, driver)
                game_infos.append(game_info)
            except:
                print(f'error - game info - {row.match_id}')
                failed_games.append(row.match_id)
                continue

            for odds_type in ['1X2', 'CS', 'AHC', 'TG']:   

                try:
                    game_odds = fetching_functions[odds_type](row.match_id, request_id)
                    odds_infos[odds_type].extend(game_odds)
                except:
                    print(f'error - {odds_type} - {row.match_id}')
    
        if failed_games:
            game_links = game_links[game_links.match_id.isin(failed_games)].copy()
            failed_games = []
            retry -= 1
        else:
            retry = 0
            
    driver.quit()
            
    return game_infos, odds_infos, failed_games