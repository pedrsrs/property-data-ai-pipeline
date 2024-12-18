import json
import csv
import re
import math
from multiprocessing import Process
from selectolax.parser import HTMLParser
import cloudscraper
import utils
import pandas as pd

WORKER_AMOUNT = 4
INPUT_FILE = 'olx_links.csv'
RENT_TABLE_NAME = "property_rent"
SALE_TABLE_NAME = "property_sale"

RESULTS_PER_PAGE = 50

client = utils.RabbitMQClient()

scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
        },
    )

def read_csv(filename):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        return [row['url'] for row in reader if row['status'] != 'finished']

def get_max_page(url):
    response = scraper.get(url)
    html = HTMLParser(response.text)
    results_string = html.css_first('div#total-of-ads > div > p').text()
    match = re.search(r"de (\d+) resultados", results_string.replace(".", ""))

    if match:
        total_results = int(match.group(1))  
        return math.ceil(total_results / RESULTS_PER_PAGE)
    else:
        raise ValueError("Could not find the number of results in the response")

def scrape_page(base_url, page_number):
    url = f'{base_url}{"&" if "?" in base_url else "?"}o={page_number}'
    response = scraper.get(url) 
    html = HTMLParser(response.text)

    if response.status_code == 200:
        results_json = html.css_first('script#__NEXT_DATA__').text()
        data, ad_type = map_data(url, results_json)
        client.send_data(url, data, ad_type)
    else:
        client.send_status(url, "failed")

def map_data(url, json_response):
    try:
        data_raw = json.loads(json_response)['props']['pageProps']['ads']
        data = []

        for item in data_raw:
            item = expand_properties(item)
            item_mapped = {
                'listing_id': item.get('listId'),
                'title': item.get('title'),
                'url': item.get('url'),
                'listing_date': item.get('date'),
                'municipality': item.get('locationDetails', {}).get('municipality'),
                'neighborhood': item.get('locationDetails', {}).get('neighbourhood'),
                'state': item.get('locationDetails', {}).get('uf'),
                'area': item.get('área construída'),
                'parking': item.get('vagas na garagem'),
                'bedrooms': item.get('quartos'),
                'bathrooms': item.get('banheiros'),
                'condominium_price': item.get('condominio'),
                'category': item.get('categoria'),
                'type': item.get('tipo'),
                'condominium_details': item.get('detalhes do condomínio'),
                'property_details': item.get('detalhes do imóvel'),
            }
            item_mapped['region'] = parse_region(url)
            data.append(item_mapped)

        ad_type = get_ad_type(url)
        return data, ad_type
    except Exception as e:
        print(f"Error in map_data: {e}")
        raise

def get_ad_type(url: str) -> str:
    if "venda" in url:
        return SALE_TABLE_NAME
    elif "aluguel" in url:
        return RENT_TABLE_NAME
    else:
        raise ValueError(f"Unknown URL: {url}")

def expand_properties(row):
    properties = row.get('properties', [])
    if not isinstance(properties, list):
        return pd.Series()

    return pd.Series({item['label'].lower(): item['value'] for item in properties})

def parse_region(url):
    pattern = re.compile(r'/([^?/]+)\?' if '?' in url else r'/([^/]+)$')
    match = pattern.search(url)
    return match.group(1).replace('-', ' ') if match else None
    
def scrape_url(url):
    max_page = get_max_page(url)

    for page_number in range(1, max_page + 1):
        scrape_page(url, page_number)

def start_process(urls):
    for url in urls:
        try:
            client.send_status(url, "started")
            scrape_url(url)
            client.send_status(url, "finished")

        except Exception as e:
            client.send_status(url, "failed")

if __name__ == "__main__":
    urls = read_csv(INPUT_FILE)
    url_chunks = [urls[i::WORKER_AMOUNT] for i in range(WORKER_AMOUNT)]

    processes = []
    for chunk in url_chunks:
        process = Process(target=start_process, args=(chunk,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()  
