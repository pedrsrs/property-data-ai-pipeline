import csv
import time
from multiprocessing import Process
from playwright.sync_api import sync_playwright

NUM_OF_WORKERS = 1
INPUT_FILE = 'olx_links.csv'
KAFKA_CONNECTION = 'kafka:29092'
KAFKA_SCRAPED_DATA_TOPIC = 'unparsed-data'
KAFKA_SCRAPING_STATUS_TOPIC = 'scraping-progress'

def read_csv(filename):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        return [row['url'] for row in reader if row['status'] != 'finished']
        
def scrape_url(page, url, browser):
    scrape_page(page, url)
    go_to_next_page(page, url, browser)

def go_to_next_page(page, url, browser):
    next_page_button = page.query_selector("text=Próxima página")

    next_page_url = page.evaluate("(element) => element.closest('a').href", next_page_button)
    print(next_page_url)
    if next_page_url:
        page.close()
        time.sleep(2)
        page = open_new_window(next_page_url, browser)
        scrape_url(page, url, browser)
    else:
        page.close()

def scrape_page(page, url):
    while True:
        try:
            page.wait_for_selector('script#__NEXT_DATA__', state='attached')
            script_content = page.evaluate('document.querySelector("script#__NEXT_DATA__").textContent')
            print(script_content)
               
        except Exception as e:
            time.sleep(1)
            page.reload()
            time.sleep(3)
            continue
        break

def open_new_window(url, browser):
    while True:
        try:
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36")
            page = context.new_page()
            page.goto(url)
            time.sleep(3)
        except Exception as e:
            page.close()
            time.sleep(1)
            continue
        break

    return page

def start_process(urls):
    scraped_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch()

        for url in urls:
            page = open_new_window(url, browser)
            #send_scraping_progress_kafka(url, "started")
            print("starting link:", url)
            scrape_url(page, url, browser)
            page.close()
            #send_scraping_progress_kafka(url, "finished")
        browser.close()

    return scraped_data

if __name__ == "__main__":
    urls = read_csv(INPUT_FILE)
    url_chunks = [urls[i::NUM_OF_WORKERS] for i in range(NUM_OF_WORKERS)]

    processes = []
    for chunk in url_chunks:
        process = Process(target=start_process, args=(chunk,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()  
