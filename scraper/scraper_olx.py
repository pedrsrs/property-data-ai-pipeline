import pika
import json
import csv
import time
from multiprocessing import Process
from playwright.sync_api import sync_playwright

NUM_OF_WORKERS = 1
INPUT_FILE = 'data/olx_links.csv'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='scraped-data')
channel.queue_declare(queue='scraping-progress')

def read_csv(filename):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        return [row['url'] for row in reader if row['status'] != 'finished']

def send_scraped_data(message, url):
    data = {
        "url": url,
        "content": message
    }
    channel.basic_publish(exchange='',
                          routing_key='scraped-data',
                          body=json.dumps(data))
    print(f"Data sent to 'scraped-data' for URL: {url}")

def send_scraping_status(url, status):
    data = {
        "url": url,
        "status": status
    }
    channel.basic_publish(exchange='',
                          routing_key='scraping-progress',
                          body=json.dumps(data))
    print(f"Data sent to 'scraping-progress' for URL: {url}, status: {status}")


def scrape_url(page, url, browser):
    scrape_page(page, url)
    go_to_next_page(page, url, browser)

def go_to_next_page(page, url, browser):
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            next_page_button = page.query_selector("text=Próxima página")
            next_page_url = page.evaluate("(element) => element.closest('a').href", next_page_button) if next_page_button else None
            if next_page_url:
                page.close()
                time.sleep(2)
                page = open_new_window(next_page_url, browser)
                scrape_url(page, url, browser)
                break
            else:
                time.sleep(1)
                page.reload()
                time.sleep(3)
        except Exception as e:
            retries += 1
            print(f"Error in go_to_next_page: {e} (Retry {retries}/{max_retries})")
            if "closed" in str(e):
                break  
            time.sleep(5)
            try:
                page.reload()
            except:
                break  
    if retries == max_retries:
        print(f"Max retries reached for {url}")
        send_scraping_status(url, "failed")

def scrape_page(page, url):
    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        try:
            page.wait_for_selector('script#__NEXT_DATA__', state='attached', timeout=20000) 
            script_content = page.evaluate('document.querySelector("script#__NEXT_DATA__").textContent')
            send_scraped_data(script_content, url)
            break  
        except Exception as e:
            retries += 1
            print(f"Error in scrape_page: {e} (Retry {retries}/{max_retries})")
            time.sleep(3)
            try:
                page.reload()
            except:
                break  
    else:
        print(f"Max retries reached for {url}, skipping.")
        send_scraping_status(url, "failed")
    
    if retries == max_retries:
        print(f"No #__NEXT_DATA__ found for {url}. Closing page.")
        page.close()
        send_scraping_status(url, "no_data")

def open_new_window(url, browser):
    context = None
    page = None
    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36")
            page = context.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_load_state('domcontentloaded', timeout=10000)
            break
        except Exception as e:
            print(f"Error opening new window for {url}: {e}")
            retries += 1
            if "net::ERR_NAME_NOT_RESOLVED" in str(e):
                time.sleep(5 * retries) 
            if page:
                page.close()
            if retries == max_retries:
                raise e  
    return page

def start_process(urls):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        for url in urls:
            try:
                page = open_new_window(url, browser)
                send_scraping_status(url, "started")
                print("Starting link:", url)
                
                while True:  # Loop through pages instead of recursion
                    scrape_page(page, url)
                    next_page_button = page.query_selector("text=Próxima página")
                    
                    if not next_page_button:
                        break  # Exit loop if no next page button
                    
                    next_page_url = page.evaluate("(element) => element.closest('a').href", next_page_button)
                    if not next_page_url:
                        break
                    
                    print(f"Navigating to next page: {next_page_url}")
                    page.goto(next_page_url)
                
                send_scraping_status(url, "finished")
            except Exception as e:
                print(f"Error processing {url}: {e}")
                send_scraping_status(url, "failed")
            finally:
                page.close()  # Ensure page is closed after use
        
        browser.close()
    connection.close()


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
