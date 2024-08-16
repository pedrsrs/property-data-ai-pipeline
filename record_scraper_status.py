import pandas as pd
import pika
import json

RECORD_FILE = 'scraper/data/olx_links.csv'

def record_status_in_csv(url, status):
    df = pd.read_csv(RECORD_FILE)
    df['status'] = df['status'].astype(object)
    df.loc[df['url'] == url, 'status'] = status
    df.to_csv(RECORD_FILE, index=False)

def callback(ch, method, properties, body):
    body_str = body.decode('utf-8')

    try:
        message = json.loads(body_str)
        record_status_in_csv(message['url'], message['status'])

    except ValueError as e:
        print(f" [!] Error: {e}")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='scraping-progress')

    channel.basic_consume(queue='scraping-progress', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()
