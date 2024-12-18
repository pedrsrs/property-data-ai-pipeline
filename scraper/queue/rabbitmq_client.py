import pika
import json

class RabbitMQClient:
    def __init__(self, host='localhost', port=5672, data_queue='scraper-data', status_queue='scraper-status'):
        self.host = host
        self.port = port 
        self.data_queue = data_queue
        self.status_queue = status_queue
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port)
        )
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(queue=self.data_queue, durable=True)
        self.channel.queue_declare(queue=self.status_queue, durable=True)
    
    def send_status(self, url, status):
        data = {"url": url, "status": status}
        self.channel.basic_publish(
            exchange='',
            routing_key=self.status_queue,
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,  
            )
        )
    
    def send_data(self, url, content, ad_type):
        data = {"url": url, "content": content, "ad_type": ad_type}
        self.channel.basic_publish(
            exchange='',
            routing_key=self.data_queue,
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,  
            )
        )
    
    def close(self):
        self.connection.close()
