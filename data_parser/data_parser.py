import pika
import sys
import json
import pandas as pd
from sqlmodel import Field, SQLModel, create_engine, Session
from typing import Optional, Set, List, Dict, Union
from sqlalchemy import Column, String, Table, MetaData
from sqlalchemy.dialects import postgresql
from datetime import datetime
from pydantic import ValidationError, field_validator
import re

POSTGRES_USERNAME = 'user'
POSTGRES_PASSWORD = 'passwd'
POSTGRES_DB_NAME = 'property-db'
DB_URL = f'postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB_NAME}'

RENT_TABLE_NAME = "property_rent"
SALE_TABLE_NAME = "property_sale"

class Property(SQLModel, table=False): 
    source: str = None
    listing_id: str = None
    title: str
    price: Optional[float] = None
    property_url: str
    listing_date: Optional[datetime] = None  
    municipality: Optional[str] = None
    neighborhood: Optional[str] = None
    state: Optional[str] = None
    bathrooms: Optional[int] = None
    condominium_price: Optional[float] = None
    condominium_details: Optional[Set[str]] = Field(default=None, sa_column=Column(postgresql.ARRAY(String())))
    property_details: Optional[Set[str]] = Field(default=None, sa_column=Column(postgresql.ARRAY(String())))
    iptu: Optional[float] = None
    bedrooms: Optional[int] = None
    type: Optional[Set[str]] = Field(default=None, sa_column=Column(postgresql.ARRAY(String())))
    parking: Optional[int] = None
    area: Optional[float] = None
    scraping_date: Optional[datetime] = Field(default=datetime.today())
    region: Optional[str] = None

    @field_validator('bedrooms', 'bathrooms', 'parking', mode='before')
    def parse_details(cls, value):
        if value is None or value == '':
            return None
        value_str = str(value).strip()
        match = re.search(r'(\d+)', value_str)
        if match:
            return int(match.group(1))
        return None

    @field_validator('listing_id', mode='before')
    def convert_listing_id_to_str(cls, value):
        return str(int(value)) if isinstance(value, (int, float)) else value

    @field_validator('condominium_details', 'property_details', 'type', mode='before')
    def process_details(cls, value: Union[str, List[str], Set[str]]):
        if isinstance(value, str):
            return set(value.split(", ")) if ", " in value else {value}
        return set(value) if isinstance(value, (list, set)) else None

    @field_validator('listing_date', mode='before')
    def convert_epoch_to_datetime(cls, value):
        return datetime.fromtimestamp(value) if isinstance(value, int) else value

    @field_validator('price', 'condominium_price', 'iptu', 'area', mode='before')
    def convert_currency_to_float(cls, value):
        if value and isinstance(value, str):
            value = re.sub(r'[^0-9,\.]', '', value) 
            value = float(value.replace('.', '').replace(',', '.')) 

        return value if isinstance(value, (int, float)) else None

    @classmethod
    def create_dynamic_table(cls, table_name: str):
        metadata = MetaData()
        return Table(table_name, metadata, *cls.__table__.columns)

def insert_data_to_db(table_name: str, model_dicts: List[Dict]):
    engine = create_engine(DB_URL)
    metadata = MetaData()
    dynamic_table = Table(table_name, metadata, autoload_with=engine)

    with Session(engine) as session:
        for model_dict in model_dicts:
            stmt = dynamic_table.insert().values(**model_dict)
            session.exec(stmt)
        session.commit()

def df_to_sqlmodel_dicts(data: List[Dict], model_class: SQLModel) -> List[Dict]:
    sqlmodel_dicts = []
    for record in data:
        try:
            df = pd.DataFrame(record, index=[0])
            rows_as_dicts = df.to_dict(orient="records")
            instances = [model_class(**row) for row in rows_as_dicts]
            sqlmodel_dicts.extend(instance.model_dump() for instance in instances)
        except ValidationError as e:
            print(f"Skipping records due to validation error: {e}")
        except Exception as e:
            print(f"Error processing data: {e}")
    return sqlmodel_dicts

def handle_message(body: bytes):
    try:
        message = json.loads(body.decode('utf-8'))
        property_data = message['content']
        table_name = message['ad_type']

        model_dicts = df_to_sqlmodel_dicts(property_data, Property)
        insert_data_to_db(table_name, model_dicts)

    except (json.JSONDecodeError, KeyError) as e:
        print(f" [!] Failed to process message: {e}")

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
    channel = connection.channel()

    channel.queue_declare(queue='scraper-data', durable=True)

    def callback(ch, method, properties, body):
        handle_message(body)

    channel.basic_consume(queue='scraper-data', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)

if __name__ == '__main__':
    main()
