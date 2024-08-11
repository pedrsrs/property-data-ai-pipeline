import pika
import sys
import os
import json
import pandas as pd
from sqlmodel import Field, SQLModel
from sqlalchemy import Column, String
from sqlalchemy.dialects import postgresql
from typing import Optional, Set, Dict, Union, List
from datetime import datetime
from pydantic import field_validator, ValidationError
import re

class BaseSQLModel(SQLModel):
    def _init_(self, **kwargs):
        self._config_.table = False
        super()._init_(**kwargs)
        self._config_.table = True

    class Config:
        validate_assignment = True

class Property(BaseSQLModel, table=True):
    listing_id: str | None = Field(default=None, primary_key=True)
    title: str
    price: Optional[float] = None
    property_url: str
    listing_date: Optional[datetime] = None  
    municipality: Optional[str] = None
    neighbourhood: Optional[str] = None
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

    @field_validator('bedrooms', 'bathrooms', 'parking', mode='before')
    def parse_details(cls, value):
        if value is None or value == '':
            return None
        value_str = str(value).strip() 
        if '5 ou mais' in value_str:
            return 5
        try:
            return int(value_str)
        except (ValueError, TypeError):
            return None 

    @field_validator('listing_id', mode='before')
    def convert_listing_id_to_str(cls, value):
        if isinstance(value, (int, float)):
            return str(int(value))  
        return value

    @field_validator('condominium_details', 'property_details', 'type', mode='before')
    def process_details(cls, value: Union[str, List[str], Set[str]]):
        if isinstance(value, str):
            return set(value.split(", ")) if ", " in value else {value}    
        elif isinstance(value, (list, set)):
            return set(value)
        return None

    @field_validator('listing_date', mode='before')
    def convert_epoch_to_datetime(cls, value):
        if isinstance(value, int):
            return datetime.fromtimestamp(value)
        return value

    @field_validator('price', 'condominium_price', 'iptu', 'area', mode='before')
    def convert_string_to_float(cls, value):
        if isinstance(value, str):
            value = re.sub(r'[^\d=]', '', value)
            return float(value)
        return value

def rename_columns(data):
    new_column_names = {
        'title': 'title',
        'price': 'price',
        'listId': 'listing_id',
        'url': 'property_url',
        'date': 'listing_date',
        'locationDetails.municipality': 'municipality',
        'locationDetails.neighbourhood': 'neighbourhood',
        'locationDetails.uf': 'state',
        'área construída': 'area',
        'vagas na garagem': 'parking',
        'quartos': 'bedrooms',
        'banheiros': 'bathrooms',
        'condomínio': 'condominium_price',
        'categoria': 'category',
        'tipo': 'type',
        'detalhes do condomínio': 'condominium_details',
        'detalhes do imóvel': 'property_details'
    }
    data.rename(columns=new_column_names, inplace=True)
    return data

def expand_properties(row):
    property_dict = {}
    properties = row.get('properties', [])
    
    if isinstance(properties, list) and properties:
        for item in properties:
            label = item['label'].lower()
            value = item['value']
            property_dict[label] = value

    return pd.Series(property_dict)

def extract_fields(unfiltered_json):
    data = pd.json_normalize(unfiltered_json)
    property_stats = data.apply(expand_properties, axis=1)
    data = pd.concat([data, property_stats], axis=1)
    
    additional_columns = [
        'title', 'price', 'listId', 'url', 'date', 
        'locationDetails.municipality', 
        'locationDetails.neighbourhood', 
        'locationDetails.uf'
    ]

    unparsed_data = data[additional_columns + property_stats.columns.tolist()]
    return unparsed_data

def prepare_dataset(raw_data):
    unparsed_data = extract_fields(raw_data)
    unparsed_data = rename_columns(unparsed_data)
    print(unparsed_data.head())

    model_dicts = df_to_sqlmodel_dicts(unparsed_data, Property)

    output_df = pd.DataFrame(model_dicts)
    output_df.to_csv('output.csv', index=False)

def df_to_sqlmodel_dicts(df: pd.DataFrame, model_class: SQLModel) -> List[Dict]:
    sqlmodel_dicts = []
    for index, row in df.iterrows():
        try:
            sqlmodel_instance = model_class(**row.to_dict())
            sqlmodel_dict = sqlmodel_instance.model_dump()
            sqlmodel_dicts.append(sqlmodel_dict)
        except ValidationError as e:
            print(f"Skipping row {index} due to validation error: {e}")

    return sqlmodel_dicts

def callback(ch, method, properties, body):
    body_str = body.decode('utf-8')
    print(body_str)

    try:
        raw_data = json.loads(body_str)
        prepare_dataset(raw_data['props']['pageProps']['ads'])
        
    except json.JSONDecodeError as e:
        print(f" [!] Failed to decode JSON: {e}")
        
def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='scraped-data')

    channel.basic_consume(queue='scraped-data', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
