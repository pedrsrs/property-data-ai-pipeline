# property-data-ai-pipeline
A data pipeline for scraping and analyzing real estate data, using Docker, Scrapy, PostgreSQL, Redis, RabbitMQ, Flask, Apache Airflow and Scikit-learn.

## Pipeline Planning
![IMG-20240810-WA0062](https://github.com/user-attachments/assets/bf575e7c-d735-4ca0-b7bb-74a9dab03967)

## Stack Choices
#### Scraping
Currently using Scrapy + CloudScraper library to bypass CloudFlare blocks. This choice is subject to change according to CloudFlare updates, and it's replacing and old solution that used Playwright but was less performatic.

#### Queue Service
Previously used Apache Kafka, but changed it to RabbitMQ because it uses less RAM memory and the logs/scraped data queue doesn't need to function in real time. RabbitMQ get's the job done fine.

#### ORM
Currently using SQLModel, which combines Pydantic and SQLAlchemy to both treat the data and insert it on PostgreSQL. Having the ORM work on a separate container from the scraper and read data from the RabbitMQ queue works nicely with the scraping multithreading system.

#### API
Flask was chosen due to it's easiness of implementation of query chaching for the ML model inferences.

#### Orquestration
Apache Airflow is being used to schedule re-scraping of the real-estate websites and re-training of the AI model.

