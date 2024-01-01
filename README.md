### NOTE: THIS PROJECT WAS MADE SOME TIME AGO TO FAMILIARIZE MYSELF WITH SCRAPY AND IS NEITHER OPTIMIZED, ELEGANT, NOR CURRENTLY MAINTAINED.

# Swehockey Stats Scraper with Scrapy

This Scrapy project is aimed at learning web scraping using Scrapy by collecting statistics from the swehockey.se website.

## Description

The purpose of this project is to extract statistics related to hockey from the Swehockey website. Swehockey hosts a vast amount of data on hockey leagues, teams, and players in Sweden. The scraper is designed to navigate the site, gather specific statistics, and store them for analysis or other purposes.

Due to rare and hard-to-spot inconsistencies in how the data is structured, there could be some edge-cases where the stats are not 100% correct. I've done my best to find and hammer out these errors, but again, they are hard to find.

It scrapes all games across all Swedish ice hockey leagues between the dates specified in the stats.py spider, and includes data from everything to penalties, shootouts and referees, which are stored in as structured sqlite3 database. 

## Project Structure

The project structure follows the standard layout of a Scrapy project:

### Spider

The main spider `stats.py` contains the scraping logic. It navigates through the website, parses HTML pages, and extracts desired statistics. 

### Pipelines

The project includes a pipeline `pipelines.py` responsible for processing and storing the scraped data. This pipeline involve saving the extracted statistics to a local file or a database.

### Items

The `items.py` file defines the data structure (`Scrapy.Item`) to store the extracted information. It outlines the fields and their types, providing a structured format for collected stats.

## Getting Started

### Installation

To run this project, make sure you have Poetry installed:

```bash
poetry shell
```

And make sure you have the dependencies installed in the virtual environment:

```bash
poetry install
```
### Settings

The spider is configured to scrape stats of games from the range of dates specified in the stats.py spider file. Thus, to change the dates between which you wish to scrape, change the dates in that file.

### Running the Spider

To execute the scraper and gather statistics from the Swehockey website, use the following command inside the swehockey dir:

```bash
scrapy crawl stats -o output_file_name.json
```

Note that the pipeline is configured to store the data in a sqlite3 database, which will be automatically generated in the directory where the $scrapy crawl command is run. Outputing to a .json file is thus optional (and not recommended). Instead, you can just run:

```bash
scrapy crawl stats
```

and query the database for the stats. See pipelines.py for schema.


