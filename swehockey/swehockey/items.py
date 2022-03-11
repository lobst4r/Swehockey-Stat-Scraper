# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

import re


def clean(text=""):
    # Remove all extra whitespace in a string.
    # Return an empty string if no string
    # is passed as an argument.

    # No need to do anything if it's not a string.
    if not isinstance(text, str):
        return text
    return " ".join((text or "").split())


def process_by_period(text):
    # Parse a common pattern describing basic stats by
    # period.
    # Convert to list of ints if int_list is true (default).
    stats = text.split(":")
    return [x for x in stats if (not x.startswith("-"))]


def remove_parens(text):
    return re.sub("[()]", "", text)


def override_default(value):
    return value


class SwehockeyItem(scrapy.Item):
    # define the fields for your item here like:
    swehockey_id = scrapy.Field()
    line_up_url = scrapy.Field()
    event_url = scrapy.Field()
    home_name_abbrev = scrapy.Field()
    away_name_abbrev = scrapy.Field()
    date_time = scrapy.Field()
    league = scrapy.Field()
    arena = scrapy.Field()
    shots_total_team_1 = scrapy.Field()
    shots_total_team_2 = scrapy.Field()
    saves_total_team_1 = scrapy.Field()
    saves_total_team_2 = scrapy.Field()
    pim_total_team_1 = scrapy.Field()
    pim_total_team_2 = scrapy.Field()
    shots_by_period_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=override_default(),
    )
    shots_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean)
    )
    saves_by_period_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean)
    )
    saves_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean)
    )
    pim_by_period_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean)
    )
    pim_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean)
    )
    pp_time_team_1 = scrapy.Field()
    pp_time_team_2 = scrapy.Field()
    pp_perc_team_1 = scrapy.Field()
    pp_perc_team_2 = scrapy.Field()
    score = scrapy.Field()
    score_team_1 = scrapy.Field()
    score_team_2 = scrapy.Field()
    spectators = scrapy.Field()
