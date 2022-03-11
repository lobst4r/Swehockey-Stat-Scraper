# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import logging
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Identity, Compose
from itemloaders.utils import arg_to_iter
from itemadapter import ItemAdapter
from w3lib.html import remove_tags
from dataclasses import dataclass, field
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


def process_score(score):
    # Convert score to a 2D list of ints. Eg. [[1,0], [0,1], [3,0]]
    return [[goals.strip().split("-")] for goals in score.split(",")]


def process_pp_perc(text):
    return text.replace(",", ".").replace("%", "")


def remove_parens(text):
    return re.sub("[()]", "", text)


def parse_player(name=""):
    # Convert a string containing a player name
    # and (optionally) player number into a list
    # with the format [NUM(optional), LAST_NAME, FIRST_NAME]
    if not name:
        return []
    player = name.replace(".", ",").split(",")
    # if len(player) == 3:
    #     player[0] = int(player[0])
    return [player]


def return_empty(value):
    return value or [""]


class BasicStatsItem(scrapy.Item):
    # define the fields for your item here like:
    swehockey_id = scrapy.Field()
    line_up_url = scrapy.Field()
    event_url = scrapy.Field()
    home_name_abbrev = scrapy.Field()
    away_name_abbrev = scrapy.Field()
    date_time = scrapy.Field(
        input_processor=MapCompose(clean, str.split),
        output_processor=Identity(),
    )
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
        output_processor=Identity(),
    )
    shots_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=Identity(),
    )
    saves_by_period_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=Identity(),
    )
    saves_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=Identity(),
    )
    pim_by_period_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=Identity(),
    )
    pim_by_period_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_by_period, clean),
        output_processor=Identity(),
    )
    pp_time_team_1 = scrapy.Field(
        input_processor=MapCompose(remove_parens, clean)
    )
    pp_time_team_2 = scrapy.Field(
        input_processor=MapCompose(remove_parens, clean)
    )
    pp_perc_team_1 = scrapy.Field(
        input_processor=MapCompose(process_pp_perc, clean)
    )

    pp_perc_team_2 = scrapy.Field(
        input_processor=MapCompose(process_pp_perc, clean)
    )
    score = scrapy.Field(
        input_processor=MapCompose(remove_parens, process_score, clean),
        output_processor=Identity(),
    )
    score_team_1 = scrapy.Field()
    score_team_2 = scrapy.Field()
    spectators = scrapy.Field()

    # Events
    goalies_teams = scrapy.Field(
        input_processor=MapCompose(clean), output_processor=Identity()
    )
    goalies_names = scrapy.Field(
        input_processor=MapCompose(clean, parse_player),
        output_processor=Identity(),
    )
    goalies_saves = scrapy.Field(
        input_processor=MapCompose(remove_parens, clean),
        output_processor=Identity(),
    )

    game_events = scrapy.Field(output_processor=Identity())


class EventItem(scrapy.Item):
    time = scrapy.Field()
    event = scrapy.Field()
    team = scrapy.Field()
    player = scrapy.Field(
        input_processor=MapCompose(clean, parse_player),
    )
    assist_1 = scrapy.Field(
        input_processor=MapCompose(clean, parse_player),
    )
    assist_2 = scrapy.Field(
        input_processor=MapCompose(clean, parse_player),
    )

    details_1 = scrapy.Field()
    details_2 = scrapy.Field()

class EventItemLoader(ItemLoader):

    default_input_processor = MapCompose(clean)
    default_output_processor = TakeFirst()

    def _add_value(self, field_name, value):
        value = arg_to_iter(value)
        processed_value = self._process_input_value(field_name, value)
        self._values.setdefault(field_name, [])
        self._values[field_name] += arg_to_iter(processed_value)

    def load_item(self):
        adapter = ItemAdapter(self.item)
        for field_name in tuple(self._values):
            value = self.get_output_value(field_name)
            adapter[field_name] = value or ""
        return adapter.item



