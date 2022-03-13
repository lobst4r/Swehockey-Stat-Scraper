import scrapy
from swehockey.items import (
    BasicStatsItem,
    EventItem,
    ShootoutItem,
    LineupItem,
    LineItem,
    EventItemLoader,
    clean,
    clean_list
)
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
import re
import logging


START_DATE = "2022-03-01"
END_DATE = "2022-03-07"


class StatsSpider(scrapy.Spider):
    name = "stats"
    allowed_domains = ["stats.swehockey.se"]
    start_urls = [f"https://stats.swehockey.se/GamesByDate/{START_DATE}"]

    def parse(self, response):

        # Retrieve URL to each game a page
        for game in response.xpath(
            "//table[@class='tblContent']/tr/td/a[starts-with(@href, 'java')]"
        ):
            # Create Loader object
            l = EventItemLoader(item=BasicStatsItem(), selector=game)
            l.default_output_processor = TakeFirst()

            # Extract URLs and game ID.
            game_link = game.xpath(".//@href").get()
            event_url = game_link.split("'")[1]
            swehockey_id = event_url.split("/")[3]
            line_up_url = f"/Game/LineUps/{swehockey_id}"

            l.add_value("swehockey_id", swehockey_id)
            l.add_value("event_url", response.urljoin(event_url))
            l.add_value("line_up_url", response.urljoin(line_up_url))

            # Open Game Event link
            yield response.follow(
                url=event_url,
                callback=self.parse_stats_summary,
                cb_kwargs={
                    "swehockey_id": swehockey_id,
                    "line_up_url": line_up_url,
                    "item": l.load_item(),
                },
            )

        # Find and navigate to next page of games (i.e. next day's games).
        next_page_url = response.xpath(
            "//div[@class='form-group btn-group']/a[2]/@href"
        ).get()

        next_page_date = int(
            clean(
                response.xpath(
                    "//div[@class='form-group btn-group']/a[2]/text()"
                )
                .get()
                .replace(">>", "")
                .replace("-", "")
            )
        )
        if next_page_date < int(END_DATE.replace("-", "")):
            yield response.follow(url=next_page_url, callback=self.parse)

    def parse_stats_summary(self, response, swehockey_id, line_up_url, item):
        game_info = response.xpath("(//table[@class='tblContent'])[1]")
        l = EventItemLoader(item=item, selector=game_info)
        l.default_output_processor = TakeFirst()
        l.default_input_processor = MapCompose(clean)

        # Check if game has ended, in order to find abnormalities
        # and avoid errors.
        game_ended_strings = [
            "Final Score",
            "Game Finished",
            "Game Winning Shots ended",
        ]
        game_status = clean(
            response.xpath("//td[@class='tdInfoArea']/div[3]/text()").get()
        )

        if not any(txt in game_status for txt in game_ended_strings):
            logging.warning(
                f"Game not finished or invalid. URL: https://stats.swehockey.se/Game/Events/{swehockey_id}\nStatus: '{game_status}'"
            )
            return

        # Parse basic game stats
        title = clean(response.xpath("//title/text()").get())
        team_names_abbrev = clean_list(title.split("-", 1))
        l.add_value("home_name_abbrev", clean_list(team_names_abbrev[0].split("(",1))[0])
        l.add_value("away_name_abbrev", clean_list(team_names_abbrev[1].split("(",1))[0])
        teams = game_info.xpath("//tr/th/h2/text()").get()
        teams = clean_list([clean(team) for team in teams.split("-")])
        l.add_value("home_name", teams[0])
        l.add_value("away_name", teams[1])
        l.add_xpath("date_time", ".//tr[2]/td[1]/h3/text()")
        l.add_xpath("league", ".//tr[2]/td[2]/h3/text()")
        l.add_xpath("arena", ".//tr[2]/td[3]/h3/b/text()")
        l.add_xpath("shots_total_team_1", self.stats_total_xpath(3, 2))
        l.add_xpath("shots_total_team_2", self.stats_total_xpath(3, 6))
        l.add_xpath("saves_total_team_1", self.stats_total_xpath(5, 2))
        l.add_xpath("saves_total_team_2", self.stats_total_xpath(5, 5))
        l.add_xpath("pim_total_team_1", self.stats_total_xpath(7, 2))
        l.add_xpath("pim_total_team_2", self.stats_total_xpath(7, 6))
        l.add_xpath("shots_by_period_team_1", self.stats_by_period_xpath(3, 3))
        l.add_xpath("shots_by_period_team_2", self.stats_by_period_xpath(3, 7))
        l.add_xpath("saves_by_period_team_1", self.stats_by_period_xpath(5, 3))
        l.add_xpath("saves_by_period_team_2", self.stats_by_period_xpath(5, 6))
        l.add_xpath("pim_by_period_team_1", self.stats_by_period_xpath(7, 3))
        l.add_xpath("pim_by_period_team_2", self.stats_by_period_xpath(7, 7))
        l.add_xpath("pp_time_team_1", self.stats_by_period_xpath(8, 3))
        l.add_xpath("pp_time_team_2", self.stats_by_period_xpath(8, 6))
        l.add_xpath("pp_perc_team_1", ".//tr[8]/td[2]/strong/text()")
        l.add_xpath("pp_perc_team_2", ".//tr[8]/td[5]/strong/text()")
        l.add_xpath("spectators", ".//td[@class='tdInfoArea']/div[4]/text()")
        l.add_xpath("score", "//td[@class='tdInfoArea']/div[1]/text()")
        l.add_xpath("score_by_period", "//td[@class='tdInfoArea']/div[2]/text()")

        self.parse_game_actions(response, swehockey_id, l.load_item())

        # Open Game Event link
        yield response.follow(
            url=line_up_url,
            callback=self.parse_line_up,
            cb_kwargs={"swehockey_id": swehockey_id, "item": l.load_item()},
        )

    def parse_game_actions(self, response, swehockey_id, item):
        events = response.xpath("(//table[@class='tblContent'])[2]")
        l = EventItemLoader(item=item, selector=events)
        l.add_xpath("goalies_teams", self.get_goalies_xpath(3))
        l.add_xpath("goalies_names", self.get_goalies_xpath(4))
        l.add_xpath("goalies_saves", self.get_goalies_xpath(5))

        # Find the last period of the game (including overtime) and extract data from each period
        actions = events.xpath(
            "(.//tr/th/h3[contains(text(), 'Overtime') or contains(text(), 'overtime') or contains(text(), '3rd period')])[1]/ancestor::node()/following-sibling::tr[not(.//th)]"
        )
        if not actions:
            logging.warning(f"No game events found. Possible irregularity in html. URL: https://stats.swehockey.se/Game/Events/{swehockey_id}")
        for action in actions:
            el = EventItemLoader(item=EventItem(), selector=action)
            el.add_xpath("time", ".//td[1]/text()")
            el.add_xpath("event", ".//td[2]/text()")
            el.add_xpath("team", ".//td[3]/text()")
            el.add_xpath("player", ".//td[4]/text()")
            el.add_xpath("assist_1", ".//td[4]/descendant-or-self::div[1]/text()")
            el.add_xpath("assist_2", ".//td[4]/descendant-or-self::div[2]/text()")
            el.add_xpath("details_1", ".//td[5]/descendant-or-self::text()[1]")
            el.add_xpath("details_2", ".//td[5]/descendant-or-self::text()[2]")
            l.add_value("game_events", el.load_item())

        # Get shootout data (if any)
        shootout_actions = response.xpath(
            "((//table[@class='tblContent'])[2]/tr/th/h3[contains(text(), 'Game Winning Shots')])[1]/ancestor::tr[1]/following-sibling::tr/th/ancestor::tr[1]/preceding-sibling::tr/td[contains(text(), 'Missed') or contains(text(), 'Scored')]/ancestor::tr[1]"
        )
        if shootout_actions:
            for action in shootout_actions:
                sl = EventItemLoader(item=ShootoutItem(), selector=action)
                sl.add_xpath("scored", ".//td[1]/text()")
                sl.add_xpath("score", ".//td[2]/text()")
                sl.add_xpath("team", ".//td[3]/text()")
                sl.add_xpath("player", ".//td[4]//div[1]/text()")
                sl.add_xpath("goalie", ".//td[4]//div[2]/text()")
                l.add_value("shootout_events", sl.load_item())
        else:
            l.add_value("shootout_events", "") # Create an empty field even if there is no shootout
        return l.load_item()

    def parse_line_up(self, response, swehockey_id, item):
        # Parse the line up page to get data on
        # participating refs, coaches, and players.

        # The names of the referees do not have a separator
        # that separates the first name(s) from the last name(s).
        # As such, it is impossible to programatically distinguish
        # the first and last names without some sort of cross reference,
        # since some have multiple last names and some have multiple
        # first names.
        lineup_selector = response.xpath("(//table[@class='tblContent'])[2]")
        l = EventItemLoader(item=item)
        ll = EventItemLoader(item=LineupItem(), selector=lineup_selector)
        ll.add_xpath('refs', "(.//table[@class='tblContent'])[1]//tr[1]/td[2]/text()")
        ll.add_xpath('linesmen', "(.//table[@class='tblContent'])[1]//tr[2]/td[2]/text()")
        ll.add_xpath('home_team_coaches', "(.//table[@class='tblContent'])[2]//tr[3]/td[2]/table/tr/td/text()")
        ll.add_xpath('away_team_coaches', "(.//table[@class='tblContent'])[2]//tr[3]/following-sibling::tr[last()]//table/tr/td/text()")

        self.get_lines(lineup_selector, "((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/preceding-sibling::tr/descendant::*[contains(@style, 'text-align')]/ancestor::tr[1]", ll.load_item(), 'lineup_home', swehockey_id)
        self.get_lines(lineup_selector, "((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/following-sibling::tr/td[contains(@style, 'text-align')]/ancestor::tr[1]", ll.load_item(), 'lineup_away', swehockey_id)

        l.add_value('lineup', ll.load_item())
        yield l.load_item()


    def stats_total_xpath(self, tr, td):
        return f".//tr[{tr}]/td[{td}]/strong/text()"

    def stats_by_period_xpath(self, tr, td):
        return f".//tr[{tr}]/td[{td}]/text()"

    def get_goalies_xpath(self, td):
        return f"(.//tr/th/h3)[2]/ancestor::tr[1]/preceding-sibling::tr/td[{td}]/text()"
        # return [clean(team.xpath(".//text()").get()) for team in goalies]

    def get_lines(self, response, line_up_raw, item, line_name, swehockey_id):
        # Parse the line up for one team.
        line_up_selector = response.xpath(line_up_raw)
        ll = EventItemLoader(item=item, selector=line_up_selector)
        if not line_up_selector:
            logging.warning(f"No Line Up found. Possible HTML irregularity. URL: https://stats.swehockey.se/Game/Events/{swehockey_id}")
        for line in line_up_selector:
            line_loader = EventItemLoader(item=LineItem(), selector=line)

            line_loader.add_xpath('line_name', ".//*/strong/text()")
            line_loader.add_xpath('players', ".//td/div/text()")

            ll.add_value(line_name, line_loader.load_item())
        # NOTE: Starting players are not always indicated except for
        # goaltenders.
        ll.add_xpath(f'starting_players_{line_name}', ".//*[contains(@class, 'red')]/text()")
        
        return ll.load_item()


