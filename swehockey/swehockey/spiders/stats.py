import scrapy
from swehockey.items import BasicStatsItem, EventItem, EventItemLoader
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
import re
import logging


START_DATE = "2022-03-06"
END_DATE = "2022-03-06"


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
            l = ItemLoader(item=BasicStatsItem(), selector=game)
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

    def parse_stats_summary(self, response, swehockey_id, item):
        game_info = response.xpath("(//table[@class='tblContent'])[1]")
        l = ItemLoader(item=item, selector=game_info)
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
        team_names_abbrev = clean_list((title.split("(", 1)[0]).split("-"))
        l.add_value("home_name_abbrev", team_names_abbrev[0])
        l.add_value("away_name_abbrev", team_names_abbrev[1])
        teams = game_info.xpath("//tr/th/h2/text()").get()
        teams = clean_list([clean(team) for team in teams.split("-")])
        l.add_xpath("date_time", ".//tr[2]/td[1]/h3/text()")
        l.add_xpath("league", ".//tr[2]/td[2]/h3/text()")
        l.add_xpath("arena", ".//tr[2]/td[3]/h3/b/text()")
        try:
            l.add_xpath("shots_total_team_1", self.stats_total_xpath(3, 2))
            l.add_xpath("shots_total_team_2", self.stats_total_xpath(3, 6))
            l.add_xpath("saves_total_team_1", self.stats_total_xpath(5, 2))
            l.add_xpath("saves_total_team_2", self.stats_total_xpath(5, 5))
            l.add_xpath("pim_total_team_1", self.stats_total_xpath(7, 2))
            l.add_xpath("pim_total_team_2", self.stats_total_xpath(7, 6))
        except:
            logging.warning(f"NONE TYPE. URL: {l.get_value('event_url')}")
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
        l.add_xpath("score", "//td[@class='tdInfoArea']/div[2]/text()")

        spectators = clean(
            game_info.xpath("//td[@class='tdInfoArea']/div[4]/text()").get()
        )
        if spectators:
            spectators = int(clean(spectators.split(":")[1]))

        return self.parse_game_actions(response, swehockey_id, l.load_item())

        # yield l.load_item()
        # Open Game Event link
        # yield response.follow(
        #     url=line_up_url,
        #     callback=self.parse_line_up,
        #     cb_kwargs={"swehockey_id": swehockey_id, "game": game},
        # )

    def parse_game_actions(self, response, swehockey_id, item):
        events = response.xpath("(//table[@class='tblContent'])[2]")
        l = ItemLoader(item=item, selector=events)
        l.add_xpath("goalies_teams", self.get_goalies(3))
        l.add_xpath("goalies_names", self.get_goalies(4))
        l.add_xpath("goalies_saves", self.get_goalies(5))

        # Find the last period of the game (including overtime) and extract data from each period
        actions = events.xpath(
            "(.//tr/th/h3[contains(text(), 'Overtime') or contains(text(), 'overtime') or contains(text(), '3rd period')])[1]/ancestor::node()/following-sibling::tr[not(.//th)]"
        )
        # game_events = []
        for action in actions:
            el = EventItemLoader(item=EventItem(), selector=action)
            el.add_xpath("time", ".//td[1]/text()")
            el.add_xpath("event", ".//td[2]/text()")
            el.add_xpath("team", ".//td[3]/text()")
            el.add_xpath("player", ".//td[4]/text()")
            el.add_xpath("assist_1", ".//td[4]/span[2]/div/text()")
            el.add_xpath("assist_2", ".//td[4]/span[3]/div/text()")
            el.add_xpath("details_1", ".//td[5]/descendant-or-self::text()[1]")
            el.add_xpath("details_1", ".//td[5]/descendant-or-self::text()[2]")
            # time = (action.xpath(".//td[1]/text()").get() or "").strip()
            # event = (action.xpath(".//td[2]/text()").get() or "").strip()
            # team = (action.xpath(".//td[3]/text()").get() or "").strip()
            # player = self.parse_player(action.xpath(".//td[4]/text()").get())

            # If the player variable is a list of length 1 or less,
            # it's not a player name, and therefore we convert it to
            # a string instead, as it indicates a team event (or
            # potentially something else).
            # if len(player) == 1:
            #     player = player[0]
            # elif len(player) == 0:
            #     player = ""
            # assist_1 = self.parse_player(
            # action.xpath(".//td[4]/span[2]/div/text()").get()
            # )
            # assist_2 = self.parse_player(
            #     action.xpath(".//td[4]/span[3]/div/text()").get()
            # )
            # details_2 = clean(
            #     action.xpath(".//td[5]/descendant-or-self::text()[2]").get()
            # )
            # details = self.parse_event_detail(
            #     details_1, details_2, player, event
            # )
            l.add_value("game_events", el.load_item())
        return l.load_item()
        #     game_events.append(
        #         [
        #             time,
        #             event,
        #             team,
        #             player,
        #             assist_1,
        #             assist_2,
        #             details_1,
        #             details_2,
        #             details,
        #         ]
        #     )k

        # # Get shootout data (if any)
        # # TODO: Clean shootout data
        # shootout_actions = response.xpath(
        #     "((//table[@class='tblContent'])[2]/tr/th/h3[contains(text(), 'Game Winning Shots')])[1]/ancestor::tr[1]/following-sibling::tr/th/ancestor::tr[1]/preceding-sibling::tr/td[contains(text(), 'Missed') or contains(text(), 'Scored')]/ancestor::tr[1]"
        # )
        # shootout_events = []
        # for action in shootout_actions:
        #     scored = clean(action.xpath(".//td[1]/text()").get())
        #     score = clean(action.xpath(".//td[2]/text()").get())
        #     team = clean(action.xpath(".//td[3]/text()").get())
        #     player = clean(action.xpath(".//td[4]/div[1]/text()").get())
        #     goalie = clean(action.xpath(".//td[4]/div[2]/text()").get())
        #     shootout_events.append([scored, score, team, player, goalie])

        # return {
        #     "swehockey_id": swehockey_id,
        #     "goalies_teams": goalies_teams,
        #     "goalies": goalies,
        #     "goalies_saves": goalies_saves,
        #     "game_events": game_events,
        #     "shootout_events": shootout_events,
        # }

    def parse_line_up(self, response, swehockey_id, game):
        # Parse the line up page to get data on
        # participating refs, coaches, and players.

        # The names of the referees do not have a separator
        # that separates the first name(s) from the last name(s).
        # As such, it is impossible to programatically distinguish
        # the first and last names without some sort of cross reference,
        # since some have multiple last names and some have multiple
        # first names.
        refs = response.xpath(
            "(//table[@class='tblContent'])[2]//tr[1]/td[2]/text()"
        ).get()
        refs = [clean(ref) for ref in split_string(refs, ",")]
        linesmen = response.xpath(
            "(//table[@class='tblContent'])[2]//tr[2]/td[2]/text()"
        ).get()
        linesmen = [
            clean(linesman) for linesman in split_string(linesmen, ",")
        ]
        home_team_coaches = response.xpath(
            "(//table[@class='tblContent'])[4]//tr[3]/td[2]/table/tr/td/text()"
        ).getall()
        home_team_coaches = [
            self.parse_player(coach) for coach in home_team_coaches
        ]
        away_team_coaches = response.xpath(
            "(//table[@class='tblContent'])[4]//tr[3]/following-sibling::tr[last()]//table/tr/td/text()"
        ).getall()
        away_team_coaches = [
            self.parse_player(coach) for coach in away_team_coaches
        ]
        line_up_home = self.get_line_up(
            response.xpath(
                "((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/preceding-sibling::tr/descendant::*[contains(@style, 'text-align')]/ancestor::tr[1]"
            )
        )
        line_up_away = self.get_line_up(
            response.xpath(
                "((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/following-sibling::tr/td[contains(@style, 'text-align')]/ancestor::tr[1]"
            )
        )

        game["line_up"] = {
            "swehockey_id": swehockey_id,
            "refs": refs,
            "linesmen": linesmen,
            "home_team_coaches": home_team_coaches,
            "away_team_coaches": away_team_coaches,
            "line_up_home": line_up_home,
            "line_up_away": line_up_away,
        }
        yield game

    def stats_total_xpath(self, tr, td):
        return f".//tr[{tr}]/td[{td}]/strong/text()"

    def stats_by_period_xpath(self, tr, td):
        return f".//tr[{tr}]/td[{td}]/text()"

    def get_goalies(self, td):
        return f"(.//tr/th/h3)[2]/ancestor::tr[1]/preceding-sibling::tr/td[{td}]/text()"
        # return [clean(team.xpath(".//text()").get()) for team in goalies]

    def get_line_up(self, line_up_raw):
        # Parse the line up for one team.
        line_up = {}
        prev_line_name = ""
        for line in line_up_raw:
            line_name = line.xpath(".//*/strong/text()").get()
            players = [
                clean(player)
                for player in line.xpath(".//td/div/text()").getall()
            ]
            players = [
                clean_list(player.replace(".", ",").split(","))
                for player in players
            ]
            if line_name:
                # If there is a line name ("1st line" for example)
                # in the current row, it means that we are
                # looking at the defensive players  for the home
                # team. Otherwise we are looking at the offensive
                # players, which is information we want to retain.
                prev_line_name = line_name
                line_up[line_name] = {}
                line_up[line_name]["players_row_1"] = players
            else:
                line_name = prev_line_name
                line_up[line_name]["players_row_2"] = players

        # NOTE: Starting players are not always indicated except for
        # goaltenders.
        starting_players = line_up_raw.xpath(
            ".//*[contains(@class, 'red')]/text()"
        ).getall()
        starting_players = [
            clean_list(player.replace(".", ",").split(","))
            for player in starting_players
        ]
        line_up["starting_players"] = starting_players

        return line_up

    def parse_player(self, name=""):
        # Convert a string containing a player name
        # and (optionally) player number into a list
        # with the format [NUM(optional), LAST_NAME, FIRST_NAME]
        if not name:
            return []
        player = clean_list(name.replace(".", ",").split(","))
        if len(player) == 3:
            player[0] = int(player[0])
        return player

    def parse_event_detail(self, details_1, details_2, player, event):
        # The details column in the game events table can
        # differ depending on the event. As such,
        # it needs to be parsed based on that specific event.

        # Penalty
        details = {}
        if details_2:
            if details_2[0] == "(":
                details["type"] = "penalty"
                details["penalty_type"] = details_1
                penalty_times = clean_list(
                    re.sub("[()]", "", details_2).split("-")
                )
                details["penalty_start_time"] = penalty_times[0]
                details["penalty_end_time"] = penalty_times[1]
        if details_1:
            # Players on the ice on goal
            if details_1.startswith("Pos"):
                # Penalty shot goals are handled below
                # and stored as type "penalty_shot"
                # with outcome "scored", instead of
                # type "goal".

                # TODO: refactor list comprehension for
                # players on ice, since it's used four times.
                if not event.endswith("(PS)"):
                    details["type"] = "goal"
                    details["on_ice_plus"] = [
                        int(x)
                        for x in clean_list(details_1.split(":")[1].split(","))
                        if x
                    ]
                    details["on_ice_minus"] = [
                        int(x)
                        for x in clean_list(details_2.split(":")[1].split(","))
                        if x
                    ]
            # Penalty Shot (the details and outcome of the penalty shot
            # is in another event)
            if details_1.startswith("PenaltyShot"):
                return "Penalty Shot"
            # Missed Penalty Shot
            if details_1.startswith("Missed"):
                details["type"] = "penalty_shot"
                details["outcome"] = "missed"
                details["player_number"] = player[0]
                details["goalie_number"] = clean_list(
                    self.parse_player(details_2.replace("Saved By ", ""))
                )[0]
            # Scored on Penalty Shot
            if event.endswith("(PS)"):
                details["type"] = "penalty_shot"
                details["outcome"] = "scored"
                details["player_number"] = [
                    int(x)
                    for x in clean_list(details_1.split(":")[1].split(","))
                ][0]
                details["goalie_number"] = [
                    int(x)
                    for x in clean_list(details_2.split(":")[1].split(","))
                ][0]

        return details


def clean(text=""):
    # Remove all extra whitespace in a string.
    # Return an empty string if no string
    # is passed as an argument.

    # No need to do anything if it's not a string.
    if not isinstance(text, str):
        return text
    return " ".join((text or "").split())


def clean_list(l=[]):
    # Clean a list of strings
    return [clean(item) for item in l]


def split_string(text, separator=None):
    # Split a string.
    # Return an empty string if
    # no string is passed as an argument.
    # This is used for when a function might
    # return a null value.
    return (text or "").split(separator)
