import scrapy
import re

START_DATE = "2022-03-02"
END_DATE = "2022-03-08"


class StatsSpider(scrapy.Spider):
    name = "stats"
    allowed_domains = ["stats.swehockey.se"]
    start_urls = ["https://stats.swehockey.se/GamesByDate/2022-03-02"]

    def parse(self, response):

        # Retrieve URL to each game a page
        for game in response.xpath(
            "//table[@class='tblContent']/tr/td/a[starts-with(@href, 'java')]"
        ):
            game_link = game.xpath(".//@href").get()

            # Extract only the URL from the href
            url = game_link.split("'")[1]
            swehockey_id = url.split("/")[3]
            line_up_url = f"/Game/LineUps/{swehockey_id}"

            # Open Game Event link
            yield response.follow(
                url=url,
                callback=self.parse_stats_summary,
                cb_kwargs={"swehockey_id": swehockey_id},
            )

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

    def parse_stats_summary(self, response, swehockey_id):
        line_up_url = f"/Game/LineUps/{swehockey_id}"
        game_info = response.xpath("//table[@class='tblContent'][1]")

        teams = game_info.xpath("//tr/th/h2/text()").get()
        teams = clean_list([clean(team) for team in teams.split("-")])

        date_time = clean(game_info.xpath("//tr[2]/td[1]/h3/text()").get())
        league = clean(game_info.xpath("//tr[2]/td[2]/h3/text()").get())
        arena = clean(game_info.xpath("//tr[2]/td[3]/h3/b/text()").get())

        shots_team_1 = self.get_stats_by_period(game_info, 3, 3)
        shots_team_2 = self.get_stats_by_period(game_info, 3, 7)
        saves_team_1 = self.get_stats_by_period(game_info, 5, 3)
        saves_team_2 = self.get_stats_by_period(game_info, 5, 6)
        pim_team_1 = self.get_stats_by_period(game_info, 7, 3)
        pim_team_2 = self.get_stats_by_period(game_info, 7, 7)
        pp_time_team_1 = self.get_stats_by_period(game_info, 8, 3, False)
        pp_time_team_2 = self.get_stats_by_period(game_info, 8, 6, False)
        pp_perc_team_1 = (
            game_info.xpath("//tr[8]/td[2]/strong/text()").get().strip()
        )
        try:
            pp_perc_team_1 = float(
                pp_perc_team_1.replace(",", ".").replace("%", "")
            )
        except:
            pp_perc_team_1 = None
        pp_perc_team_2 = (
            game_info.xpath("//tr[8]/td[5]/strong/text()").get().strip()
        )
        try:
            pp_perc_team_2 = float(
                pp_perc_team_2.replace(",", ".").replace("%", "")
            )
        except:
            pp_perc_team_2 = None

        spectators = clean(
            game_info.xpath("//td[@class='tdInfoArea']/div[4]/text()").get()
        )
        if spectators:
            spectators = int(clean(spectators.split(":")[1]))

        # Get score by period
        score = (
            game_info.xpath("//td[@class='tdInfoArea']/div[2]/text()")
            .get()
            .strip()
        )
        # Convert score to a 2D list of ints. Eg. [[1,0], [0,1], [3,0]]
        score = [
            [int(goal) for goal in goals.strip().split("-")]
            for goals in re.sub("[()]", "", score).split(",")
        ]
        # Separate score by team
        score_team_1 = [goals[0] for goals in score]
        score_team_2 = [goals[1] for goals in score]
        game = {}
        game["swehockey_id"] = swehockey_id
        game["stats"] = {
            "team_1": teams[0],
            "team_2": teams[1],
            "date_time": date_time,
            "league": league,
            "arena": arena,
            "shots_team_1": shots_team_1,
            "shots_team_2": shots_team_2,
            "shots_sum_team_1": sum(shots_team_1),
            "shots_sum_team_2": sum(shots_team_2),
            "saves_team_1": saves_team_1,
            "saves_team_2": saves_team_2,
            "saves_sum_team_1": sum(saves_team_1),
            "saves_sum_team_2": sum(saves_team_2),
            "pim_team_1": pim_team_1,
            "pim_team_2": pim_team_2,
            "pim_sum_team_1": sum(pim_team_1),
            "pim_sum_team_2": sum(pim_team_2),
            "pp_time_team_1": pp_time_team_1,
            "pp_time_team_2": pp_time_team_2,
            "pp_perc_team_1": pp_perc_team_1,
            "pp_perc_team_2": pp_perc_team_2,
            "score": score,
            "score_team_1": score_team_1,
            "score_team_2": score_team_2,
            "spectators": spectators,
        }
        game["events"] = self.parse_game_actions(response, swehockey_id)
        # Open Game Event link
        yield response.follow(
            url=line_up_url,
            callback=self.parse_line_up,
            cb_kwargs={"swehockey_id": swehockey_id, "game": game},
        )

    def parse_game_actions(self, response, swehockey_id):
        goalies_stats = response.xpath("(//table[@class='tblContent'])[2]")

        goalies_teams = self.get_goalies(goalies_stats, 3)
        goalies = [
            self.parse_player(goalie)
            for goalie in self.get_goalies(goalies_stats, 4)
        ]
        goalies_saves = self.get_goalies(goalies_stats, 5)

        # Find the last period of the game (including overtime) and extract data from each period
        actions = response.xpath(
            "((//table[@class='tblContent'])[2]//tr/th/h3[contains(text(), 'Overtime') or contains(text(), 'overtime') or contains(text(), '3rd period')])[1]/ancestor::node()/following-sibling::tr[not(.//th)]"
        )
        game_events = []
        for action in actions:
            time = (action.xpath(".//td[1]/text()").get() or "").strip()
            event = (action.xpath(".//td[2]/text()").get() or "").strip()
            team = (action.xpath(".//td[3]/text()").get() or "").strip()
            player = self.parse_player(action.xpath(".//td[4]/text()").get())
            # If the player variable is a list of length 1 or less,
            # it's not a player name, and therefore we convert it to
            # a string instead, as it indicates a team event (or
            # potentially something else).
            if len(player) == 1:
                player = player[0]
            elif len(player) == 0:
                player = ""

            assist_1 = self.parse_player(
                action.xpath(".//td[4]/span[2]/div/text()").get()
            )
            assist_2 = self.parse_player(
                action.xpath(".//td[4]/span[3]/div/text()").get()
            )
            details_1 = clean(
                action.xpath(".//td[5]/descendant-or-self::text()").get()
            )
            details_2 = clean(
                action.xpath(".//td[5]/descendant-or-self::text()[2]").get()
            )
            details = self.parse_event_detail(
                details_1, details_2, player, event
            )
            game_events.append(
                [
                    time,
                    event,
                    team,
                    player,
                    assist_1,
                    assist_2,
                    details_1,
                    details_2,
                    details,
                ]
            )

        # Get shootout data (if any)
        # TODO: Clean shootout data
        shootout_actions = response.xpath(
            "((//table[@class='tblContent'])[2]/tr/th/h3[contains(text(), 'Game Winning Shots')])[1]/ancestor::tr[1]/following-sibling::tr/th/ancestor::tr[1]/preceding-sibling::tr/td[contains(text(), 'Missed') or contains(text(), 'Scored')]/ancestor::tr[1]"
        )
        shootout_events = []
        for action in shootout_actions:
            scored = clean(action.xpath(".//td[1]/text()").get())
            score = clean(action.xpath(".//td[2]/text()").get())
            team = clean(action.xpath(".//td[3]/text()").get())
            player = clean(action.xpath(".//td[4]/div[1]/text()").get())
            goalie = clean(action.xpath(".//td[4]/div[2]/text()").get())
            shootout_events.append([scored, score, team, player, goalie])

        return {
            "swehockey_id": swehockey_id,
            "goalies_teams": goalies_teams,
            "goalies": goalies,
            "goalies_saves": goalies_saves,
            "game_events": game_events,
            "shootout_events": shootout_events,
        }

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

    def get_stats_by_period(self, game_info, tr, td, int_list=True):
        # Parse a common pattern describing basic stats by
        # period.
        # Convert to list of ints if int_list is true (default).
        stat = game_info.xpath(f"//tr[{tr}]/td[{td}]/text()").get().strip()
        stat = re.sub("[()]", "", stat)
        if int_list is True:
            stat = stat.split(":")
            return [int(x) for x in stat if (not x.startswith("-"))]
        return stat

    def get_goalies(self, goalies_stats, td):
        goalies = goalies_stats.xpath(
            f"((//table[@class='tblContent'])[2]/tr/th/h3)[2]/ancestor::tr[1]/preceding-sibling::tr/td[{td}]"
        )
        return [clean(team.xpath(".//text()").get()) for team in goalies]

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
                    ]
                    details["on_ice_minus"] = [
                        int(x)
                        for x in clean_list(details_2.split(":")[1].split(","))
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
