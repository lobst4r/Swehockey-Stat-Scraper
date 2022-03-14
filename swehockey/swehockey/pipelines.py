# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3
import logging


class SwehockeyPipeline:
    def __init__(self):
        self.con = sqlite3.connect("test5.db")
        self.cur = self.con.cursor()
        self.create_games_table()
        self.create_lines_table()
        self.create_refs_table()
        self.create_stats_by_period_table()
        self.create_goalie_stats_table()
        self.create_plus_minus_table()
        self.create_game_events_table()

    def create_goalie_stats_table(self):
        self.cur.execute(
            """CREATE TABLE goalie_stats(
        swehockey_id INT,
        team_name TEXT,
        first_name TEXT,
        last_name TEXT,
        player_number INT,
        saves TEXT,
        shots TEXT
        )"""
        )

    def create_stats_by_period_table(self):
        self.cur.execute(
            """CREATE TABLE stats_by_period(
        swehockey_id INT,
        team_name TEXT,
        stat_name TEXT,
        period INT,
        stat INT
        )"""
        )

    def create_refs_table(self):
        self.cur.execute(
            """CREATE TABLE refs(
        swehockey_id INT,
        ref_name TEXT,
        position TEXT
        )"""
        )

    def create_lines_table(self):
        self.cur.execute(
            """CREATE TABLE lines(
            swehockey_id INT,
            team TEXT,
            line_name TEXT,
            player_first_name,
            player_last_name,
            player_number,
            starting INT

)"""
        )

    def create_games_table(self):
        self.cur.execute(
            """CREATE TABLE games(
            swehockey_id INT PRIMARY KEY,
            arena TEXT,
            home_name TEXT,
            home_name_abbrev TEXT,
            away_name TEXT,
            away_name_abbrev TEXT,
            event_url TEXT,
            league TEXT,
            line_up_url TEXT,
            pim_total_team_1 INT,
            pim_total_team_2 INT,
            pp_perc_team_1 FLOAT,
            pp_perc_team_2 FLOAT,
            pp_time_team_1 TEXT,
            pp_time_team_2 TEXT,
            saves_total_team_1 INT,
            saves_total_team_2 INT,
            shots_total_team_1 INT,
            shots_total_team_2 INT,
            spectators INT
        )"""
        )

    def generate_sql_dict(self, table, d):
        cols = ", ".join(d.keys())
        var_str = ", ".join("?" * len(d))
        return "INSERT INTO %s (%s) VALUES (%s)" % (table, cols, var_str)

    def execute_db_query(self, sql):
        pass

    def insert_coaches(self, item, team, swehockey_id):

        line = {
            "team": team,
            "swehockey_id": swehockey_id,
        }

        coaches = item["lineup"][team]
        if len(coaches) > 0:
            line["player_number"] = 997
            line["player_last_name"] = coaches[0][0]
            line["player_first_name"] = coaches[0][1]
            line["line_name"] = "Head Coach"

            sql = self.generate_sql_dict("lines", line)
            self.cur.execute(sql, list(line.values()))
        if len(coaches) > 1:
            line["player_number"] = 998
            line["player_last_name"] = coaches[1][0]
            line["player_first_name"] = coaches[1][1]
            line["line_name"] = "Assistant Coach"

            sql = self.generate_sql_dict("lines", line)
            self.cur.execute(sql, list(line.values()))
        if len(coaches) > 2:
            logging.warning(f"More than 2 coaches! ID: {swehockey_id}")

    def insert_lines(self, item, team, swehockey_id):
        line_name = ""
        for i in item["lineup"][team]:
            if len(i["line_name"]) >= 1:
                line_name = i["line_name"]
            line = {
                "line_name": line_name,
                "team": team,
                "swehockey_id": swehockey_id,
            }
            for player in i["players"]:
                line["player_number"] = player[0]
                line["player_last_name"] = player[1]
                line["player_first_name"] = player[2]
                line["starting"] = any(
                    player[0] in number
                    for number in item["lineup"][f"starting_players_{team}"]
                )

                sql = self.generate_sql_dict("lines", line)
                self.cur.execute(sql, list(line.values()))

    def insert_refs(self, item, position, swehockey_id):
        refs = {"swehockey_id": swehockey_id, "position": position}
        for ref in item["lineup"][position]:
            refs["ref_name"] = ref
            sql = self.generate_sql_dict("refs", refs)
            self.cur.execute(sql, list(refs.values()))

    def insert_stat_by_period(self, item, stat, team, team_name, swehockey_id):
        stats = {
            "swehockey_id": swehockey_id,
            "team_name": team_name,
            "stat_name": stat,
        }
        for stat in enumerate(item[f"{stat}_by_period_{team}"]):
            stats["period"] = str(stat[0] + 1)
            stats["stat"] = stat[1]
            sql = self.generate_sql_dict("stats_by_period", stats)
            self.cur.execute(sql, list(stats.values()))

    def insert_score_by_period(self, item, swehockey_id):
        score = {"swehockey_id": swehockey_id, "stat_name": "score"}
        home_name = item["home_name"]
        away_name = item["away_name"]
        for period in enumerate(item["score_by_period"]):
            score["period"] = str(period[0] + 1)
            score["stat"] = period[1][0]
            score["team_name"] = home_name
            sql = self.generate_sql_dict("stats_by_period", score)
            self.cur.execute(sql, list(score.values()))

            score["stat"] = period[1][1]
            score["team_name"] = away_name
            sql = self.generate_sql_dict("stats_by_period", score)
            self.cur.execute(sql, list(score.values()))

    def insert_goalie_stats(self, item, swehockey_id):
        stats = {"swehockey_id": swehockey_id}
        for goalie, saves, team in zip(
            item["goalies_names"],
            item["goalies_saves"],
            item["goalies_teams"],
        ):
            stats["first_name"] = goalie[2]
            stats["last_name"] = goalie[1]
            stats["player_number"] = goalie[0]
            stats["shots"] = saves.split("/")[1]
            stats["saves"] = saves.split("/")[0]
            stats["team_name"] = team
            sql = self.generate_sql_dict("goalie_stats", stats)
            self.cur.execute(sql, list(stats.values()))

    def insert_game_events(self, item, swehockey_id):
        events = {"swehockey_id": swehockey_id}
        for event in item["game_events"]:
            player = event["player"]
            assist_1 = event["assist_1"]
            assist_2 = event["assist_2"]
            events["time"] = event["time"]
            events["team"] = event["team"]
            events["event"] = event["event"]
            if len(player) == 3:
                events["player_first_name"] = event["player"][2]
                events["player_last_name"] = event["player"][1]
                events["player_number"] = event["player"][0]
            if len(assist_1) == 3:
                events["assist_1_first_name"] = event["assist_1"][2]
                events["assist_1_last_name"] = event["assist_1"][1]
                events["assist_1_number"] = event["assist_1"][0]
            if len(assist_2) == 3:
                events["assist_2_first_name"] = event["assist_2"][2]
                events["assist_2_last_name"] = event["assist_2"][1]
                events["assist_2_number"] = event["assist_2"][0]

            d1 = event["details_1"]
            d2 = event["details_2"]

            # Penalty
            if len(d2) > 0:
                if d2["type"] == "penalty":
                    events["type"] = "penalty"
                    events["penalty_start_time"] = d2["penalty_start_time"]
                    events["penalty_end_time"] = d2["penalty_end_time"]
                    events["penalty_type"] = d1["note"]
                # Goal
                if d2["type"] == "penalty_shot":
                    events["type"] = "penalty_shot"
                    events["ps_outcome"] = "missed"
                    events["ps_goalie_number"] = d2["goalie_number"][0]
            if len(d1) > 0:
                if (d1["type"] == "goal") and ("(PS)" in event["event"]):
                    events["type"] = "penalty_shot"
                    events["ps_outcome"] = "scored"
                    events["ps_goalie_number"] = d2["goalie_number"][0]
                if d1["type"] == "goal":
                    plus_minus = {"swehockey_id": swehockey_id}

                    for num in d1["on_ice_plus"]:
                        plus_minus["side"] = "plus"
                        plus_minus["num"] = num
                        sql = self.generate_sql_dict("plus_minus", plus_minus)
                        self.cur.execute(sql, list(plus_minus.values()))

                    for num in d2["on_ice_minus"]:
                        plus_minus["side"] = "minus"
                        plus_minus["num"] = num
                        sql = self.generate_sql_dict("plus_minus", plus_minus)
                        self.cur.execute(sql, list(plus_minus.values()))

            sql = self.generate_sql_dict("game_events", events)
            self.cur.execute(sql, list(events.values()))

    def create_plus_minus_table(self):
        self.cur.execute(
            """CREATE TABLE plus_minus(
        swehockey_id INT,
        side TEXT,
        num INT
        )"""
        )

    def create_game_events_table(self):
        self.cur.execute(
            """CREATE TABLE game_events(
        id INTEGER PRIMARY KEY,
            swehockey_id INT,
            time TEXT,
            team TEXT,
            event TEXT,
            player_first_name TEXT,
            player_last_name TEXT,
            player_number INT,
            assist_1_first_name TEXT,
            assist_1_last_name TEXT,
            assist_1_number TEXT,
            assist_2_first_name TEXT,
            assist_2_last_name TEXT,
            assist_2_number TEXT,
            type TEXT,
            penalty_type TEXT,
            penalty_start_time TEXT,
            penalty_end_time TEXT,
            ps_outcome TEXT,
            ps_goalie_number INT
        )"""
        )

    def process_item(self, item, spider):
        swehockey_id = item["swehockey_id"]
        score_home = item["score"][0]
        score_away = item["score"][1]
        self.insert_lines(item, "lineup_home", swehockey_id)
        self.insert_lines(item, "lineup_away", swehockey_id)
        self.insert_coaches(item, "home_team_coaches", swehockey_id)
        self.insert_coaches(item, "away_team_coaches", swehockey_id)
        self.insert_refs(item, "refs", swehockey_id)
        self.insert_refs(item, "linesmen", swehockey_id)
        self.insert_stat_by_period(
            item, "shots", "team_1", item["home_name"], swehockey_id
        )
        self.insert_stat_by_period(
            item, "shots", "team_2", item["away_name"], swehockey_id
        )
        self.insert_stat_by_period(
            item, "saves", "team_1", item["home_name"], swehockey_id
        )
        self.insert_stat_by_period(
            item, "saves", "team_2", item["away_name"], swehockey_id
        )
        self.insert_stat_by_period(
            item, "pim", "team_1", item["home_name"], swehockey_id
        )
        self.insert_stat_by_period(
            item, "pim", "team_2", item["away_name"], swehockey_id
        )
        self.insert_score_by_period(item, swehockey_id)
        self.insert_goalie_stats(item, swehockey_id)
        self.insert_game_events(item, swehockey_id)

        # TODO:
        # date_time
        # game_events
        # shootout_events
        # Players -> player table with ID
        # Teams -> teams table with ID
        # Leagues -> league table with ID
        # swehockey id -> auto id

        # self.cur.execute(
        #     """INSERT OR IGNORE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        #     (
        #         item["swehockey_id"],
        #         item["arena"],
        #         item["home_name"],
        #         item["home_name_abbrev"],
        #         item["away_name"],
        #         item["away_name_abbrev"],
        #         item["event_url"],
        #         item["league"],
        #         item["line_up_url"],
        #         item["pim_total_team_1"],
        #         item["pim_total_team_2"],
        #         item["pp_perc_team_1"],
        #         item["pp_perc_team_2"],
        #         item["pp_time_team_1"],
        #         item["pp_time_team_2"],
        #         item["saves_total_team_1"],
        #         item["saves_total_team_2"],
        #         item["shots_total_team_1"],
        #         item["shots_total_team_2"],
        #         item["spectators"],
        #     ),
        # )
        self.con.commit()
        return item
