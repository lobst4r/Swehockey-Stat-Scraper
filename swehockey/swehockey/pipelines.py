# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import sqlite3


class SwehockeyPipeline:
    def __init__(self):
        self.con = sqlite3.connect("test.db")
        self.cur = self.con.cursor()
        self.create_games_table()

    def create_games_table(self):
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS games(
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

    def process_item(self, item, spider):
        self.cur.execute(
            """INSERT OR IGNORE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item["swehockey_id"],
                item["arena"],
                item["home_name"],
                item["home_name_abbrev"],
                item["away_name"],
                item["away_name_abbrev"],
                item["event_url"],
                item["league"],
                item["line_up_url"],
                item["pim_total_team_1"],
                item["pim_total_team_2"],
                item["pp_perc_team_1"],
                item["pp_perc_team_2"],
                item["pp_time_team_1"],
                item["pp_time_team_2"],
                item["saves_total_team_1"],
                item["saves_total_team_2"],
                item["shots_total_team_1"],
                item["shots_total_team_2"],
                item["spectators"],
            ),
        )
        self.con.commit()
        return item
