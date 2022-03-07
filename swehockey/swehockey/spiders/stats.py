import scrapy
import re
class StatsSpider(scrapy.Spider):
    name = 'stats'
    allowed_domains = ['stats.swehockey.se']
    start_urls = ['https://stats.swehockey.se/GamesByDate/2022-03-06']



    def parse(self, response):

        # Retrieve URL to each game a page 
        for game in response.xpath("//table[@class='tblContent']/tr/td/a[starts-with(@href, 'java')]"):
            game_link = game.xpath(".//@href").get()

            # Extract only the URL from the href
            url = game_link.split("'")[1]
            swehockey_id = url.split("/")[3]

            # Open Game Event link
            yield response.follow(url=url, callback=self.parse_game, cb_kwargs={'swehockey_id': swehockey_id})

    def parse_game(self, response, swehockey_id):
        game_info = response.xpath("//table[@class='tblContent'][1]")

        teams = game_info.xpath("//tr/th/h2/text()").get()
        teams = [team.strip() for team in teams.split("-")]

        date_time = game_info.xpath("//tr[2]/td[1]/h3/text()").get()
        league = game_info.xpath("//tr[2]/td[2]/h3/text()").get()
        arena = game_info.xpath("//tr[2]/td[3]/h3/b/text()").get()

        shots_team_1 = self.get_stats_by_period(game_info, 3, 3)
        shots_team_2 = self.get_stats_by_period(game_info, 3, 7)
        saves_team_1 = self.get_stats_by_period(game_info, 5, 3)
        saves_team_2 = self.get_stats_by_period(game_info, 5, 6)
        pim_team_1 = self.get_stats_by_period(game_info, 7, 3)
        pim_team_2 = self.get_stats_by_period(game_info, 7, 7)
        pp_time_team_1 = self.get_stats_by_period(game_info, 8, 3, False)
        pp_time_team_2 = self.get_stats_by_period(game_info, 8, 6, False)
        pp_perc_team_1 = game_info.xpath("//tr[8]/td[2]/strong/text()").get().strip()
        pp_perc_team_1 = float(pp_perc_team_1.replace(",", ".")
                               .replace("%", ""))
        pp_perc_team_2 = game_info.xpath("//tr[8]/td[5]/strong/text()").get().strip()
        pp_perc_team_2 = float(pp_perc_team_2.replace(",", ".")
                               .replace("%", ""))

        spectators = game_info.xpath("//td[@class='tdInfoArea']/div[4]/text()").get().strip()
        spectators = int(spectators.split(":")[1].strip())
        
        # Get score by period
        score = game_info.xpath("//td[@class='tdInfoArea']/div[2]/text()").get().strip()
        # Convert score to a 2D list of ints. Eg. [[1,0], [0,1], [3,0]]
        score = [[int(goal) for goal in goals.strip().split("-")]
                 for goals in re.sub('[()]', '', score).split(",")]
        # Separate score by team
        score_team_1 = [goals[0] for goals in score]
        score_team_2 = [goals[1] for goals in score]



        yield {
            'game_stats': {
                'team_1': teams[0],
                'team_2': teams[1],
                'date_time': date_time,
                'league': league,
                'arena': arena,
                'shots_team_1': shots_team_1,
                'shots_team_2': shots_team_2,
                'saves_team_1': saves_team_1,
                'saves_team_2': saves_team_2,
                'pim_team_1': pim_team_1,
                'pim_team_2': pim_team_2,
                'pp_time_team_1': pp_time_team_1,
                'pp_time_team_2': pp_time_team_2,
                'pp_perc_team_1': pp_perc_team_1,
                'pp_perc_team_2': pp_perc_team_2,
                'score': score,
                'score_team_1': score_team_1,
                'score_team_2': score_team_2,
                'spectators': spectators,
                'swehockey_id': swehockey_id
             }
        }

    # Helper function to parse a common pattern describing basic stats by
    # period.
    def get_stats_by_period(self, game_info, tr, td, int_list=True):
        stat = game_info.xpath(f"//tr[{tr}]/td[{td}]/text()").get().strip()
        stat = re.sub('[()]', '', stat)
        if (int_list is True):
            stat = stat.split(":")
            return [int(x) for x in stat]
        return stat
