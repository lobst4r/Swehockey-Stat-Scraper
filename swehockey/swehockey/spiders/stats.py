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

            # Open Game Event link
            yield response.follow(url=url, callback=self.parse_game)

    def parse_game(self, response):
        game_info = response.xpath("//table[@class='tblContent'][1]")

        teams = game_info.xpath("//tr/th/h2/text()").get()
        teams = [team.strip() for team in teams.split("-")]

        date_time = game_info.xpath("//tr[2]/td[1]/h3/text()").get()
        league = game_info.xpath("//tr[2]/td[2]/h3/text()").get()
        arena = game_info.xpath("//tr[2]/td[3]/h3/b/text()").get()

        # Get shots for team 1 and convert to list of ints (store the shots for each period)
        # shots_team_1 = game_info.xpath("//tr[3]/td[3]/text()").get().strip()
        # shots_team_1 = re.sub('[()]', '', shots_team_1).split(":")
        # shots_team_1 = [int(x) for x in shots_team_1]

        shots_team_1 = self.get_stats_by_period(game_info, 3, 3)
        # Get shots for team 2 and convert to list of ints (store the shots for each period)
        # shots_team_2 = game_info.xpath("//tr[3]/td[7]/text()").get().strip()
        # shots_team_2 = re.sub('[()]', '', shots_team_2).split(":")
        # shots_team_2 = [int(x) for x in shots_team_2]
        shots_team_2 = self.get_stats_by_period(game_info, 3, 7)
        
        # Get saves for team 1 and convert to list of ints (store the shots for each period)
        yield {
            'game_stats': {
                'team_1': teams[0],
                'team_2': teams[1],
                'date_time': date_time,
                'league': league,
                'arena': arena,
                'shots_team_1': shots_team_1,
                'shots_team_2': shots_team_2
             }
        }

    def get_stats_by_period(self, game_info, tr, td):
        stat = game_info.xpath(f"//tr[{tr}]/td[{td}]/text()").get().strip()
        stat = re.sub('[()]', '', stat).split(":")
        return [int(x) for x in stat]

