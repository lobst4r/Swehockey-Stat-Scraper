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
            line_up_url = f"/Game/LineUps/{swehockey_id}"

            # Open Game Event link
            yield response.follow(url=line_up_url, callback=self.parse_line_up, cb_kwargs={'swehockey_id': swehockey_id})

    def parse_stats_summary(self, response, swehockey_id):
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

    def parse_game_actions(self, response, swehockey_id):
        goalies_stats = response.xpath("(//table[@class='tblContent'])[2]")

        goalies_teams = self.get_goalies(goalies_stats, 3)
        goalies = self.get_goalies(goalies_stats, 4)
        goalies_saves = self.get_goalies(goalies_stats, 5)

        # Find the last period of the game (including overtime) and extract data from each period
        actions = response.xpath("((//table[@class='tblContent'])[2]//tr/th/h3[contains(text(), 'Overtime') or contains(text(), 'overtime') or contains(text(), '3rd period')])[1]/ancestor::node()/following-sibling::tr[not(.//th)]")
        game_events = []
        for action in actions:
            time = (action.xpath(".//td[1]/text()").get() or "").strip()
            event = (action.xpath(".//td[2]/text()").get() or "").strip()
            team = (action.xpath(".//td[3]/text()").get() or "").strip()
            player = " ".join((action.xpath(".//td[4]/text()").get() or "")
                              .split())
            assist_1 = " ".join((action.xpath(".//td[4]/span[2]/div/text()")
                                 .get() or "").split())
            assist_2 = " ".join((action.xpath(".//td[4]/span[3]/div/text()")
                                 .get() or "").split())
            details_1 = " ".join((action.xpath(".//td[5]/text()").get() or "")
                                 .split())
            details_2 = " ".join((action.xpath(".//td[5]/text()[2]").get() or "")
                                 .split())
            game_events.append([time, event, team, player, assist_1, assist_2, details_1, details_2])

        # Get shootout data (if any)
        shootout_actions = response.xpath("((//table[@class='tblContent'])[2]/tr/th/h3[contains(text(), 'Game Winning Shots')])[1]/ancestor::tr[1]/following-sibling::tr/th/ancestor::tr[1]/preceding-sibling::tr/td[contains(text(), 'Missed') or contains(text(), 'Scored')]/ancestor::tr[1]")
        shootout_events = []
        for action in shootout_actions:
            scored = " ".join((action.xpath(".//td[1]/text()").get() or "")
                              .split())
            score = " ".join((action.xpath(".//td[2]/text()").get() or "")
                             .split())
            team = " ".join((action.xpath(".//td[3]/text()").get() or "")
                            .split())
            player = " ".join((action.xpath(".//td[4]/div[1]/text()").get() or "")
                              .split())
            goalie = " ".join((action.xpath(".//td[4]/div[2]/text()").get() or "")
                              .split())
            shootout_events.append([scored, score, team, player, goalie])

        yield {
            'goalies_teams': goalies_teams,
            'goalies': goalies,
            'goalies_saves': goalies_saves,
            'game_events': game_events,
            'shootout_events': shootout_events
        }

    def parse_line_up(self, response, swehockey_id):
        refs = response.xpath("(//table[@class='tblContent'])[2]//tr[1]/td[2]/text()").get()
        refs = [ref.strip() for ref in (refs or "").split(",")]
        linesmen = response.xpath("(//table[@class='tblContent'])[2]//tr[2]/td[2]/text()").get()
        linesmen = [linesman.strip() for linesman in (linesmen or "").split(",")]
        home_team_coaches = response.xpath("(//table[@class='tblContent'])[4]//tr[3]/td[2]/table/tr/td/text()").getall()
        home_team_coaches = [" ".join(coach.split()) for coach in home_team_coaches]
        away_team_coaches = response.xpath("(//table[@class='tblContent'])[4]//tr[3]/following-sibling::tr[last()]//table/tr/td/text()").getall()
        away_team_coaches = [" ".join(coach.split()) for coach in away_team_coaches]


        line_up_home_raw = response.xpath("((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/preceding-sibling::tr/descendant::*[contains(@style, 'text-align')]/ancestor::tr[1]")
        line_up_home = self.get_line_up(line_up_home_raw)
        line_up_away_raw = response.xpath("((//table[@class='tblContent'])[4]//tr/th[contains(@class, 'tdSubTitle')])[2]/ancestor::tr[1]/following-sibling::tr/td[contains(@style, 'text-align')]/ancestor::tr[1]")
        line_up_away = self.get_line_up(line_up_away_raw)        

            
            
        yield {
            'swehockey_id': swehockey_id,
            'refs': refs,
            'linesmen': linesmen,
            'home_team_coaches': home_team_coaches,
            'away_team_coaches': away_team_coaches,
            'line_up_home': line_up_home,
            'line_up_away': line_up_away
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

    def get_goalies(self, goalies_stats, td):
        goalies = goalies_stats.xpath(f"((//table[@class='tblContent'])[2]/tr/th/h3)[2]/ancestor::tr[1]/preceding-sibling::tr/td[{td}]")
        return [" ".join((team.xpath(".//text()").get() or "").split()) for team in goalies]

    def get_line_up(self, line_up_raw):
        line_up= {}
        prev_line_name = ""
        for line in line_up_raw:
            line_name = line.xpath(".//*/strong/text()").get()
            players = [" ".join((player or "").split()) for player in line
                       .xpath(".//td/div/text()").getall()]
            if line_name:
                # If there is a line name ("1st line" for example),
                # that means we are looking at the defensive players
                # for the home team. Otherwise we are looking at the
                # offensive players, which is information we want to
                # retain.
               prev_line_name = line_name
               line_up[line_name] = {}
               line_up[line_name]['players_row_1'] = players
            else:
               line_name = prev_line_name
               line_up[line_name]['players_row_2'] = players

        starting_players = line_up_raw.xpath(".//*[contains(@class, 'red')]/text()").getall()
        starting_players = [clean(player) for player in starting_players]
        line_up['starting_players'] = starting_players
        return line_up



def clean(text=""):
    # Remove all extra whitespace in a string.
    # Return an empty string if no string
    # is passed as an argument.
    return " ".join((text or "").split())
def clean_list(l=[]):
    return [clean(item) for item in l]

def split_string(text, separator=None):
    # Split a string.
    # Return an empty string if
    # no string is passed as an argument.
    # This is used for when a function might
    # return a null value.
    return (text or "").split(separator)
