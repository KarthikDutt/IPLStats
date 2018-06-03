from Utility import setup_mongo_client
from stats_logger import logger
import sys
import traceback
from Utility import load_all_data_from_db

def get_list_all_batsmen(bat_stats_cursor):
    try:
        batsmen_list = []
        batsmen_list_unique = []
        for item in bat_stats_cursor:
            for player in item['stats']:
                #print(player)
                batsmen_list.append(player.keys())
        # player_list=list(player_list.keys())
        # print(player_list)
        for item in batsmen_list:
            for key in item:
                batsmen_list_unique.append(key)
        batsmen_list_unique = list(set(batsmen_list_unique))
        bat_stats_cursor.rewind()
    except:
        logger.error("Exception Inside get_list_all_batsmen Method" + traceback.format_exc())
        sys.exit(-1)
    return batsmen_list_unique


def get_list_all_bowlers(bowl_stats_cursor):
    try:
        bowler_list = []
        bowler_list_unique = []
        for item in bowl_stats_cursor:
            for player in item['stats']:
                bowler_list.append(player.keys())

        for item in bowler_list:
            for key in item:
                bowler_list_unique.append(key)
        bowler_list_unique = list(set(bowler_list_unique))
        bowl_stats_cursor.rewind()
    except:
        logger.error("Exception Inside get_list_all_batsmen Method" + traceback.format_exc())
        sys.exit(-1)
    return bowler_list_unique

def get_list_all_fielders(field_stats_cursor):
    try:
        fielder_list = []
        fielder_list_unique = []
        for item in field_stats_cursor:
            for player in item['stats']:
                fielder_list.append(player.keys())

        for item in fielder_list:
            for key in item:
                fielder_list_unique.append(key)
        fielder_list_unique = list(set(fielder_list_unique))
        field_stats_cursor.rewind()
    except:
        logger.error("Exception Inside get_list_all_batsmen Method" + traceback.format_exc())
        sys.exit(-1)
    return fielder_list_unique

def load_stats_from_db():
    match_info_cursor=load_all_data_from_db('testdb','match_info')
    bat_stats_cursor = load_all_data_from_db('testdb', 'bat_stats')
    batsmen_list_unique = get_list_all_batsmen(bat_stats_cursor)
    bowl_stats_cursor = load_all_data_from_db('testdb', 'bowl_stats')
    bowler_list_unique = get_list_all_bowlers(bowl_stats_cursor)
    field_stats_cursor = load_all_data_from_db('testdb', 'field_stats')
    fielder_list_unique=get_list_all_fielders(field_stats_cursor)
    consolidated_player_list = list(set(batsmen_list_unique + bowler_list_unique))
    #res_2=list(set(fielder_list_unique + consolidated_player_list))
    list_of_subs_who_cauught=list(set(fielder_list_unique) - set(consolidated_player_list))
    wickets_stats_cursor = load_all_data_from_db('testdb', 'wickets_stats')
    return bat_stats_cursor,bowl_stats_cursor,field_stats_cursor,wickets_stats_cursor,batsmen_list_unique,bowler_list_unique,consolidated_player_list

def get_individual_bat_raw_stats(player,bat_stats_cursor):
    bat_raw_stats=[]
    try:
        for item in bat_stats_cursor:
            for i,item2 in enumerate(item['stats']):
                if player in item2.keys():
                    #print(item2[player])
                    item2[player].append(i+1)
                    bat_raw_stats.append(item2[player])
        #print(player,bat_raw_stats)
        #print(bat_raw_stats)
        bat_stats_cursor.rewind()
    except:
        logger.error("Exception Inside get_individual_bat_raw_stats Method" + traceback.format_exc())
        sys.exit(-1)
    return bat_raw_stats

def get_individual_bat_stats(player,bat_stats_cursor):
    raw_bat_stats_list=get_individual_bat_raw_stats(player,bat_stats_cursor)
    #total_runs,balls_faced,strike_rate,total_no_balls,total_fours,total_sixes,percent_dots,\
    #percent_balls_boundaries,percent_runs_boundaries
    #if innings==1:
    total_matches_inn_1=len([item[9] for item in raw_bat_stats_list if item[9]==1])
    total_runs_inn_1=sum([item[0] for item in raw_bat_stats_list if item[9]==1])
    total_balls_faced_inn_1 = sum([item[1] for item in raw_bat_stats_list if item[9]==1])
    total_dots_faced_inn_1 = sum([item[3] for item in raw_bat_stats_list if item[9]==1])
    total_fours_inn_1 = sum([item[4] for item in raw_bat_stats_list if item[9]==1])
    total_sixes_inn_1 = sum([item[5] for item in raw_bat_stats_list if item[9]==1])
    #elif innings==2:
    total_matches_inn_2=len([item[9] for item in raw_bat_stats_list if item[9]==2])
    total_runs_inn_2=sum([item[0] for item in raw_bat_stats_list if item[9]==2])
    total_balls_faced_inn_2 = sum([item[1] for item in raw_bat_stats_list if item[9]==2])
    total_dots_faced_inn_2 = sum([item[3] for item in raw_bat_stats_list if item[9]==2])
    total_fours_inn_2 = sum([item[4] for item in raw_bat_stats_list if item[9]==2])
    total_sixes_inn_2 = sum([item[5] for item in raw_bat_stats_list if item[9]==2])
    #else:
    total_matches = len([item[9] for item in raw_bat_stats_list])
    total_runs = sum([item[0] for item in raw_bat_stats_list])
    total_balls_faced = sum([item[1] for item in raw_bat_stats_list ])
    total_dots_faced = sum([item[3] for item in raw_bat_stats_list ])
    total_fours = sum([item[4] for item in raw_bat_stats_list ])
    total_sixes = sum([item[5] for item in raw_bat_stats_list])
    try:
        overall_strike_rate_inn_1 = round((total_runs_inn_1 / total_balls_faced_inn_1) * 100, 1)
    except ZeroDivisionError:
        overall_strike_rate_inn_1 = 0
    try:
        overall_strike_rate_inn_2 = round((total_runs_inn_2 / total_balls_faced_inn_2) * 100, 1)
    except ZeroDivisionError:
        overall_strike_rate_inn_2 = 0
    try:
        overall_strike_rate= round((total_runs/total_balls_faced)*100,1)
    except ZeroDivisionError:
        overall_strike_rate=0
    try:
        percent_dots_inn_1 = round((total_dots_faced_inn_1 / total_balls_faced_inn_1) * 100, 1)
    except ZeroDivisionError:
        percent_dots_inn_1=0
    try:
        percent_dots_inn_2 = round((total_dots_faced_inn_2 / total_balls_faced_inn_2) * 100, 1)
    except:
        percent_dots_inn_2=0
    try:
        percent_dots=round((total_dots_faced/total_balls_faced)*100,1)
    except ZeroDivisionError:
        percent_dots=0
    try:
        percent_boundary_balls_inn_1=round((len([item[4] for item in raw_bat_stats_list if item[9]==1])/total_balls_faced_inn_1)*100,1)
    except ZeroDivisionError:
        percent_boundary_balls_inn_1=0
    try:
        percent_boundary_balls_inn_2 = round(
                (len([item[4] for item in raw_bat_stats_list if item[9] == 2]) / total_balls_faced_inn_2) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_balls_inn_2=0
    try:
        percent_boundary_balls = round((len([item[4] for item in raw_bat_stats_list ]) / total_balls_faced) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_balls=0
    try:
        percent_boundary_runs_inn_1 = round((sum([item[4] for item in raw_bat_stats_list if item[9] == 1]) / total_balls_faced_inn_1) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_runs_inn_1=0
    try:
        percent_boundary_runs_inn_2 = round(
                (sum([item[4] for item in raw_bat_stats_list if item[9] == 2]) / total_balls_faced_inn_2) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_runs_inn_2=0
    try:
        percent_boundary_runs = round((sum([item[4] for item in raw_bat_stats_list]) / total_balls_faced) * 100, 1)
    except:
        percent_boundary_runs=0
    try:
        highest_score_inn_1=max([item[0] for item in raw_bat_stats_list if item[9] == 1])
    except ValueError:
        highest_score_inn_1 = 0
    try:
        highest_score_inn_2 = max([item[0] for item in raw_bat_stats_list if item[9] == 2])
    except ValueError:
        highest_score_inn_2=0
    try:
        highest_score = max([item[0] for item in raw_bat_stats_list])
    except ValueError:
        highest_score=0
    overall_batting_player_stats=[total_matches,total_runs,total_balls_faced,overall_strike_rate,total_dots_faced,total_fours,
           total_sixes,percent_dots,percent_boundary_balls,percent_boundary_runs,highest_score]
    overall_batting_player_stats_inn_1 = [total_matches_inn_1, total_runs_inn_1, total_balls_faced_inn_1, overall_strike_rate_inn_1, total_dots_faced_inn_1,
                                    total_fours_inn_1,
                                    total_sixes_inn_1, percent_dots_inn_1, percent_boundary_balls_inn_1, percent_boundary_runs_inn_1,
                                    highest_score_inn_1]
    overall_batting_player_stats_inn_2 = [total_matches_inn_2, total_runs_inn_2, total_balls_faced_inn_2, overall_strike_rate_inn_2, total_dots_faced_inn_2,
                                    total_fours_inn_2,
                                    total_sixes_inn_2, percent_dots_inn_2, percent_boundary_balls_inn_2, percent_boundary_runs_inn_2,
                                    highest_score_inn_2]
    '''print("Player              - ",player)
    print("Total Innings       - ",total_matches)
    print("Total Runs          - ", total_runs)
    print("Total Balls Faced   - ", total_balls_faced)
    print("Overall Strike rate - ", overall_strike_rate)
    print("No of Dot balls     - ", total_dots_faced)
    print("No of 4s            - ", total_fours)
    print("No of 6s            - ", total_sixes)
    print("% of Dots           - ", percent_dots)
    print("% of Boundary balls - ", percent_boundary_balls)
    print("% of Boundary Runs  - ", percent_boundary_runs)
    print("Highest Score       - ", highest_score)'''

    return overall_batting_player_stats,overall_batting_player_stats_inn_1,overall_batting_player_stats_inn_2

def get_individual_bowl_raw_stats(player,bowl_stats_cursor,wickets_stats_cursor):
    bowl_raw_stats=[]
    try:
        for item in bowl_stats_cursor:
            for i,item2 in enumerate(item['stats']):
                if player in item2.keys():
                    item2[player].append(i + 1)
                    bowl_raw_stats.append(item2[player])
        #print(bowl_raw_stats)
        if not bowl_raw_stats:
            raise ValueError
        bowl_stats_cursor.rewind()
    except ValueError:
        bowl_stats_cursor.rewind()
        return [[0,0,0,0,0,0,0,0,0,0,0]]
    except:
        bowl_stats_cursor.rewind()
        logger.error("Exception Inside get_individual_bowl_raw_stats Method" + traceback.format_exc())
        sys.exit(-1)
    return bowl_raw_stats

def get_individual_bowl_stats(player,bowl_stats_cursor,wickets_stats_cursor):
    raw_bowl_stats_list=get_individual_bowl_raw_stats(player,bowl_stats_cursor,wickets_stats_cursor)
    #total_runs,balls_faced,strike_rate,total_no_balls,total_fours,total_sixes,percent_dots,\
    #percent_balls_boundaries,percent_runs_boundaries
    total_matches=len([item[9] for item in raw_bowl_stats_list])
    total_runs_conc=sum([item[0] for item in raw_bowl_stats_list])
    total_balls_bowled = sum([item[1] for item in raw_bowl_stats_list])
    total_dots_bowled = sum([item[3] for item in raw_bowl_stats_list])
    total_fours_conc = sum([item[4] for item in raw_bowl_stats_list])
    total_sixes_conc = sum([item[5] for item in raw_bowl_stats_list])
    total_wickets_taken = sum([item[9] for item in raw_bowl_stats_list])

    total_matches_inn_1=len([item[10] for item in raw_bowl_stats_list if item[10]==1])
    total_runs_conc_inn_1=sum([item[0] for item in raw_bowl_stats_list if item[10]==1])
    total_balls_bowled_inn_1 = sum([item[1] for item in raw_bowl_stats_list if item[10]==1])
    total_dots_bowled_inn_1 = sum([item[3] for item in raw_bowl_stats_list if item[10]==1])
    total_fours_conc_inn_1 = sum([item[4] for item in raw_bowl_stats_list if item[10]==1])
    total_sixes_conc_inn_1 = sum([item[5] for item in raw_bowl_stats_list if item[10]==1])
    total_wickets_taken_inn_1 = sum([item[9] for item in raw_bowl_stats_list if item[10]==1])

    total_matches_inn_2 = len([item[10] for item in raw_bowl_stats_list if item[10] == 2])
    total_runs_conc_inn_2=sum([item[0] for item in raw_bowl_stats_list if item[10] == 2])
    total_balls_bowled_inn_2 = sum([item[1] for item in raw_bowl_stats_list if item[10] == 2])
    total_dots_bowled_inn_2 = sum([item[3] for item in raw_bowl_stats_list if item[10] == 2])
    total_fours_conc_inn_2 = sum([item[4] for item in raw_bowl_stats_list if item[10] == 2])
    total_sixes_conc_inn_2 = sum([item[5] for item in raw_bowl_stats_list if item[10] == 2])
    total_wickets_taken_inn_2 = sum([item[9] for item in raw_bowl_stats_list if item[10] == 2])
    try:
        overall_economy_rate= round((total_runs_conc/total_balls_bowled)*6,1)
    except ZeroDivisionError:
        overall_economy_rate=0
    try:
        overall_economy_rate_inn_1= round((total_runs_conc_inn_1/total_balls_bowled_inn_1)*6,1)
    except ZeroDivisionError:
        overall_economy_rate_inn_1=0
    try:
        overall_economy_rate_inn_2= round((total_runs_conc_inn_2/total_balls_bowled_inn_2)*6,1)
    except ZeroDivisionError:
        overall_economy_rate_inn_2=0
    try:
        percent_dots_bowled=round((total_dots_bowled/total_balls_bowled)*100,1)
    except ZeroDivisionError:
        percent_dots_bowled=0
    try:
        percent_dots_bowled_inn_1=round((total_dots_bowled_inn_1/total_balls_bowled_inn_1)*100,1)
    except ZeroDivisionError:
        percent_dots_bowled_inn_1=0
    try:
        percent_dots_bowled_inn_2=round((total_dots_bowled_inn_2/total_balls_bowled_inn_2)*100,1)
    except ZeroDivisionError:
        percent_dots_bowled_inn_2=0
    try:
        percent_boundary_balls=round((len([item[4] for item in raw_bowl_stats_list ])/total_balls_bowled)*100,1)
    except ZeroDivisionError:
        percent_boundary_balls=0
    try:
        percent_boundary_balls_inn_1=round((len([item[4] for item in raw_bowl_stats_list if item[10]==1])/total_balls_bowled_inn_1)*100,1)
    except ZeroDivisionError:
        percent_boundary_balls_inn_1=0
    try:
        percent_boundary_balls_inn_2=round((len([item[4] for item in raw_bowl_stats_list if item[10]==2])/total_balls_bowled_inn_2)*100,1)
    except ZeroDivisionError:
        percent_boundary_balls_inn_2=0
    try:
        percent_boundary_runs = round((sum([item[4] for item in raw_bowl_stats_list]) / total_balls_bowled) * 100, 1)
    except:
        percent_boundary_runs=0
    try:
        percent_boundary_runs_inn_1 = round((sum([item[4] for item in raw_bowl_stats_list if item[10]==1]) / total_balls_bowled_inn_1) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_runs_inn_1=0
    try:
        percent_boundary_runs_inn_2 = round((sum([item[4] for item in raw_bowl_stats_list if item[10]==2]) / total_balls_bowled_inn_2) * 100, 1)
    except ZeroDivisionError:
        percent_boundary_runs_inn_2=0
    #highest_score=max([item[0] for item in raw_bat_stats_list])
    overall_bownling_player_stats=[total_matches,total_runs_conc,total_balls_bowled,overall_economy_rate,total_dots_bowled,total_fours_conc,
                                  total_sixes_conc,percent_dots_bowled,percent_boundary_balls,percent_boundary_runs,total_wickets_taken]

    overall_bownling_player_stats_inn_1=[total_matches_inn_1,total_runs_conc_inn_1,total_balls_bowled_inn_1,overall_economy_rate_inn_1,total_dots_bowled_inn_1,total_fours_conc_inn_1,
                                  total_sixes_conc_inn_1,percent_dots_bowled_inn_1,percent_boundary_balls_inn_1,percent_boundary_runs_inn_1,total_wickets_taken_inn_1]
    overall_bownling_player_stats_inn_2=[total_matches_inn_2,total_runs_conc_inn_2,total_balls_bowled_inn_2,overall_economy_rate_inn_2,total_dots_bowled_inn_2,total_fours_conc_inn_2,
                                  total_sixes_conc_inn_2,percent_dots_bowled_inn_2,percent_boundary_balls_inn_2,percent_boundary_runs_inn_2,total_wickets_taken_inn_2]
    '''print("Player                            - ",player)
    print("Total Innings                     - ",total_matches)
    print("Total Runs Conceded               - ", total_runs_conc)
    print("Total Wickets taken               - ", total_wickets_taken)
    print("Total Balls Bowled                - ", total_balls_bowled)
    print("Overall Economy rate              - ", overall_economy_rate)
    print("No of Dot balls Bowled            - ", total_dots_bowled)
    print("No of 4s Conceded                 - ", total_fours_conc)
    print("No of 6s Conceded                 - ", total_sixes_conc)
    print("% of Dot balls bowled             - ", percent_dots_bowled)
    print("% of Boundary balls Bowled        - ", percent_boundary_balls)
    print("% of Runs in Boundaries Conceded  - ", percent_boundary_runs)'''

    return overall_bownling_player_stats,overall_bownling_player_stats_inn_1,overall_bownling_player_stats_inn_2

def get_individual_field_raw_stats(player,field_stats_cursor):
    field_raw_stats=[]
    try:
        for item in field_stats_cursor:
            for item2 in item['stats']:
                if player in item2.keys():
                    #print(item2[player])
                    field_raw_stats.append(item2[player])
        #print(bat_raw_stats)
        if not field_raw_stats:
            raise ValueError
        field_stats_cursor.rewind()
    except ValueError:
        return [0]
        field_stats_cursor.rewind()
    except:
        logger.error("Exception Inside get_individual_field_raw_stats Method" + traceback.format_exc())
        sys.exit(-1)
    return field_raw_stats

def get_individual_field_stats(player,field_stats_cursor):
    try:
        raw_field_stats_list=get_individual_field_raw_stats(player,field_stats_cursor)
        total_catches = sum([item for item in raw_field_stats_list])
        #print("Player                            - ", player)
        #print("Total Catches                     - ", total_catches)
        overall_fielding_player_stats=[total_catches]
    except:
        logger.error("Exception Inside get_individual_field_stats Method" + traceback.format_exc())
        sys.exit(-1)
    return overall_fielding_player_stats