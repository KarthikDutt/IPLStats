import traceback
import sys
from stats_logger import logger
from collections import OrderedDict
from DbFetch import get_individual_bat_stats,get_individual_bowl_stats,get_individual_field_stats
from Utility import insert_docs_to_tables,load_all_data_from_db

# Generates individual bat, bowl and field statistics for all players
# This function runs only when generate_stats=true. We want to run it only when new matches are added
def generate_ind_stats_data_for_all(consolidated_list,bat_stats_cursor, bowl_stats_cursor, field_stats_cursor, wickets_stats_cursor):
    individual_stats_all = {}
    overall_batting_player_stats_dict = OrderedDict() # Ordered Dict is needed as we derive rank.
    #Normal dict will not store the order in which data was inserted
    overall_bowling_player_stats_dict= OrderedDict()
    overall_fielding_player_stats_dict= OrderedDict()
    for key in consolidated_list:# Consolidated list has all the player names
        try:
        # print(key)
        # print("------------------BATTING STATS-------------------")
            overall_batting_player_stats = get_individual_bat_stats(key, bat_stats_cursor)
            #Bat stats has batting records for all matches
            overall_batting_player_stats_dict[key] = overall_batting_player_stats #Append new player as key in dict
            #Derive bat stats for indivuidual players
            # print ("-----------------BOWLING STATS-------------------")
            overall_bowling_player_stats = get_individual_bowl_stats(key, bowl_stats_cursor, wickets_stats_cursor)
            #print(overall_bowling_player_stats)
            overall_bowling_player_stats_dict[key] = overall_bowling_player_stats
            # print ("-----------------FIELDING STATS------------------")
            overall_fielding_player_stats = get_individual_field_stats(key, field_stats_cursor)
            overall_fielding_player_stats_dict[key] = overall_fielding_player_stats
            individual_stats_all[key] = [overall_batting_player_stats, overall_bowling_player_stats,
                                         overall_fielding_player_stats]
        except:
            logger.error("Exception Inside generate_ind_stats_data_for_all Method" + traceback.format_exc())
            sys.exit(-1)

    overall_batting_player_doc = {"stats":[overall_batting_player_stats_dict]}
    overall_bowling_player_doc = {"stats": [overall_bowling_player_stats_dict]}
    overall_fielding_player_doc = {"stats": [overall_fielding_player_stats_dict]}
    #print (overall_bowling_player_stats_dict)
    post_id_bat=insert_docs_to_tables('testdb', 'individual_bat_stats_all', overall_batting_player_doc)
    post_id_bowl = insert_docs_to_tables('testdb', 'individual_bowl_stats_all', overall_bowling_player_doc)
    post_id_field = insert_docs_to_tables('testdb', 'individual_field_stats_all', overall_fielding_player_doc)

    return overall_batting_player_stats_dict,overall_bowling_player_stats_dict,overall_fielding_player_stats_dict
# This function runs only when generate_stats=true. We want to run it only when new matches are added
# It takes 5-6 seconds to run these two functions. So run them once and sae stats in db
def generate_batting_rankings(overall_batting_player_stats_dict):
    try:
    #####Rankings batting
    #Ordered dict ensures that order of insert of docs based on ranking is not messed up usual behaviour of dict
    #Ordered dict is a tuple though
        highest_scores_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][10], reverse=True))
        most_matches_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][0], reverse=True))
        most_runs_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][1], reverse=True))
        highest_str_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][3], reverse=True))
        number_of_fours_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][5], reverse=True))
        number_of_sixes_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][6], reverse=True))
        percent_dots_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][7], reverse=True))
        percent_boundary_order = OrderedDict(
            sorted(overall_batting_player_stats_dict.items(), key=lambda x: x[1][9], reverse=True))
        #####Ranking
        # print (type(individual_stats_all))
        highest_scores_order = list(highest_scores_order.items())
        highest_scores_order_2 = []
        for item in highest_scores_order:
            highest_scores_order_2.append([item[0], item[1][10]])
        #Player name is item[0] and item[1][10]] is highest score
        most_matches_order = list(most_matches_order.items())
        most_matches_order_2 = []
        for item in most_matches_order:
            most_matches_order_2.append([item[0], item[1][0]])

        most_runs_order = list(most_runs_order.items())
        most_runs_order_2 = []
        for item in most_runs_order:
            most_runs_order_2.append([item[0], item[1][1]])

        highest_str_order = list(highest_str_order.items())
        highest_str_order_2 = []
        for item in highest_str_order:
            if item[1][0] > 25:
                highest_str_order_2.append([item[0], item[1][3]])

        number_of_fours_order = list(number_of_fours_order.items())
        number_of_fours_order_2 = []
        for item in number_of_fours_order:
            number_of_fours_order_2.append([item[0], item[1][5]])

        number_of_sixes_order = list(number_of_sixes_order.items())
        number_of_sixes_order_2 = []
        for item in number_of_sixes_order:
            number_of_sixes_order_2.append([item[0], item[1][6]])

        percent_dots_order = list(percent_dots_order.items())
        percent_dots_order_2 = []
        for item in percent_dots_order:
            if item[1][1] > 500:
                percent_dots_order_2.append([item[0], item[1][7]])

        percent_boundary_order = list(percent_boundary_order.items())
        percent_boundary_order_2 = []
        for item in percent_boundary_order:
            if item[1][1] > 500:
                percent_boundary_order_2.append([item[0], item[1][9]])

        highest_score_rank_doc = {"stats":highest_scores_order_2}
        most_matches_rank_doc = {"stats": most_matches_order_2}
        most_runs_rank_doc = {"stats": most_runs_order_2}
        highest_str_rank_doc = {"stats": highest_str_order_2}
        number_of_fours_rank_doc = {"stats": number_of_fours_order_2}
        number_of_sixes_rank_doc = {"stats": number_of_sixes_order_2}
        percent_dots_rank_doc = {"stats": percent_dots_order_2}
        percent_boundary_rank_doc = {"stats": percent_boundary_order_2}
    except:
        logger.error("Exception Inside generate_batting_rankings Method" + traceback.format_exc())
        sys.exit(-1)

    post_id_high_score=insert_docs_to_tables('testdb', 'highest_score_rank', highest_score_rank_doc)
    post_id_most_matches = insert_docs_to_tables('testdb', 'most_matches_rank', most_matches_rank_doc)
    post_id_most_runs = insert_docs_to_tables('testdb', 'most_runs_rank', most_runs_rank_doc)
    post_id_high_str = insert_docs_to_tables('testdb', 'highest_str_rank', highest_str_rank_doc)
    post_id_fours = insert_docs_to_tables('testdb', 'number_of_fours_rank', number_of_fours_rank_doc)
    post_id_sixes = insert_docs_to_tables('testdb', 'number_of_sixes_rank', number_of_sixes_rank_doc)
    post_id_dots = insert_docs_to_tables('testdb', 'percent_dots_rank', percent_dots_rank_doc)
    post_id_boundaries = insert_docs_to_tables('testdb', 'percent_boundary_rank', percent_boundary_rank_doc)

#Load individual player stats and rankings from database.
def load_rankings_from_db():
    ind_bat_stats_cursor = load_all_data_from_db('testdb', 'individual_bat_stats_all')
    ind_bowl_stats_cursor = load_all_data_from_db('testdb', 'individual_bowl_stats_all')
    ind_field_stats_cursor = load_all_data_from_db('testdb', 'individual_field_stats_all')
    highest_str_rank_cursor = load_all_data_from_db('testdb', 'highest_str_rank')
    most_matches_rank_cursor = load_all_data_from_db('testdb', 'most_matches_rank')
    most_runs_rank_cursor = load_all_data_from_db('testdb', 'most_runs_rank')
    number_of_fours_rank_cursor = load_all_data_from_db('testdb', 'number_of_fours_rank')
    number_of_sixes_rank_cursor = load_all_data_from_db('testdb', 'number_of_sixes_rank')
    highest_score_rank_cursor = load_all_data_from_db('testdb', 'highest_score_rank')
    percent_boundary_rank_cursor = load_all_data_from_db('testdb', 'percent_boundary_rank')
    percent_dots_rank_cursor = load_all_data_from_db('testdb', 'percent_dots_rank')
    best_economy_rank_cursor = load_all_data_from_db('testdb', 'best_economy_rank')
    highest_wickets_rank_cursor = load_all_data_from_db('testdb', 'highest_wickets_rank')
    most_boundary_balls_rank_cursor = load_all_data_from_db('testdb', 'most_boundary_balls_rank')
    most_fours_conc_rank_cursor = load_all_data_from_db('testdb', 'most_fours_conc_rank')
    most_percent_dots_rank_cursor = load_all_data_from_db('testdb', 'most_percent_dots_rank')
    most_sixes_conc_rank_cursor = load_all_data_from_db('testdb', 'most_sixes_conc_rank')
    try:
        [percent_dots_rank_tuple]= [item['stats'] for item in percent_dots_rank_cursor]
        [ind_bat_stats_tuple] = [item['stats'] for item in ind_bat_stats_cursor]
        [ind_bowl_stats_tuple]=[item['stats'] for item in ind_bowl_stats_cursor]
        [ind_field_stats_tuple]=[item['stats'] for item in ind_field_stats_cursor]
        [highest_score_rank_tuple] = [item['stats'] for item in highest_score_rank_cursor]
        [highest_str_rank_tuple] = [item['stats'] for item in highest_str_rank_cursor]
        [most_runs_rank_tuple] = [item['stats'] for item in most_runs_rank_cursor]
        [most_matches_rank_tuple] = [item['stats'] for item in most_matches_rank_cursor]
        [number_of_fours_rank_tuple] = [item['stats'] for item in number_of_fours_rank_cursor]
        [number_of_sixes_rank_tuple] = [item['stats'] for item in number_of_sixes_rank_cursor]
        [percent_boundary_rank_tuple] = [item['stats'] for item in percent_boundary_rank_cursor]
        [best_eco_tuple] = [item['stats'] for item in best_economy_rank_cursor]
        [higest_wickets_tuple] = [item['stats'] for item in highest_wickets_rank_cursor]
        [most_boundary_balls_tuple] = [item['stats'] for item in most_boundary_balls_rank_cursor]
        [most_fours_conc_tuple] = [item['stats'] for item in most_fours_conc_rank_cursor]
        [most_percent_dots_tuple] = [item['stats'] for item in most_percent_dots_rank_cursor]
        [most_sixes_conc_tuple] = [item['stats'] for item in most_sixes_conc_rank_cursor]
    except:
        logger.error("Exception Inside load_rankings_from_db Method" + traceback.format_exc())
        sys.exit(-1)

    return ind_bat_stats_tuple,ind_bowl_stats_tuple,ind_field_stats_tuple,highest_score_rank_tuple,\
       highest_str_rank_tuple,most_matches_rank_tuple,most_runs_rank_tuple,number_of_fours_rank_tuple,\
       number_of_sixes_rank_tuple,percent_boundary_rank_tuple,percent_dots_rank_tuple,best_eco_tuple,\
           higest_wickets_tuple,most_boundary_balls_tuple,most_fours_conc_tuple,most_percent_dots_tuple,\
           most_sixes_conc_tuple

def get_player_stats(key,ind_bat_stats_tuple,ind_bowl_stats_tuple,ind_field_stats_tuple,highest_score_rank_tuple,\
       highest_str_rank_tuple,most_matches_rank_tuple,most_runs_rank_tuple,number_of_fours_rank_tuple,\
       number_of_sixes_rank_tuple,percent_boundary_rank_tuple,percent_dots_rank_tuple,best_eco_tuple,
        higest_wickets_tuple,most_boundary_balls_tuple,most_fours_conc_tuple,most_percent_dots_tuple,most_sixes_conc_tuple):
    try:
        for item in ind_bat_stats_tuple:
            print (item[key])
        for item in ind_bowl_stats_tuple:
            print (item[key])
        for item in ind_field_stats_tuple:
            print (item[key])
        print("highest_score_rank",[x for x, y in enumerate(highest_score_rank_tuple) if y[0] == key])
        print("highest_str_rank",[x for x, y in enumerate(highest_str_rank_tuple) if y[0] == key])
        print("most_matches_rank",[x for x, y in enumerate(most_matches_rank_tuple) if y[0] == key])
        print("most_runs_rank",[x for x, y in enumerate(most_runs_rank_tuple) if y[0] == key])
        print("number_of_fours_rank",[x for x, y in enumerate(number_of_fours_rank_tuple) if y[0] == key])
        print("number_of_sixes_rank",[x for x, y in enumerate(number_of_sixes_rank_tuple) if y[0] == key])
        print("percent_boundary_rank",[x for x, y in enumerate(percent_boundary_rank_tuple) if y[0] == key])
        print("percent_dots_rank",[x for x, y in enumerate(percent_dots_rank_tuple) if y[0] == key])
        print("Eco Rate Rank", [x for x, y in enumerate(best_eco_tuple) if y[0] == key])
        print("Highest Wickets Rank", [x for x, y in enumerate(higest_wickets_tuple) if y[0] == key])
        print("Most Boundary Balls percent rank", [x for x, y in enumerate(most_boundary_balls_tuple) if y[0] == key])
        print("Most Fours Conceded rank", [x for x, y in enumerate(most_fours_conc_tuple) if y[0] == key])
        print("Most dots percent bowled rank", [x for x, y in enumerate(most_percent_dots_tuple) if y[0] == key])
        print("Most 6s conc rank", [x for x, y in enumerate(most_sixes_conc_tuple) if y[0] == key])
    except:
        logger.error("Exception Inside get_player_stats Method" + traceback.format_exc())
        sys.exit(-1)
    #print(ind_bowl_stats_tuple[key])
    #print(ind_field_stats_tuple[key])

def generate_bowling_rankings(overall_bowling_player_stats_dict):
    try:
    #####Rankings batting
    #Ordered dict ensures that order of insert of docs based on ranking is not messed up usual behaviour of dict
    #Ordered dict is a tuple though
        highest_wickets_order = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][10], reverse=True))
        best_economy_rate = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][3]))
        most_fours_conc = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][5], reverse=True))
        most_sixes_conc = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][6], reverse=True))
        most_percent_dots = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][7], reverse=True))
        most_percent_balls_boundaries = OrderedDict(
            sorted(overall_bowling_player_stats_dict.items(), key=lambda x: x[1][8], reverse=True))

        #####Ranking
        # print (type(individual_stats_all))


        highest_wickets_order = list(highest_wickets_order.items())
        highest_wickets_order_2 = []
        for item in highest_wickets_order:
            highest_wickets_order_2.append([item[0], item[1][10]])
        #Player name is item[0] and item[1][10]] is highest score
        best_economy_rate = list(best_economy_rate.items())
        best_economy_rate_2 = []
        for item in best_economy_rate:
            if item[1][0] > 25:
                best_economy_rate_2.append([item[0], item[1][3]])

        most_fours_conc = list(most_fours_conc.items())
        most_fours_conc_2 = []
        for item in most_fours_conc:
            most_fours_conc_2.append([item[0], item[1][5]])

        most_sixes_conc = list(most_sixes_conc.items())
        most_sixes_conc_2 = []
        for item in most_sixes_conc:
            most_sixes_conc_2.append([item[0], item[1][6]])

        most_percent_dots = list(most_percent_dots.items())
        most_percent_dots_2 = []
        for item in most_percent_dots:
            if item[1][0] > 25:
                most_percent_dots_2.append([item[0], item[1][7]])

        most_percent_balls_boundaries = list(most_percent_balls_boundaries.items())
        most_percent_balls_boundaries_2 = []
        for item in most_percent_balls_boundaries:
            if item[1][0] > 25:
                most_percent_balls_boundaries_2.append([item[0], item[1][8]])

        highest_wickets_order_doc = {"stats":highest_wickets_order_2}
        best_economy_rate_doc = {"stats": best_economy_rate_2}
        most_fours_conc_doc = {"stats": most_fours_conc_2}
        most_sixes_conc_doc = {"stats": most_sixes_conc_2}
        most_percent_dots_doc = {"stats": most_percent_dots_2}
        most_percent_balls_boundaries_doc = {"stats": most_percent_balls_boundaries_2}
    except:
        logger.error("Exception Inside generate_batting_rankings Method" + traceback.format_exc())
        sys.exit(-1)

    post_id_high_score=insert_docs_to_tables('testdb', 'highest_wickets_rank', highest_wickets_order_doc)
    post_id_most_matches = insert_docs_to_tables('testdb', 'best_economy_rank', best_economy_rate_doc)
    post_id_most_runs = insert_docs_to_tables('testdb', 'most_fours_conc_rank', most_fours_conc_doc)
    post_id_high_str = insert_docs_to_tables('testdb', 'most_sixes_conc_rank', most_sixes_conc_doc)
    post_id_fours = insert_docs_to_tables('testdb', 'most_percent_dots_rank', most_percent_dots_doc)
    post_id_sixes = insert_docs_to_tables('testdb', 'most_boundary_balls_rank', most_percent_balls_boundaries_doc)
