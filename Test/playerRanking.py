from collections import OrderedDict
from DbFetch import get_individual_bat_stats,get_individual_bowl_stats,get_individual_field_stats
from Utility import insert_docs_to_tables,load_all_data_from_db

def generate_ind_stats_data_for_all(consolidated_list,bat_stats_cursor, bowl_stats_cursor, field_stats_cursor, wickets_stats_cursor):
    individual_stats_all = {}
    overall_batting_player_stats_dict = OrderedDict()
    overall_bowling_player_stats_dict= OrderedDict()
    overall_fielding_player_stats_dict= OrderedDict()
    for key in consolidated_list:
        # print(key)
        # print("------------------BATTING STATS-------------------")
        overall_batting_player_stats = get_individual_bat_stats(key, bat_stats_cursor)
        overall_batting_player_stats_dict[key] = overall_batting_player_stats
        # print ("-----------------BOWLING STATS-------------------")
        overall_bowling_player_stats = get_individual_bowl_stats(key, bowl_stats_cursor, wickets_stats_cursor)
        overall_bowling_player_stats_dict[key] = overall_bowling_player_stats
        # print ("-----------------FIELDING STATS------------------")
        overall_fielding_player_stats = get_individual_field_stats(key, field_stats_cursor)
        overall_fielding_player_stats_dict[key] = overall_fielding_player_stats
        individual_stats_all[key] = [overall_batting_player_stats, overall_bowling_player_stats,
                                     overall_fielding_player_stats]

    overall_batting_player_doc = {"stats":[overall_batting_player_stats_dict]}
    overall_bowling_player_doc = {"stats": [overall_bowling_player_stats_dict]}
    overall_fielding_player_doc = {"stats": [overall_fielding_player_stats_dict]}

    post_id_bat=insert_docs_to_tables('testdb', 'individual_bat_stats_all', overall_batting_player_doc)
    post_id_bowl = insert_docs_to_tables('testdb', 'individual_bowl_stats_all', overall_bowling_player_doc)
    post_id_field = insert_docs_to_tables('testdb', 'individual_field_stats_all', overall_fielding_player_doc)

    return overall_batting_player_stats_dict

def generate_batting_rankings(overall_batting_player_stats_dict):
    #####Rankings batting
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

    post_id_high_score=insert_docs_to_tables('testdb', 'highest_score_rank', highest_score_rank_doc)
    post_id_most_matches = insert_docs_to_tables('testdb', 'most_matches_rank', most_matches_rank_doc)
    post_id_most_runs = insert_docs_to_tables('testdb', 'most_runs_rank', most_runs_rank_doc)
    post_id_high_str = insert_docs_to_tables('testdb', 'highest_str_rank', highest_str_rank_doc)
    post_id_fours = insert_docs_to_tables('testdb', 'number_of_fours_rank', number_of_fours_rank_doc)
    post_id_sixes = insert_docs_to_tables('testdb', 'number_of_sixes_rank', number_of_sixes_rank_doc)
    post_id_dots = insert_docs_to_tables('testdb', 'percent_dots_rank', percent_dots_rank_doc)
    post_id_boundaries = insert_docs_to_tables('testdb', 'percent_boundary_rank', percent_boundary_rank_doc)

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

    for item in ind_bat_stats_cursor:
        ind_bat_stats_tuple=item['stats']
    for item in ind_bowl_stats_cursor:
        ind_bowl_stats_tuple=item['stats']
    for item in ind_field_stats_cursor:
        ind_field_stats_tuple=item['stats']
    for item in highest_score_rank_cursor:
        highest_score_rank_tuple=item['stats']
    for item in highest_str_rank_cursor:
        highest_str_rank_tuple=item['stats']
    for item in most_matches_rank_cursor:
        most_matches_rank_tuple=item['stats']
    for item in most_runs_rank_cursor:
        most_runs_rank_tuple=item['stats']
    for item in number_of_fours_rank_cursor:
        number_of_fours_rank_tuple=item['stats']
    for item in number_of_sixes_rank_cursor:
        number_of_sixes_rank_tuple=item['stats']
    for item in percent_boundary_rank_cursor:
        percent_boundary_rank_tuple=item['stats']
    for item in percent_dots_rank_cursor:
        percent_dots_rank_tuple=item['stats']

    return ind_bat_stats_tuple,ind_bowl_stats_tuple,ind_field_stats_tuple,highest_score_rank_tuple,\
       highest_str_rank_tuple,most_matches_rank_tuple,most_runs_rank_tuple,number_of_fours_rank_tuple,\
       number_of_sixes_rank_tuple,percent_boundary_rank_tuple,percent_dots_rank_tuple

def get_player_stats(key,ind_bat_stats_tuple,ind_bowl_stats_tuple,ind_field_stats_tuple,highest_score_rank_tuple,\
       highest_str_rank_tuple,most_matches_rank_tuple,most_runs_rank_tuple,number_of_fours_rank_tuple,\
       number_of_sixes_rank_tuple,percent_boundary_rank_tuple,percent_dots_rank_tuple):
    for item in ind_bat_stats_tuple:
        print (item[key] for item in ind_bat_stats_tuple)
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
    #print(ind_bowl_stats_tuple[key])
    #print(ind_field_stats_tuple[key])