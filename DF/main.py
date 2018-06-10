import os
import json
import yaml
import traceback
import sys
import time
import configparser as cp
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date, datetime
from pandas.io.json import json_normalize
from stats_df_logger import logger
from Utility import setup_mongo_client,remove_dots,load_all_data_from_db

def read_config():
    try:
        logger.info("Process Start")
        config = cp.ConfigParser()
        logger.info("Reading Config")
        config_data=config.read("config.ini")
        if len(config_data)!=1:
            directory_in_str = ''
            load_data = ''
            directory = ''
            raise ValueError
        else:
            directory_in_str = config['BASIC']['yaml_directory']
            load_data = config['BASIC']['load_data']
            generate_stats = config['BASIC']['generate_stats']
            logger.info("Data Directory - %s", directory_in_str)
            directory = os.fsencode(directory_in_str)
    except ValueError:
        logger.error("Config File Not found"+traceback.format_exc())
        sys.exit(-1)
    except KeyError:
        logger.error("Config File Not in the expected format"+traceback.format_exc())
        sys.exit(-1)
    return directory_in_str,load_data,generate_stats,directory

directory_in_str,load_data,generate_stats,directory=read_config()

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def append_general_match_info(match_summary_2_df,match_df):
    if 'info.city' in match_df:
        match_summary_2_df.loc[:, 'City'] = match_df['info.city'].values
    else:
        match_summary_2_df.loc[:, 'City'] ='Unknown'
    match_summary_2_df.loc[:, 'Competition'] = match_df['info.competition'].values
    [date_match] = match_df['info.dates']
    match_summary_2_df.loc[:, 'Year'] = pd.to_datetime(date_match[0], infer_datetime_format=True).year
    match_summary_2_df.loc[:, 'Month'] = pd.to_datetime(date_match[0], infer_datetime_format=True).month
    match_summary_2_df.loc[:, 'Day'] = pd.to_datetime(date_match[0], infer_datetime_format=True).day
    match_summary_2_df.loc[:, 'Type'] = match_df['info.match_type'].values
    if 'info.outcome.by.runs' in match_df:
        match_summary_2_df.loc[:, 'Outcome_Runs'] = match_df['info.outcome.by.runs'].values
    if 'info.outcome.by.wickets' in match_df:
        match_summary_2_df.loc[:, 'Outcome_Runs'] = match_df['info.outcome.by.wickets'].values
    if 'info.outcome.winner' in match_df:
        match_summary_2_df.loc[:, 'Winner'] = match_df['info.outcome.winner'].values
    match_summary_2_df.loc[:, 'Overs'] = match_df['info.overs'].values
    if 'info.player_of_match' in match_df:
        match_summary_2_df.loc[:, 'MOM'] = match_df['info.player_of_match'][0]
    match_summary_2_df.loc[:, 'Team1'] = (match_df['info.teams'][0][0])
    match_summary_2_df.loc[:, 'Team2'] = (match_df['info.teams'][0][1])
    match_summary_2_df.loc[:, 'Toss_Winner'] = match_df['info.toss.winner'].values
    match_summary_2_df.loc[:, 'Toss_Decision'] = match_df['info.toss.decision'].values
    if 'info.umpires' in match_df:
        match_summary_2_df.loc[:, 'Umpire_1'] = (match_df['info.umpires'][0][0])
        match_summary_2_df.loc[:, 'Umpire_2'] = match_df['info.umpires'][0][1]
    else:
        match_summary_2_df.loc[:, 'Umpire_1'] ='Unknown'
        match_summary_2_df.loc[:, 'Umpire_2'] = 'Unknown'

    match_summary_2_df.loc[:, 'Venue'] = match_df['info.venue'].values
    return match_summary_2_df

def initialize_lists():
    innings_list = []
    bat_team_list = []
    delivery_list = []
    batsman_facing_list = []
    bowler_list = []
    non_striker_list = []
    runs_batsman_list = []
    runs_extras_list = []
    runs_total_list = []
    wicket_fielder_list = []
    wicket_kind_list = []
    player_out_list = []
    all_lists=[innings_list,bat_team_list,delivery_list,batsman_facing_list,bowler_list,non_striker_list
        ,runs_batsman_list,runs_extras_list,runs_total_list,wicket_fielder_list,wicket_kind_list,player_out_list]
    return all_lists

def populate_innings_list(innings_details,lists,innings,bat_team):
    for item in innings_details:
        for key, value in item.items():
            if innings == 1:
                lists[0].append(1)
            else:
                lists[0].append(2)
            lists[1].append(bat_team)
            lists[2].append(key)
            lists[3].append(value['batsman'])
            lists[4].append(value['bowler'])
            lists[5].append(value['non_striker'])
            lists[6].append(value['runs']['batsman'])
            lists[7].append(value['runs']['extras'])
            lists[8].append(value['runs']['total'])
            if 'wicket' in value:
                # match_summary_df['Wicket_Fielder'] = value['wicket']['fielders'][0]
                if 'fielders' in value['wicket']:
                    lists[9].append(value['wicket']['fielders'][0])
                    lists[10].append(value['wicket']['kind'])
                    lists[11].append(value['wicket']['player_out'])
                else:
                    lists[9].append(np.nan)
                    lists[10].append(value['wicket']['kind'])
                    lists[11].append(value['wicket']['player_out'])
            else:
                lists[9].append(np.nan)
                lists[10].append(np.nan)
                lists[11].append(np.nan)
    return lists

def create_df_from_lists(match_summary_2_df,updated_list_sec_innings):
    match_summary_2_df['innings'] = updated_list_sec_innings[0]
    match_summary_2_df['Team_batting'] = updated_list_sec_innings[1]
    match_summary_2_df['Delivery'] = updated_list_sec_innings[2]
    match_summary_2_df['Batsman_facing'] = updated_list_sec_innings[3]
    match_summary_2_df['Bowler'] = updated_list_sec_innings[4]
    match_summary_2_df['Non_Striker'] = updated_list_sec_innings[5]
    match_summary_2_df['Batsman_Runs'] = updated_list_sec_innings[6]
    match_summary_2_df['Extras_Runs'] = updated_list_sec_innings[7]
    match_summary_2_df['Total_Runs'] = updated_list_sec_innings[8]
    match_summary_2_df['Wicket_Fielder'] = updated_list_sec_innings[9]
    match_summary_2_df['Wicket_Kind'] = updated_list_sec_innings[10]
    match_summary_2_df['Player_Out'] = updated_list_sec_innings[11]
    return match_summary_2_df

def obtain_raw_dataframe(filename):
    with open(filename, 'r') as stream:
        json_obj = json.dumps(yaml.load(stream), default=json_serial, indent=4)
        match_json = json_normalize(json.loads(json_obj))
        match_df = pd.DataFrame(match_json)
    return match_df


def process_files(file):
    try:
        print(file)
        match_summary_df=pd.DataFrame()
        match_summary_2_df = pd.DataFrame()
        filename = os.fsdecode(file)
        filename = directory_in_str + filename
        match_df=obtain_raw_dataframe(filename)
        innings_v = match_df['innings'][0][0].values()
        if len(match_df['innings'][0])==2:
            innings_2_v = match_df['innings'][0][1].values()

        all_lists=initialize_lists()
        updated_list=populate_innings_list(list(innings_v)[0]['deliveries'],all_lists,1,list(innings_v)[0]['team'])
        updated_list_sec_innings=updated_list
        if len(match_df['innings'][0])==2:
            updated_list_sec_innings = populate_innings_list(list(innings_2_v)[0]['deliveries'], updated_list,2,list(innings_2_v)[0]['team'])
        match_summary_2_df=create_df_from_lists(match_summary_2_df,updated_list_sec_innings)
        match_summary_2_df=append_general_match_info(match_summary_2_df,match_df)
    except:
        print("Error",file,traceback.format_exc())
        pass
    return match_summary_2_df

if load_data=='true':
    client=setup_mongo_client()#Create a connection handler
    db = client.IPL_DataFrames#Connect to the database
    collection = db.all_match_summary #Connect to the table
    all_matches_df_list = []
    if len(os.listdir(directory)) != 0:
        all_matches_df_list=[process_files(file) for file in os.listdir(directory)]
        #result = pd.concat(all_matches_df,ignore_index=True)
        #result.reset_index(drop=True)
    #print(result)
    #doc=json.loads(result.to_json())
    for item in all_matches_df_list:
        result_dict=item.to_dict()
    #print(result_dict)
        doc={'stats':result_dict}
    #print(json.loads(json.dumps(doc)))
        post_id = collection.insert(json.loads(json.dumps(doc),object_hook=remove_dots))
if generate_stats=='true':
    start_time = time.time()
    match_info_dict_list=[]
    #post_id = collection.insert(json.loads(json.dumps(doc),object_hook=remove_dots))
    #print(post_id)
    #result.to_csv('test.csv', sep='\t', encoding='utf-8', index=False)
    match_info_cursor=load_all_data_from_db('IPL_DataFrames','all_match_summary')
    for item in match_info_cursor:
        match_info_dict_list.append(item['stats'])

    all_matches_df=[pd.DataFrame.from_dict(item) for item in match_info_dict_list]
    result = pd.concat(all_matches_df,ignore_index=True)
    #------------------Overall Batsman Stats/Ranking -------------------------------#
    #------------------Most Runs in IPL History-----------------------#
    '''highest_scores_rank=result.groupby(['Batsman_facing'])[["Batsman_Runs"]].sum().\
        sort_values(['Batsman_Runs'],ascending=False)
    highest_scores_rank['Batsman_facing'] = highest_scores_rank.index
    highest_scores_rank=highest_scores_rank.reset_index(drop=True)
    # ------------------Most Balls Faced in IPL History-----------------------#
    number_of_balls_faced=result.groupby(['Batsman_facing'])[["Batsman_Runs"]].count().\
        sort_values(['Batsman_Runs'],ascending=False)
    number_of_balls_faced['Batsman_facing'] = number_of_balls_faced.index
    number_of_balls_faced = number_of_balls_faced.reset_index(drop=True)
    # ------------------Most Sixes Hit in IPL History-----------------------#
    number_of_sixes=result[result.Batsman_Runs==6].groupby(['Batsman_facing'])[["Batsman_Runs"]].count().\
        sort_values(['Batsman_Runs'],ascending=False)
    number_of_sixes['Batsman_facing'] = number_of_sixes.index
    number_of_sixes = number_of_sixes.reset_index(drop=True)
    # ------------------Most Fours Hit in IPL History-----------------------#
    number_of_fours = result[result.Batsman_Runs == 4].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_fours['Batsman_facing'] = number_of_fours.index
    number_of_fours = number_of_fours.reset_index(drop=True)
    # ------------------Most Boundary balls hit in IPL History-----------------------#
    number_of_boundaries = result[(result.Batsman_Runs == 4) | (result.Batsman_Runs == 6)].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_boundaries['Batsman_facing'] = number_of_boundaries.index
    number_of_boundaries = number_of_boundaries.reset_index(drop=True)
    # ------------------Most Number of Runs scored in boundaries in IPL History-----------------------#
    number_of_runs_boundaries = result[(result.Batsman_Runs == 4) | (result.Batsman_Runs == 6)].groupby(['Batsman_facing'])[["Batsman_Runs"]].sum(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_runs_boundaries['Batsman_facing'] = number_of_runs_boundaries.index
    number_of_runs_boundaries = number_of_runs_boundaries.reset_index(drop=True)
    # ------------------Most Dots in IPL History-----------------------#
    number_of_dots = result[result.Batsman_Runs == 0].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_dots['Batsman_facing'] = number_of_dots.index
    number_of_dots = number_of_dots.reset_index(drop=True)
    # ------------------Most Dot % in IPL History (For batsman who have scored <1500 runs-----------------------#
    number_of_dots_percent = number_of_balls_faced.merge(number_of_dots,on='Batsman_facing',how='inner')
    number_of_dots_percent['Dot_percent']= number_of_dots_percent.Batsman_Runs_y/number_of_dots_percent.Batsman_Runs_x
    number_of_dots_percent=(number_of_dots_percent[number_of_dots_percent.Batsman_Runs_x>1500][['Batsman_facing','Dot_percent']]
          .sort_values(['Dot_percent'], ascending=True))
    number_of_dots_percent=number_of_dots_percent.reset_index(drop=True)
    # ------------------Most Boundary balls % in IPL History (For batsman who have scored <1000 runs-----------------------#
    number_of_boundary_balls_percent = number_of_balls_faced.merge(number_of_boundaries,on='Batsman_facing',how='inner')
    number_of_boundary_balls_percent['Boundary_percent']= number_of_boundary_balls_percent.Batsman_Runs_y/number_of_boundary_balls_percent.Batsman_Runs_x
    number_of_boundary_percent=(number_of_boundary_balls_percent[number_of_boundary_balls_percent.Batsman_Runs_x>1000][['Batsman_facing','Boundary_percent']]
          .sort_values(['Boundary_percent'], ascending=False))
    number_of_boundary_percent=number_of_boundary_percent.reset_index(drop=True)
    # ------------------Most Boundary Runs % in IPL History (For batsman who have scored >1000 runs-----------------------#
    number_of_boundary_runs_percent = highest_scores_rank.merge(number_of_runs_boundaries,on='Batsman_facing',how='inner')
    number_of_boundary_runs_percent['Boundary_percent']= number_of_boundary_runs_percent.Batsman_Runs_y/number_of_boundary_runs_percent.Batsman_Runs_x
    number_of_boundary_runs_percent=(number_of_boundary_runs_percent[number_of_boundary_runs_percent.Batsman_Runs_x>1000][['Batsman_facing','Boundary_percent']]
          .sort_values(['Boundary_percent'], ascending=False))
    number_of_boundary_runs_percent=number_of_boundary_runs_percent.reset_index(drop=True)
    #------------------------Batsmen Powerplay Stats --------------------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------POWER PLAYS  ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    #   Most Runs in IPL Powerplay history  #
    #print(result.columns)
    highest_pp_scores_rank=result[(pd.to_numeric(result.Delivery)>0) & (pd.to_numeric(result.Delivery)<6)]
    highest_pp_scores_rank=highest_pp_scores_rank.groupby(['Batsman_facing'])[["Batsman_Runs"]].sum().\
        sort_values(['Batsman_Runs'],ascending=False)
    highest_pp_scores_rank['Batsman_facing'] = highest_pp_scores_rank.index
    highest_pp_scores_rank = highest_pp_scores_rank.reset_index(drop=True)
    # ------------------Most Balls Faced PP in IPL History-----------------------#
    result_power_play = result[(pd.to_numeric(result.Delivery) > 0) & (pd.to_numeric(result.Delivery) < 6)]
    number_of_pp_balls_faced=result_power_play.groupby(['Batsman_facing'])[["Batsman_Runs"]].count().\
        sort_values(['Batsman_Runs'],ascending=False)
    number_of_pp_balls_faced['Batsman_facing'] = number_of_pp_balls_faced.index
    number_of_pp_balls_faced = number_of_pp_balls_faced.reset_index(drop=True)
    #------------------ Most Dots in PP -----------------------------
    result_power_play = result[(pd.to_numeric(result.Delivery) > 0) & (pd.to_numeric(result.Delivery) < 6)]
    number_of_pp_dots = result_power_play[result_power_play.Batsman_Runs == 0].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_pp_dots['Batsman_facing'] = number_of_pp_dots.index
    number_of_pp_dots = number_of_pp_dots.reset_index(drop=True)
    # ------------------Most Dot % in IPL History (For batsman who have scored <1500 runs-----------------------#
    number_of_pp_dots_percent = number_of_pp_balls_faced.merge(number_of_pp_dots, on='Batsman_facing', how='inner')
    number_of_pp_dots_percent[
        'Dot_percent'] = number_of_pp_dots_percent.Batsman_Runs_y / number_of_pp_dots_percent.Batsman_Runs_x
    number_of_pp_dots_percent = (
        number_of_pp_dots_percent[number_of_pp_dots_percent.Batsman_Runs_x > 500][['Batsman_facing', 'Dot_percent']]
        .sort_values(['Dot_percent'], ascending=True))
    number_of_pp_dots_percent = number_of_pp_dots_percent.reset_index(drop=True)
    #---------------------Highest ever scores in PP --------------------------------
    high_pp_score_match=result_power_play.groupby(['Year','Month','Day','Batsman_facing'])[["Batsman_Runs"]].sum()
    high_pp_score_match=high_pp_score_match.sort_values('Batsman_Runs', ascending=False)
    #-------Highest strike rate in PP -----------------------------
    most_pp_runs = result_power_play.groupby(['Batsman_facing'])[["Batsman_Runs"]].sum()
    most_pp_runs['Batsman_facing'] = most_pp_runs.index
    most_pp_runs = most_pp_runs.reset_index(drop=True)
    pp_strike_rate = most_pp_runs.merge(number_of_pp_balls_faced, on='Batsman_facing', how='inner')
    pp_strike_rate['Strike_Rate']=(pp_strike_rate.Batsman_Runs_x/pp_strike_rate.Batsman_Runs_y)*100
    pp_strike_rate=(pp_strike_rate[pp_strike_rate.Batsman_Runs_x>20][['Batsman_facing', 'Strike_Rate','Batsman_Runs_x']].
                    sort_values(['Strike_Rate'], ascending=False))
    pp_strike_rate = pp_strike_rate.reset_index(drop=True)
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------MIDDLE OVERS ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    result_middle_overs = result[(pd.to_numeric(result.Delivery) > 6) & (pd.to_numeric(result.Delivery) < 16)]
    highest_mdl_scores_rank = result_middle_overs.groupby(['Batsman_facing'])[["Batsman_Runs"]].sum(). \
        sort_values(['Batsman_Runs'], ascending=False)
    highest_mdl_scores_rank['Batsman_facing'] = highest_mdl_scores_rank.index
    number_of_mdl_balls_faced = result_middle_overs.groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_mdl_balls_faced['Batsman_facing'] = number_of_mdl_balls_faced.index
    number_of_mdl_dots = result_middle_overs[result_middle_overs.Batsman_Runs == 0].groupby(['Batsman_facing'])[
        ["Batsman_Runs"]].count().sort_values(['Batsman_Runs'], ascending=False)
    number_of_mdl_dots['Batsman_facing'] = number_of_mdl_dots.index
    number_of_mdl_boundaries = result_middle_overs[(result_middle_overs.Batsman_Runs == 4) |
            (result_middle_overs.Batsman_Runs == 6)].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_mdl_boundaries['Batsman_facing'] = number_of_mdl_boundaries.index
    mdl_strike_rotate = result_middle_overs[(result_middle_overs.Batsman_Runs == 1) |
            (result_middle_overs.Batsman_Runs == 2)].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    mdl_strike_rotate['Batsman_facing'] = mdl_strike_rotate.index
    # Strike Rate
    mdl_strike_rate = highest_mdl_scores_rank.merge(number_of_mdl_balls_faced, on='Batsman_facing', how='inner')
    mdl_strike_rate['Strike_Rate'] = (mdl_strike_rate.Batsman_Runs_x / mdl_strike_rate.Batsman_Runs_y) * 100
    mdl_strike_rate = (
        mdl_strike_rate[mdl_strike_rate.Batsman_Runs_x > 250][['Batsman_facing', 'Strike_Rate', 'Batsman_Runs_x']].
        sort_values(['Strike_Rate'], ascending=False))
    mdl_strike_rate = mdl_strike_rate.reset_index(drop=True)
    #Dots %
    mdl_dots_percent = number_of_mdl_balls_faced.merge(number_of_mdl_dots, on='Batsman_facing', how='inner')
    mdl_dots_percent[
        'Dot_percent'] = (mdl_dots_percent.Batsman_Runs_y / mdl_dots_percent.Batsman_Runs_x)*100
    mdl_dots_percent = (
        mdl_dots_percent[mdl_dots_percent.Batsman_Runs_x > 500][['Batsman_facing', 'Dot_percent']]
            .sort_values(['Dot_percent'], ascending=True))
    mdl_dots_percent = mdl_dots_percent.reset_index(drop=True)
    # Boundary %
    mdl_boundary_percent = number_of_mdl_balls_faced.merge(number_of_mdl_boundaries, on='Batsman_facing', how='inner')
    mdl_boundary_percent[
        'Boundary_percent'] = (mdl_boundary_percent.Batsman_Runs_y / mdl_boundary_percent.Batsman_Runs_x)*100
    mdl_boundary_percent = (
        mdl_boundary_percent[mdl_boundary_percent.Batsman_Runs_x > 500][
            ['Batsman_facing', 'Boundary_percent']]
        .sort_values(['Boundary_percent'], ascending=False))
    mdl_boundary_percent = mdl_boundary_percent.reset_index(drop=True)
    #Strike Rotates best
    mdl_str_rotate_percent = number_of_mdl_balls_faced.merge(mdl_strike_rotate, on='Batsman_facing', how='inner')
    mdl_str_rotate_percent[
        'Rotate_percent'] = (mdl_str_rotate_percent.Batsman_Runs_y / mdl_str_rotate_percent.Batsman_Runs_x) * 100
    mdl_str_rotate_percent = (
        mdl_str_rotate_percent[mdl_str_rotate_percent.Batsman_Runs_x > 500][
            ['Batsman_facing', 'Rotate_percent']]
            .sort_values(['Rotate_percent'], ascending=False))
    mdl_str_rotate_percent = mdl_str_rotate_percent.reset_index(drop=True)
    #print(mdl_str_rotate_percent)
    print("--- %s seconds ---" % (time.time() - start_time))
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------DEATH  OVERS ------------------------------#
    #----------------------------             ------------------------------#
    #----------------------------             ------------------------------#
    result_death_overs = result[(pd.to_numeric(result.Delivery) > 16) & (pd.to_numeric(result.Delivery) < 20)]
    highest_dth_scores_rank = result_death_overs.groupby(['Batsman_facing'])[["Batsman_Runs"]].sum(). \
        sort_values(['Batsman_Runs'], ascending=False)
    highest_dth_scores_rank['Batsman_facing'] = highest_dth_scores_rank.index
    number_of_dth_balls_faced = result_death_overs.groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_dth_balls_faced['Batsman_facing'] = number_of_dth_balls_faced.index
    number_of_dth_dots = result_death_overs[result_death_overs.Batsman_Runs == 0].groupby(['Batsman_facing'])[
        ["Batsman_Runs"]].count().sort_values(['Batsman_Runs'], ascending=False)
    number_of_dth_dots['Batsman_facing'] = number_of_dth_dots.index
    number_of_dth_boundaries = result_death_overs[(result_death_overs.Batsman_Runs == 4) |
            (result_death_overs.Batsman_Runs == 6)].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    number_of_dth_boundaries['Batsman_facing'] = number_of_dth_boundaries.index
    dth_strike_rotate = result_death_overs[(result_death_overs.Batsman_Runs == 1) |
            (result_death_overs.Batsman_Runs == 2)].groupby(['Batsman_facing'])[["Batsman_Runs"]].count(). \
        sort_values(['Batsman_Runs'], ascending=False)
    dth_strike_rotate['Batsman_facing'] = dth_strike_rotate.index
    # Strike Rate
    dth_strike_rate = highest_dth_scores_rank.merge(number_of_dth_balls_faced, on='Batsman_facing', how='inner')
    dth_strike_rate['Strike_Rate'] = (dth_strike_rate.Batsman_Runs_x / dth_strike_rate.Batsman_Runs_y) * 100
    dth_strike_rate = (
        dth_strike_rate[dth_strike_rate.Batsman_Runs_x > 250][['Batsman_facing', 'Strike_Rate', 'Batsman_Runs_x']].
        sort_values(['Strike_Rate'], ascending=False))
    dth_strike_rate = dth_strike_rate.reset_index(drop=True)
    #Dots %
    dth_dots_percent = number_of_dth_balls_faced.merge(number_of_dth_dots, on='Batsman_facing', how='inner')
    dth_dots_percent[
        'Dot_percent'] = (dth_dots_percent.Batsman_Runs_y / dth_dots_percent.Batsman_Runs_x)*100
    dth_dots_percent = (
        dth_dots_percent[dth_dots_percent.Batsman_Runs_x > 250][['Batsman_facing', 'Dot_percent']]
            .sort_values(['Dot_percent'], ascending=True))
    dth_dots_percent = dth_dots_percent.reset_index(drop=True)
    # Boundary %
    dth_boundary_percent = number_of_dth_balls_faced.merge(number_of_dth_boundaries, on='Batsman_facing', how='inner')
    dth_boundary_percent[
        'Boundary_percent'] = (dth_boundary_percent.Batsman_Runs_y / dth_boundary_percent.Batsman_Runs_x)*100
    dth_boundary_percent = (
        dth_boundary_percent[dth_boundary_percent.Batsman_Runs_x > 100][
            ['Batsman_facing', 'Boundary_percent']]
        .sort_values(['Boundary_percent'], ascending=False))
    dth_boundary_percent = dth_boundary_percent.reset_index(drop=True)
    #Strike Rotates best
    dth_str_rotate_percent = number_of_dth_balls_faced.merge(dth_strike_rotate, on='Batsman_facing', how='inner')
    dth_str_rotate_percent[
        'Rotate_percent'] = (dth_str_rotate_percent.Batsman_Runs_y / dth_str_rotate_percent.Batsman_Runs_x) * 100
    dth_str_rotate_percent = (
        dth_str_rotate_percent[dth_str_rotate_percent.Batsman_Runs_x > 100][
            ['Batsman_facing', 'Rotate_percent']]
            .sort_values(['Rotate_percent'], ascending=False))
    dth_str_rotate_percent = dth_str_rotate_percent.reset_index(drop=True)
    print(dth_strike_rate)
    print("--- %s seconds ---" % (time.time() - start_time))'''
    #print(result[result.Year.isnull()])

    result['Unique_Match_Id']=result['Day'].map(str)+result['Month'].map(str)+result['Year'].map(str)+result['City']+result['Team1']
    result_b4_death_overs = result[(pd.to_numeric(result.Delivery) > 0) & (pd.to_numeric(result.Delivery) < 16)]
    result_death_overs = result[(pd.to_numeric(result.Delivery) > 16) & (pd.to_numeric(result.Delivery) < 20)]
    scores_death_overs=result_death_overs.groupby(['Unique_Match_Id','innings'])[["Total_Runs"]].sum()
    scores_death_overs['Unique_Match_Id'] = scores_death_overs.index
    scores_death_overs['innings'] = scores_death_overs.index
    scores_death_overs = scores_death_overs.reset_index(drop=True)

    wickets_b4_death=result_b4_death_overs.groupby(['Unique_Match_Id','innings'])[["Player_Out"]].count()
    wickets_b4_death['Unique_Match_Id'] = wickets_b4_death.index
    wickets_b4_death['innings'] = wickets_b4_death.index
    wickets_b4_death = wickets_b4_death.reset_index(drop=True)

    merged_result = scores_death_overs.merge(wickets_b4_death, on='Unique_Match_Id', how='inner')
    #merged_result.to_csv('test1.csv', sep=';', encoding='utf-8', index=False)
    merged_result = merged_result.drop(['Unique_Match_Id','innings_x','innings_y'], 1)
    merged_result=merged_result.groupby(['Player_Out'])[["Total_Runs"]].median().round()
    merged_result['Player_Out'] = merged_result.index
    merged_result = merged_result.reset_index(drop=True)
    print(merged_result)
    #merged_result[['Player_Out', 'Total_Runs']].plot()
    #plt.show()

    #print(merged_result)
    '''plt.figure(figsize=(12, 9))

    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.xticks(fontsize=14)
    plt.yticks(range(5000, 30001, 5000), fontsize=14)

    plt.hist(merged_result)

    plt.xlabel("No Of Wickets", fontsize=16)
    plt.ylabel("Mean Score in Overs 16-20", fontsize=16)

    plt.text(1300, -5000, "Data source: www.cricsheets.com | "
                          , fontsize=10)

    #print(merged_result)
    #high_ever_scores=result.groupby(['Unique_Match_Id','Batsman_facing'])[["Batsman_Runs"]].sum(). \
        #sort_values(['Batsman_Runs'], ascending=False)
    #print(result.columns)
    plt.show()'''