import yaml
import json
import pandas as pd
import shutil
import sys
from pandas.io.json import json_normalize
from datetime import date, datetime
import os
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
import pymongo
from pymongo import MongoClient
import pprint

directory_in_str="C:\\Users\\Siddi\\PycharmProjects\\IPL_Prediction\\Data\\Yaml\\"
directory = os.fsencode(directory_in_str)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

#Function to remove the . in the keys and replace them with - . Key values cannot have '.'
def remove_dots(obj):
    for key in obj.keys():
        new_key = key.replace(".","-")
        if new_key != key:
            obj[new_key] = obj[key]
            del obj[key]
    return obj

def get_df_from_yaml(filename):
    match_info_df=pd.DataFrame()
    if filename.endswith(".yaml"):
        print(filename)
        filename = directory_in_str + filename
        with open(filename, 'r') as stream:
            try:
                json_obj = json.dumps(yaml.load(stream), default=json_serial, indent=4)
                match_info = json_normalize(json.loads(json_obj)["info"])
                match_info = pd.DataFrame(match_info)
                match_info_dict=match_info.to_dict()
            except yaml.YAMLError as exc:
                    print(exc)
        return match_info_dict

def get_consolidated_match_Stats(score_batsmen,score_bowler,wickets_bowler,fielding_details):
    #Function to summarise the stats
    try:
        batsmen_score_total = {}
        bowler_score_total={}
        wickets_bowler_total={}
        fielding_details_total={}
        #Runs Scored/Conceded,Balls Faced/bowled,Str Rate/Eco Rate,No of Dots played/faced,No of 4s,No of 6s,% of Dots,"
                 # "% of balls in Boundaries,% of runs in Boundaries
        for key, value in score_batsmen.items():
            try:
                batsmen_score_total[key] = [sum(value), len(value), "%.1f" % round((sum(value) * 100 / len(value)), 1),
                                            value.count(0), value.count(4), value.count(6),
                                            round(value.count(0) * 100 / len(value), 1),
                                            round((value.count(4) + value.count(6)) * 100 / len(value), 1),
                                            round((value.count(4) * 4 + value.count(6) * 6) * 100 / sum(value), 1)]
            except ZeroDivisionError:
                batsmen_score_total[key] = [sum(value), len(value), "%.1f" % round((sum(value) * 100 / len(value)), 1),
                                            value.count(0), value.count(4), value.count(6),
                                            round(value.count(0) * 100 / len(value), 1),
                                            round((value.count(4) + value.count(6)) * 100 / len(value), 1),
                                            0]#Zero division error while caclulating % of boundary in score
                # print (batsmen_score_total)
        for key, value in score_bowler.items():
            try:
                bowler_score_total[key] = [sum(value), len(value), "%.1f" % round((sum(value) * 6 / len(value)), 1),
                                           value.count(0), value.count(4), value.count(6),
                                           round(value.count(0) * 100 / len(value), 1),
                                           round((value.count(4) + value.count(6)) * 100 / len(value), 1),
                                           round((value.count(4) * 4 + value.count(6) * 6) * 100 / sum(value), 1)]
            except ZeroDivisionError:
                bowler_score_total[key] = [sum(value), len(value), "%.1f" % round((sum(value) * 6 / len(value)), 1),
                                           value.count(0), value.count(4), value.count(6),
                                           round(value.count(0) * 100 / len(value), 1),
                                           round((value.count(4) + value.count(6)) * 100 / len(value), 1),
                                           0]#Zero division error while caclulating % of boundary balls
        # print(bowler_score_total)
        for key, value in wickets_bowler.items():
            wickets_bowler_total[key] = [len(value), len(score_bowler[key]) / len(value)]  # No of Wickets, Strike Rate
        # print(wickets_bowler_total)
        for key, value in fielding_details.items():
            fielding_details_total[key] = int(len(value) / 2)  # No of Catches
        # print(fielding_details_total)
        # stats_list=[batsmen_score_total,bowler_score_total,wickets_bowler_total,fielding_details_total]
    except:
        print("exception Occurred")
    return batsmen_score_total,bowler_score_total,wickets_bowler_total,fielding_details_total

def get_score_from_yaml(innings,filename):
    #This function is called per innings. Twice per match.
    #Return values are used by get_consolidated_match_Stats to summarise the details
    match_info_df=pd.DataFrame()
    #source = os.listdir("C:\\Users\\Siddi\\PycharmProjects\\IPL_Prediction\\Data\\Yaml")
    #destination = "C:\\Users\\Siddi\\PycharmProjects\\IPL_Prediction\\Data\\Errors"
    filename = directory_in_str + filename
    with open(filename, 'r') as stream:
        try:
            #Initialize dictionaries to store the required stats.
            score_batsmen={}
            score_bowler={}
            wickets_bowler = {}
            fielding_details={}
            json_obj = json.dumps(yaml.load(stream), default=json_serial, indent=4)
            #Separates the info details from yaml and obtains only the ball by ball inns details
            # Contains the details of both the innings.
            fir_innings_info = json_normalize(json.loads(json_obj)["innings"])
            fir_innings_info = pd.DataFrame(fir_innings_info)
            if (innings==1):#Get first innings ball by ball details
                fir_inns_del=fir_innings_info.iloc[0,0]
            else:#Gets second innings ball by ball details
                fir_inns_del=fir_innings_info.iloc[1,2]
            #Details that are fetched
            #print("Runs Scored/Conceded,Balls Faced,Str Rate,No of Dots,No of 4s,No of 6s,% of Dots,"
                 # "% of balls in Boundaries,% of runs in Boundaries")
            for i in fir_inns_del:
                for key, value in i.items():
                    if value["batsman"] in score_batsmen:#value["batsman"] contains the name of the batsman
                        #If the name is present in dict already as a key then:
                        score_batsmen[value["batsman"]].append(value["runs"]["batsman"])
                        if "wicket" in value:
                            if value["wicket"]["kind"]!="run out" and value["wicket"]["kind"] != "retired hurt" \
                                    and value["wicket"]["kind"] != "obstructing the field":
                                #Credit the bowler for wicket only if the mode of dismissal is none of the above
                                if value["bowler"] in wickets_bowler:#If bowler name is present as a key in dict
                                    wickets_bowler[value["bowler"]].append(value["wicket"]["player_out"])
                                else: #Create the key with bowler name
                                    wickets_bowler[value["bowler"]] = [value["runs"]["total"]]
                                if (value["wicket"]["kind"] != "run out" and value["wicket"]["kind"] != "bowled"
                                        and value["wicket"]["kind"] != "lbw" and value["wicket"]["kind"] !=
                                        "caught and bowled" and value["wicket"]["kind"] != "retired hurt"
                                        and value["wicket"]["kind"] != "obstructing the field" and value["wicket"]["kind"]!="hit wicket"):
                                    #Credit the fielder if mode of dismissal is none of the above
                                    if value["wicket"]["fielders"][0] in fielding_details:
                                        fielding_details[value["wicket"]["fielders"][0]].append(value["wicket"]["player_out"])
                                        fielding_details[value["wicket"]["fielders"][0]].append(value["bowler"])
                                    else:
                                        fielding_details[value["wicket"]["fielders"][0]]= [value["runs"]["total"]]
                                        fielding_details[value["wicket"]["fielders"][0]].append(value["bowler"])
                    else:
                        score_batsmen[value["batsman"]] = [value["runs"]["batsman"]]
                    if value["bowler"] in score_bowler:#Runs conceded by the bowler
                        score_bowler[value["bowler"]].append(value["runs"]["total"])
                    else:
                        score_bowler[value["bowler"]] = [value["runs"]["total"]]
        except yaml.YAMLError as exc:
            print(exc)
        except:
            #shutil.copy(file, destination)
            print("Exception occured")
            print(file)
            pass
    return score_batsmen,score_bowler,wickets_bowler,fielding_details

def show_mom_details(df):
    man_of_match_list = []
    try:
        for val in (df.loc[:, 'player_of_match'].values):
            for item in val:
                man_of_match_list.append(item)
        mom_counts = Counter(man_of_match_list)
        df = pd.DataFrame.from_dict(mom_counts, orient='index')
        print (df)
        df = df[df.iloc[:, 0] > 0]
    except KeyError:
        man_of_match_list.append("None")
    except TypeError:
        man_of_match_list.append("None")
    return df

def setup_mongo_client():
    client = MongoClient()
    client = MongoClient('localhost', 27017)
    return client

def get_all_stats_of_match(filename):
    score_batsmen, score_bowler, wickets_bowler, fielding_details = get_score_from_yaml(1, filename)
    #Raw Stats of first innings
    bat_inns_one, bowl_inns_one, wickets_inns_one, field_inns_one = get_consolidated_match_Stats(score_batsmen,
                                                                                                 score_bowler,
                                                                                                 wickets_bowler,
                                                                                                 fielding_details)
    #Summary stats of first innings
    score_batsmen, score_bowler, wickets_bowler, fielding_details = get_score_from_yaml(2, filename)
    # Raw Stats of second innings
    bat_inns_two, bowl_inns_two, wickets_inns_two, field_inns_two = get_consolidated_match_Stats(score_batsmen,
                                                                                                 score_bowler,
                                                                                                 wickets_bowler,
                                                                                                 fielding_details)
    # Summary stats of second innings
    # doc_stats={"_id":post_id.inserted_id,"stats":stats_one}
    bat_inns = [bat_inns_one, bat_inns_two]  #Combine stats of both the innings to one bucket
    bowl_inns = [bowl_inns_one, bowl_inns_two]
    wickets_inns = [wickets_inns_one, wickets_inns_two]
    field_inns = [field_inns_one, field_inns_two]

    return bat_inns,bowl_inns,wickets_inns,field_inns

def create_documets_for_storing(bat_inns, bowl_inns, wickets_inns, field_inns,post_id):
    bat_doc = {"_id": str(post_id), "stats": bat_inns}#Linking the stats with the inserted match Id
    bowl_doc = {"_id": str(post_id), "stats": bowl_inns}
    wickets_doc = {"_id": str(post_id), "stats": wickets_inns}
    field_doc = {"_id": str(post_id), "stats": field_inns}
    return bat_doc,bowl_doc,wickets_doc,field_doc

for file in os.listdir(directory):
    print(file)
    filename = os.fsdecode(file)
    info_dict=get_df_from_yaml(filename)
    client=setup_mongo_client()#Create a connection handler
    db = client.test_db_2#Connect to the database
    collection = db.match_info #Connect to the table
    post_id = collection.insert_one(json.loads(json.dumps(info_dict),object_hook=remove_dots))
    bat_inns, bowl_inns, wickets_inns, field_inns=get_all_stats_of_match(filename)
    bat_doc, bowl_doc, wickets_doc, field_doc=create_documets_for_storing(bat_inns, bowl_inns, wickets_inns,
                                                                          field_inns,post_id.inserted_id)
    collection = db.bat_stats
    post_id_bat = collection.insert_one(json.loads(json.dumps(bat_doc), object_hook=remove_dots))
    collection = db.bowl_stats
    post_id_bowl = collection.insert_one(json.loads(json.dumps(bowl_doc), object_hook=remove_dots))
    collection = db.wickets_stats
    post_id_field = collection.insert_one(json.loads(json.dumps(wickets_doc), object_hook=remove_dots))
    collection = db.field_stats
    post_id_wickets = collection.insert_one(json.loads(json.dumps(field_doc), object_hook=remove_dots))
    #print(doc_stats)

