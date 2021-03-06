import configparser as cp
import os
import sys
import json
from stats_logger import logger
from datetime import date, datetime
import pymongo
import traceback
from pymongo import MongoClient

#Sets up the client for the mongo db. Port name is hardcoded to 27017
def setup_mongo_client():
    try:
        logger.info("Inside setup_mongo_client Method")
        client = MongoClient()
        client = MongoClient('localhost', 27017)
    except:
        logger.error("Exception occurred while setting up Mongo client"+traceback.format_exc())
    return client

def insert_docs_to_tables(database,table,doc):
    client = setup_mongo_client()
    try:
        db = client[database]
    except:
        logger.error("Exception occurred while connecting to database" + traceback.format_exc())
    if (db):
        try:
            collection = db[table]
        except:
            logger.error("Exception occurred while connecting to the table" + traceback.format_exc())
    #print (json.loads(json.dumps(doc)))
    post_id = collection.insert_one(json.loads(json.dumps(doc), object_hook=remove_dots))
    return post_id

def create_documets_for_storing(bat_inns, bowl_inns, wickets_inns, field_inns,bat_pp,bat_middle,bat_death,post_id):
    logger.info("Inside create_documets_for_storing Method")
    bat_doc = {"_id": str(post_id), "stats": bat_inns}#Linking the stats with the inserted match Id
    bowl_doc = {"_id": str(post_id), "stats": bowl_inns}
    wickets_doc = {"_id": str(post_id), "stats": wickets_inns}
    field_doc = {"_id": str(post_id), "stats": field_inns}
    bat_pp_doc = {"_id": str(post_id), "stats": bat_pp}
    bat_mdl_doc = {"_id": str(post_id), "stats": bat_middle}
    bat_death_doc = {"_id": str(post_id), "stats": bat_death}
    return bat_doc,bowl_doc,wickets_doc,field_doc,bat_pp_doc,bat_mdl_doc,bat_death_doc

#Function to remove the . in the keys and replace them with - . Key values cannot have '.'
def remove_dots(obj):
    #logger.info("Inside Remove Dots Method")
    for key in obj.keys():
        new_key = key.replace(".","-")
        if new_key != key:
            obj[new_key] = obj[key]
            del obj[key]
    return obj

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

#This method reads all the parameters from the config file and returns them as variables
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

#This method returns from the specified db and table all the documents as a cursor
def load_all_data_from_db(database,table):
    try:
        client = setup_mongo_client()
        db = client[database]
        if (db):
            collection = db[table]
        if(collection):
            cursor = collection.find({})
    except:
        logger.error("Input directory is empty. Read data from database"+traceback.format_exc())
        sys.exit(-1)
    return cursor