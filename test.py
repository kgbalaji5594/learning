import phoenixdb
from datetime import datetime, timedelta
import time
import json 
from decimal import Decimal
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import requests
from numpy import arccos, sin, cos, radians;
import ast
from dateutil import tz
import sys
import boto3

from recommendation_engine.utils.Utils import *
from recommendation_engine.clients import SQSClient, DynamoClient
from recommendation_engine.utils.LoggerImpl import *
logger = Logger.__call__().get_logger()

def partition_transformation(checkcalls):
    filtered_customers = get_customers_list()
    for each_checkcall in checkcalls:
        try:
            each_checkcall["shipperId"] = each_checkcall["shipperId"] if each_checkcall["shipperId"] else "NA"
            each_checkcall["carrierId"] = each_checkcall["carrierId"] if each_checkcall["carrierId"] else "NA"
            each_checkcall["tplId"] = each_checkcall["tplId"] if each_checkcall["tplId"] else "NA"
            if (each_checkcall["shipperId"] or each_checkcall["carrierId"] or each_checkcall["tplId"]) in filtered_customers and each_checkcall["loadStatus"] in conf['CHECK_CALL_TOPIC']['TRACKING_STATUS']:
                    each_checkcall["trackingId"] = str(each_checkcall["trackingId"])
                    logger.info({"method":"zone_computation:partition_transformation", "message" : "filtered checkcall", "data" : each_checkcall["trackingId"]})
                    company_zone_list = get_zones(each_checkcall["shipperId"],each_checkcall["carrierId"],each_checkcall["tplId"]) 
                    custom_zone_messages = zone_computation(each_checkcall, company_zone_list)
                    if custom_zone_messages:
                        logger.info({"method":"zone_computation:partition_transformation", "message" : "{} has zone".format(each_checkcall["trackingId"]), "data" : custom_zone_messages})
                        push_dynamo_message(custom_zone_messages[2])
                        logger.info({"method":"zone_computation:partition_transformation", "message" : "pushed dynamo messages".format(each_checkcall["trackingId"]), "data" : custom_zone_messages[2]})
                        push_sqs_message([custom_zone_messages[0], custom_zone_messages[1]])
                        logger.info({"method":"zone_computation:partition_transformation", "message" : "pushed sqs messages", "data" : [custom_zone_messages[0], custom_zone_messages[1]]})
        except Exception as error:
            logger.exception({"zone_computation:partition_transformation": "error", "error_details": error, "error_line_no" : sys.exc_info()[2].tb_lineno, "data" : each_checkcall})


def push_sqs_message(sqs_messages):
    obj_sqs_client = SQSClient.SQSClient()
    sqs_queues = conf["CUSTOM_ZONES"]["SQS_QUEUES"] 
    es_sync = False
    for message,queue in zip(sqs_messages,sqs_queues):
        if message or es_sync :
            if es_sync:
                logger.info({"method":"zone_computation:push_sqs_message", "message" : json.dumps(message), "queue" : queue})
                for each_message in message:
                    obj_sqs_client.send_message(conf, json.dumps(each_message), queue) 
            else:
                logger.info({"method":"zone_computation:push_sqs_message", "message" : json.dumps(message), "queue" : queue})
                obj_sqs_client.send_message(conf, json.dumps(message), queue)
            es_sync = True

def push_dynamo_message(message):
    dynamoClient = DynamoClient.DynamoClient()
    dynamo_conn, table = dynamoClient.create_dynamo_connection(conf, conf["CUSTOM_ZONES"]["DYNAMO_DB"])
    if "zones" in message.keys() and len(message["zones"]):
        json_dump_record = json.dumps({"tracking_id" : int(message["TrackingId"]), "company_permalink" : message["CompanyPermalink"], "zones" : message["zones"], "expired_at" : int(time.mktime((datetime.now() + timedelta(days=conf['CUSTOM_ZONES']['DYNAMO_DB_TTL'])).timetuple()))}, cls=DecimalEncoder)
        decimal_dynamo_record = json.loads(json_dump_record, parse_float=Decimal)
        table.put_item(Item = decimal_dynamo_record)

def phoenix_query_action(query,value,is_return,query_type):
    try:
        phoenix_conn, phoenix_cursor = get_phoenix_cursor()
        if query_type == "upsert":
            phoenix_cursor.execute(query,value)
        else:
            phoenix_cursor.execute(query % value) if value else phoenix_cursor.execute(query)
        if is_return:
            result_set = [row for row in phoenix_cursor]
            return result_set
        close_phoenix_connection(phoenix_cursor, phoenix_conn)
    except:
        logger.error({"zone_computation:phoenix_query_action": "error", "error while doing phoenix query action": error_handling(str(query))})

def get_customers_list():
    filter_customer_query = queries["PHOENIX"]["CZ_FILTER_CUSTOMER"]
    customers = phoenix_query_action(filter_customer_query,None,True,"select")
    return customers[0][0]

def get_zones(shipper_id,carrier_id,tpl_id):
    zone_config_query = queries["PHOENIX"]["CZ_ZONE_CONFIG"]
    zone_config_query_parameter = (shipper_id,carrier_id,tpl_id)
    zone_values = phoenix_query_action(zone_config_query,zone_config_query_parameter,True,"select")
    keys = conf["CUSTOM_ZONES"]["ZONE_CONFIG_KEYS"]
    zone_list = [dict(zip(keys, zone)) for zone in zone_values]
    return zone_list

def get_isochrone_geo_points(zone,geofence_values):
    current_utc_time = datetime.strptime(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),"%Y-%m-%dT%H:%M:%SZ")
    last_refresh_time = datetime.strptime(geofence_values["refresh_time"],"%Y-%m-%dT%H:%M:%SZ") 
    logger.info({"method" : "zone_computation", "current_utc_time" : current_utc_time,"last_refresh_time":last_refresh_time})
    last_refresh_hour = get_last_refresh_hour(current_utc_time,last_refresh_time)
    logger.info({"method" : "zone_computation", "last_refresh_hour" : last_refresh_hour})
    if last_refresh_hour > conf["CUSTOM_ZONES"]["ISOCHRONE_REFRESH_TIME"] :
        logger.info({"method" : "zone_computation", "function" : "updating isochrone points"})
        geo_points = get_latest_geopoints(geofence_values["latitude"],geofence_values["longitude"],geofence_values["isochrone_time"] * 60)
        if geo_points:
            try:
                logger.info({"method" : "zone_computation", "new geo_points" : geo_points})
                geofence = json.dumps({"type" : "isochrone", "latitude" : float(geofence_values["latitude"]), "longitude" : float(geofence_values["longitude"]), "isochrone_time" : geofence_values["isochrone_time"], "geofence_points" : geo_points,"refresh_time" : datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")})
                update_zone_geopoints(zone["company_permalink"],zone["zone_id"],zone["updated_at"],geofence)
            except Exception as error:
                logger.info({"method" : "zone_computation", "old geo_points" : geo_points})
                geo_points = parse_geopoints(eval(zone["geofence_values"])["geofence_points"])  
        else:
            logger.info({"method" : "zone_computation", "old geo_points" : geo_points})
            geo_points = parse_geopoints(eval(zone["geofence_values"])["geofence_points"])
    else:
        logger.info({"method" : "zone_computation", "function" : "using existing isochrone points"})
        geo_points = parse_geopoints(eval(zone["geofence_values"])["geofence_points"])
    return geo_points

def get_last_refresh_hour(current_utc_time,last_refresh_time):
    difference_time = current_utc_time - last_refresh_time
    days_difference, seconds_difference = difference_time.days, difference_time.seconds
    hours_difference = days_difference * 24 + seconds_difference // 3600
    return hours_difference

def parse_geopoints(geo_points):
    parsed_geopoints = []
    for each_point in geo_points:
        parsed_geopoints.append((float(str(Decimal(each_point['lat']))),float(str(Decimal(each_point['lng'])))))
    return parsed_geopoints

def get_circular_violation_flag(geo_points,checkcall,zone_config):
    radius = eval(zone_config["geofence_values"])["geofence_radius"]
    distance_diff = get_distance_between_checkcalls([float(checkcall[0]),float(checkcall[1])],geo_points)
    return 1 if distance_diff <= radius else 0

def get_latest_geopoints(latitude,longitude,time):
    here_isochrone_url = requests.get(conf["CUSTOM_ZONES"]["ISOCHRONE_URL"].format(str(latitude),str(longitude),str(time)))
    try:
        isochrone_points = here_isochrone_url.json()["response"]["isoline"][0]["component"][0]["shape"]
        geofence_points = []
        for each_isochrone_point in isochrone_points:
            geo_coordinates = each_isochrone_point.split(",")
            geofence_points.append({"lat": float(geo_coordinates[0]), "lng": float(geo_coordinates[1])})
        return geofence_points
    except Exception as error:
        return None

def update_zone_geopoints(company_permalink,zone_id,updated_at,geofence_points):
    update_zone_query = queries["PHOENIX"]["CZ_UPDATE_ZONE"]
    update_zone_query_parameter = (company_permalink,zone_id,updated_at,geofence_points,1)
    phoenix_query_action(update_zone_query,update_zone_query_parameter,False,"upsert")
    logger.info({"method" : "zone_computation", "function" : "updated isochrone points in hbase"})

def get_distance_between_checkcalls(previous_checkcall,current_checkcall):
    slat = radians(float(previous_checkcall[0]));
    elat = radians(float(current_checkcall[0]));
    slon = radians(float(previous_checkcall[1]));
    elon = radians(float(current_checkcall[1]));
    dist = 3956 * arccos(sin(slat)*sin(elat) + cos(slat)*cos(elat)*cos(slon - elon))
    return dist #in miles

def checkcall_distance_time(previous_checkcall, current_checkcall):
    previous_checkcall_coordinates = [previous_checkcall[0],previous_checkcall[1]]
    current_checkcall_coordinates = [current_checkcall[0],current_checkcall[1]]
    distance = get_distance_between_checkcalls(previous_checkcall_coordinates,current_checkcall_coordinates) * 1609.34 #in meters
    current_checkcall_time_secs = int(time.mktime(time.strptime(current_checkcall[2],"%Y-%m-%dT%H:%M:%SZ")))
    previous_checkcall_time_secs = int(time.mktime(time.strptime(previous_checkcall[2],"%Y-%m-%dT%H:%M:%SZ"))) 
    time_gap_secs = current_checkcall_time_secs - previous_checkcall_time_secs
    time_gap_in_mins = time_gap_secs / 60
    return distance, time_gap_in_mins


def get_early_checkcall(checkcall,zone_config,geo_points,current_checkcall):
    import datetime
    previous_checkcall = previous_check_call_information(checkcall,1,True,current_checkcall)
    logger.info({"method":"zone_computation", "step" : "getting previous_checkcall for extra validation", "data" : previous_checkcall })
    if previous_checkcall:
        if zone_config["geofence_type"] in ["polygonal","isochrone"]:
            flag = inside_zone_flag((previous_checkcall[0],previous_checkcall[1]), geo_points)
        else:
            flag = get_circular_violation_flag(geo_points,previous_checkcall,zone_config)    
        return [previous_checkcall,checkcall] if flag else []
    else:
        return []

def early_checkcall_check_pass(early_checkcall_check,total_distance,total_time_gap_in_mins,zone_config):
    current_distance, current_time_gap_in_mins = checkcall_distance_time(early_checkcall_check[0], early_checkcall_check[1])
    total_distance = total_distance + current_distance
    total_time_gap_in_mins = total_time_gap_in_mins + current_time_gap_in_mins
    if total_distance < conf["CUSTOM_ZONES"]["MIN_DISTANCE"]:
        return False,True,early_checkcall_check[0],total_distance,total_time_gap_in_mins
    else:
        total_distance = total_distance - current_distance
        total_time_gap_in_mins = total_time_gap_in_mins - current_time_gap_in_mins
        if total_distance < conf["CUSTOM_ZONES"]["MIN_DISTANCE"] and total_time_gap_in_mins >= zone_config["zone_stop"]:
            return True,True,early_checkcall_check[1],total_distance,total_time_gap_in_mins
        else:   
            return True,False,early_checkcall_check[1],total_distance,total_time_gap_in_mins

def early_checkcall_check_fail(previous_checkcall,total_distance,total_time_gap_in_mins,zone_config):
    last_checkcall_flag = True
    logger.info({"method":"zone_computation", "step" : "after adding extra checkcall distance time", "distance" : total_distance, "time" : total_time_gap_in_mins })
    stop_flag = True if total_distance < conf["CUSTOM_ZONES"]["MIN_DISTANCE"] and total_time_gap_in_mins >= zone_config["zone_stop"] else False
    if stop_flag:
        logger.info({"method":"zone_computation", "step" : "success with extra checkcall distance time", "distance" : total_distance, "time" : total_time_gap_in_mins })
        return True,True, previous_checkcall,total_distance,total_time_gap_in_mins
    else:
        return True,False,None,total_distance,total_time_gap_in_mins

def get_stop_violation_flag(checkcall,zone_config,previous_checkcall_information,geo_points):
    if previous_checkcall_information:
        total_distance = 0
        total_time_gap_in_mins = 0
        for checkcall_list in range(len(previous_checkcall_information)):
            if checkcall_list < len(previous_checkcall_information) - 1:
                previous_checkcall = previous_checkcall_information[checkcall_list]
                current_checkcall = previous_checkcall_information[checkcall_list + 1]
                current_distance, current_time_gap_in_mins = checkcall_distance_time(previous_checkcall, current_checkcall)
                total_distance = total_distance + current_distance
                total_time_gap_in_mins = total_time_gap_in_mins + current_time_gap_in_mins
        logger.info({"method":"zone_computation", "distance" :total_distance, "time in mins" : total_time_gap_in_mins })
        if total_distance < conf["CUSTOM_ZONES"]["MIN_DISTANCE"]:
            previous_checkcall = previous_checkcall_information[0]
            last_checkcall_flag = False
            while not last_checkcall_flag:
                logger.info({"method":"zone_computation", "step" : "before going to get_early_checkcall", "previous_checkcall" : previous_checkcall })
                early_checkcall_check = get_early_checkcall(previous_checkcall,zone_config,geo_points,checkcall)
                logger.info({"method":"zone_computation", "step" : "extra checkcall", "data" : early_checkcall_check })
                last_checkcall_flag, stop_flag,previous_checkcall,total_distance,total_time_gap_in_mins = early_checkcall_check_pass(early_checkcall_check,total_distance,total_time_gap_in_mins,zone_config) if len(early_checkcall_check) else early_checkcall_check_fail(previous_checkcall,total_distance,total_time_gap_in_mins,zone_config)
                logger.info({"method":"zone_computation", "step" : "after early checkcall check", "last_checkcall_flag":last_checkcall_flag,"stop_flag":stop_flag,"previous_checkcall":previous_checkcall,"total_distance":total_distance,"total_time_gap_in_mins":total_time_gap_in_mins})
                if last_checkcall_flag:
                    return stop_flag,previous_checkcall
        else:
            return False, previous_checkcall_information[0]
    else:
        return False, None

def get_exisiting_violation_information(checkcall,zone_config):
    stop_zone_query = queries["PHOENIX"]["CZ_STOP_ZONE"]
    stop_zone_query_parameter = (str(checkcall["trackingId"]),checkcall["shipperId"],checkcall["carrierId"],checkcall["tplId"],zone_config["zone_id"])
    stop_zone_info = phoenix_query_action(stop_zone_query,stop_zone_query_parameter,True,"select")
    return stop_zone_info

def new_violation_insert(tracking_id,zone_config,checkcall):
    insert_violation_query = queries["PHOENIX"]["CZ_INSERT_VIOLATION"]
    insert_violation_query_parameter = (tracking_id,zone_config["company_permalink"],zone_config["zone_id"],str(checkcall[0])+"|"+str(checkcall[1]),checkcall[2])
    phoenix_query_action(insert_violation_query , insert_violation_query_parameter,False,"upsert")

def remove_stop_violation(checkcall,zone_config):
    remove_stop_violation_query = queries["PHOENIX"]["CZ_REMOVE_STOP_VIOLATION"]
    remove_stop_violation_query_parameter = (str(checkcall["trackingId"]),zone_config["company_permalink"],zone_config["zone_id"])
    phoenix_query_action(remove_stop_violation_query, remove_stop_violation_query_parameter, False,"delete")

def inside_zone_flag(checkcall,zone_points):
    logger.info({"method":"zone_computation","incoming checkcall" : (checkcall[0],checkcall[1]), "zone point" : zone_points })
    polygon = Polygon(zone_points)
    point = Point((checkcall[0],checkcall[1]))
    return 1 if polygon.contains(point) else 0

def get_last_checkcall(tracking_id,time,max_records):
    previous_checkcall_query = queries["PHOENIX"]["CZ_PREVIOUS_CHECKCALL"]
    previous_checkcall_query_param = (int(tracking_id),time,max_records)
    checkcall_result = phoenix_query_action(previous_checkcall_query,previous_checkcall_query_param,True,"select")
    return checkcall_result

def previous_check_call_information(checkcall,max_records,extra_validation_flag,current_checkcall):
    if extra_validation_flag:
        from_tz = current_checkcall['timeZone'] if current_checkcall['timeZone'] else 'UTC'
        cc_time_millis = int(datetime.timestamp(utc_time_conversion(from_tz, checkcall[2])))
        logger.info({"method":"zone_computation", "step" : "getting extra checkcall query", "time" : cc_time_millis})
        checkcall_result = get_last_checkcall(current_checkcall["trackingId"],cc_time_millis,max_records)
    else:
        from_tz = checkcall['timeZone'] if checkcall['timeZone'] else 'UTC'
        cc_time_millis = int(datetime.timestamp(utc_time_conversion(from_tz, checkcall['checkCallTime'])))
        checkcall_result = get_last_checkcall(checkcall["trackingId"],cc_time_millis,max_records)
    if checkcall_result and len(checkcall_result) > 1 :
        previous_checkcall = [(each_checkcall[0],each_checkcall[1],each_checkcall[2]) for each_checkcall in checkcall_result]
        previous_checkcall = [previous_checkcall[1],previous_checkcall[0]]
        previous_checkcall.append((float(checkcall["lat"]), float(checkcall["lng"]), checkcall["checkCallTime"]))
        return previous_checkcall
    elif checkcall_result and len(checkcall_result) < 2 :
        previous_checkcall = [(float(each_checkcall[0]),float(each_checkcall[1]),each_checkcall[2]) for each_checkcall in checkcall_result]
        return previous_checkcall[0]


def get_zone_violation_dict(checkcall,zone_config,flag,geo_points,previous_checkcall_info):
    zone_violation_flag = {}
    if zone_config["zone_enter"] and flag == conf["CUSTOM_ZONES"]["ENTRY_SEQ"]:
        zone_violation_flag["entry"] = {"latitude" : previous_checkcall_info[1][0], "longitude" : previous_checkcall_info[1][1],"event_time" : previous_checkcall_info[1][2], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0}
    if zone_config["zone_exit"]and flag == conf["CUSTOM_ZONES"]["EXIT_SEQ"]:
        zone_violation_flag["exit"] = {"latitude" : previous_checkcall_info[1][0], "longitude" : previous_checkcall_info[1][1],"event_time" : previous_checkcall_info[1][2], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0}
    existing_violation_info = get_exisiting_violation_information(checkcall,zone_config)
    previous_checkcall = previous_check_call_information(checkcall,conf["CUSTOM_ZONES"]["MIN_CHECKCALLS"], None, None) #get previous checkcall lat and long if it presents in checkcall table
    if zone_config["zone_stop"] and flag == conf["CUSTOM_ZONES"]["STOP_SEQ"] :
        stop_violation_flag, violation_checkcall_info = get_stop_violation_flag(checkcall,zone_config,previous_checkcall,geo_points)
        if stop_violation_flag:
            if not len(existing_violation_info) :
                new_violation_insert(checkcall["trackingId"],zone_config,violation_checkcall_info)
                zone_violation_flag["stop"] = {"latitude" : violation_checkcall_info[0], "longitude" : violation_checkcall_info[1],"violation_start_time" : violation_checkcall_info[2], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0, "current_checkcall_time" : checkcall["checkCallTime"]}
                logger.info({"method":"zone_computation", "new_violation_insert" :zone_violation_flag["stop"]})
            else:
                zone_violation_flag["stop"] = {"latitude" : float(existing_violation_info[0][3].split("|")[0]), "longitude" : float(existing_violation_info[0][3].split("|")[1]),"violation_start_time" : existing_violation_info[0][-1], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0, "current_checkcall_time" : checkcall["checkCallTime"]}
                logger.info({"method":"zone_computation", "on going violation" :zone_violation_flag["stop"]})
        elif stop_violation_flag is False and len(existing_violation_info) :
            remove_stop_violation(checkcall,zone_config)
            zone_violation_flag["stop"] = {"latitude" :float(existing_violation_info[0][3].split("|")[0]), "longitude" :float(existing_violation_info[0][3].split("|")[1]), "violation_start_time": existing_violation_info[0][-1], "violation_end_time" : previous_checkcall[1][2], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0}
            logger.info({"method":"zone_computation", "end violation" : zone_violation_flag["stop"]})
    else:
        if len(existing_violation_info) :
            remove_stop_violation(checkcall,zone_config)
            zone_violation_flag["stop"] = {"latitude" :float(existing_violation_info[0][3].split("|")[0]), "longitude" :float(existing_violation_info[0][3].split("|")[1]), "violation_start_time": existing_violation_info[0][-1], "violation_end_time" : previous_checkcall[1][2], "time_zone" : checkcall["timeZone"] if checkcall["timeZone"] else "UTC", "time_zone_offset" : checkcall["timeZoneOffset"] if checkcall["timeZoneOffset"] else 0}
    logger.info({"method":"zone_computation","zone_violation_flag" : zone_violation_flag, "tracking_id" : checkcall["trackingId"] })
    return zone_violation_flag


def parse_checkcalls_info(checkcalls_info):
    checkcall_coordinates = []
    checkcalls_info = checkcalls_info if isinstance(checkcalls_info, list) else [checkcalls_info]
    logger.info({"method":"zone_computation","parse_checkcalls_info" : checkcalls_info})
    for each_checkcall in checkcalls_info:
        checkcall_coordinates.append((each_checkcall[0],each_checkcall[1],each_checkcall[2]))
    return checkcall_coordinates


def zone_violation(geofence_type,geo_points,checkcall,zone_config):
    last_checkcalls_info = previous_check_call_information(checkcall,conf["CUSTOM_ZONES"]["MIN_CHECKCALLS"],None,None)
    logger.info({"method":"zone_computation","previous_checkcall" : last_checkcalls_info, "tracking_id" : checkcall["trackingId"] })
    if last_checkcalls_info:
        last_checkcalls = parse_checkcalls_info(last_checkcalls_info)
        flag = []
        if zone_config["geofence_type"] in ["polygonal","isochrone"]:
            for each_last_checkcall in last_checkcalls:
                flag.append(inside_zone_flag(each_last_checkcall, geo_points))
        else:
            for each_last_checkcall in last_checkcalls:
                flag.append(get_circular_violation_flag(geo_points,each_last_checkcall,zone_config))
        logger.info({"method":"zone_computation","flag" : flag, "tracking_id" : checkcall["trackingId"] })
        zone_violation_flag = get_zone_violation_dict(checkcall,zone_config,flag,geo_points,last_checkcalls)
        return zone_violation_flag

def zone_data_formation(zone_violation_list, zone_config):
    zone_info = {"zone_id" : zone_config["zone_id"], "zone_name" : zone_config["zone_name"], "zone_description" : zone_config["zone_description"], "zone_type" : zone_config["geofence_type"],
                  "zone_map_details" : zone_config["geofence_values"],"subscribers" : zone_config["subscribers"],
                  "zone_created_by" : zone_config["created_by"], "zone_created_at" : zone_config["created_at"], "zone_updated_by" : zone_config["updated_by"], "zone_updated_at" : zone_config["updated_at"]}
    if "entry" in zone_violation_list.keys():
        zone_info["zone_entry"] = [zone_violation_list["entry"]]
    if "exit" in zone_violation_list.keys():
        zone_info["zone_exit"] = [zone_violation_list["exit"]]
    if "stop" in zone_violation_list.keys():
        zone_info["zone_stop"] = [zone_violation_list["stop"]]
    return zone_info

def get_alert_stop(zone,existing_violation_info):
    for each_violation in existing_violation_info:
        if "violation_end_time" not in each_violation.keys():
            #each_violation["violation_end_time"] = zone["zone_stop"][0]["violation_end_time"]
            each_violation["violation_end_time"] = each_violation["current_checkcall_time"]
    return existing_violation_info
        
def get_violation_alert(old_value,zone,zone_flag):
    if old_value:
        if zone_flag in zone.keys():
            old_value.append(zone[zone_flag][0])
            return old_value
        else:
            return old_value
    else:
        return zone[zone_flag] if zone_flag in zone.keys() else []
 
def get_alert_stop_value(results,zone):
    alert_stop_dict = ast.literal_eval(results[0][5])
    if "zone_stop" in zone.keys():
        if "violation_end_time" not in zone["zone_stop"][0].keys():
            if "violation_end_time" not in alert_stop_dict[-1]:
                alert_stop_dict[-1]["current_checkcall_time"] = zone["zone_stop"][0]["current_checkcall_time"]
            else:
                alert_stop_dict.append(zone["zone_stop"][0])
            return alert_stop_dict
        else:
            return get_alert_stop(zone,ast.literal_eval(results[0][5]))
    else:
        return alert_stop_dict

def sqs_message_formation(zone):
    zone_sqs_message = {"id" : zone["zone_id"], "entry" : True if zone["alert_entry"] else False, "exit" : True if zone["alert_exit"] else False, "duration" : True if zone["alert_stop"] else False }
    return zone_sqs_message

def get_final_sqs_message(custom_zones,tracking_id):
    if len(custom_zones):
        sqs_message = {"TrackingId" : tracking_id, "customzone_alert" : json.dumps({"customZones" : custom_zones})}
        return sqs_message

def get_final_dynamo_message(custom_zones_info,tracking_id,company_permalink):
    dynamo_message = {"TrackingId" : tracking_id, "CompanyPermalink" : company_permalink, "zones" : custom_zones_info}
    return dynamo_message

def get_dynamo_message(zone):
    dynamo_zone_message = {"zone_id" :zone["zone_id"], "zone_name" :zone["zone_name"],"zone_map_details" : zone["zone_map_details"], "alert_entry":zone["alert_entry"], "alert_exit":zone["alert_exit"], "alert_stop":zone["alert_stop"],"zone_created_by":zone["created_by"], "zone_created_at":zone["created_at"], "zone_updated_by":zone["updated_by"], "zone_updated_at":zone["updated_at"],"alert_count" : zone["alert_count"] }
    return dynamo_zone_message

def get_violation_stop_alert(zone,active_alerts):
    alert_stop = get_alert_stop_value(active_alerts,zone) if ast.literal_eval(active_alerts[0][5]) else zone["zone_stop"] if "zone_stop" in zone.keys() else []
    return alert_stop

def get_ongoing_violation(zone,active_alerts):
    if active_alerts:
        alert_entry = get_violation_alert(ast.literal_eval(active_alerts[0][3]),zone,"zone_entry")
        alert_exit = get_violation_alert(ast.literal_eval(active_alerts[0][4]),zone,"zone_exit")
        alert_stop = get_violation_stop_alert(zone,active_alerts)
    else:
        alert_entry = zone["zone_entry"] if "zone_entry" in zone.keys() else []
        alert_exit = zone["zone_exit"] if "zone_exit" in zone.keys() else []
        alert_stop = zone["zone_stop"] if "zone_stop" in zone.keys() else []
    return alert_entry,alert_exit,alert_stop

def upsert_active_alerts_table(zone_info):
    upsert_active_alert_query = queries["PHOENIX"]["CZ_UPSERT_ACTIVE_ALERT"]
    upsert_query_parameter = (str(zone_info["tracking_id"]),zone_info["company_permalink"],zone_info["zone_id"],str(zone_info["alert_entry"]),str(zone_info["alert_exit"]),str(zone_info["alert_stop"]),zone_info["created_by"],zone_info["created_at"],zone_info["updated_by"],zone_info["updated_at"],zone_info["alert_count"])
    phoenix_query_action(upsert_active_alert_query,upsert_query_parameter,False,"upsert")

def get_mail_sqs_message(zone,tracking_id,company_permalink):
    return {"EmailAddress" : zone["subscribers"],"TrackingIds" : tracking_id, "MessageType" : "RECO_EMAIL_ALERTS", "Type" : "custom_zone", "zone_name" : zone["zone_name"], "zone_id" : zone["zone_id"], "CompanyPermalink" : company_permalink}

def get_all_affected_zones(tracking_id,company_permalink):
    affected_zone_query = queries["PHOENIX"]["CZ_AFFECTED_ZONES"]
    affected_zone_query_param = (tracking_id,company_permalink)
    affected_zones = phoenix_query_action(affected_zone_query, affected_zone_query_param,True,"select")
    affected_zones_keys = conf["CUSTOM_ZONES"]["ZONE_VIOLATION_KEYS"]
    affected_zone_list = [dict(zip(affected_zones_keys, each_affected_zone)) for each_affected_zone in affected_zones]
    return affected_zone_list

def update_active_alerts(zone_violation_load):  
    active_alert_query = queries["PHOENIX"]["CZ_ACTIVE_ALERT"]
    zone_details = []
    for each_zone in zone_violation_load["zones"]:
        active_alert_query_parameter = (zone_violation_load["tracking_id"],zone_violation_load["company_permalink"],each_zone["zone_id"])
        active_alerts = phoenix_query_action(active_alert_query, active_alert_query_parameter,True,"select")
        alert_entry,alert_exit,alert_stop = get_ongoing_violation(each_zone,active_alerts)
        alert_count = len(alert_entry)  + len(alert_exit)  + len(alert_stop)
        zone_info = {"alert_entry" : alert_entry, "alert_exit" : alert_exit, "alert_stop" : alert_stop, "alert_count":alert_count,"zone" : each_zone, "tracking_id" :zone_violation_load["tracking_id"], "company_permalink" : zone_violation_load["company_permalink"], "created_by":each_zone["zone_created_by"], "created_at": each_zone["zone_created_at"], "updated_by" : each_zone["zone_updated_by"], "updated_at" : each_zone["zone_updated_at"] , "zone_id" : each_zone["zone_id"], "zone_map_details" : eval(each_zone["zone_map_details"]), "zone_name" : each_zone["zone_name"], "subscribers" : each_zone["subscribers"]}
        upsert_active_alerts_table(zone_info)
        zone_details.append(zone_info)
    currently_affected_zone_ids = [each_zone["zone_id"] for each_zone in zone_violation_load["zones"]]
    previously_affected_zones = get_all_affected_zones(zone_violation_load["tracking_id"],zone_violation_load["company_permalink"])
    for each_affected_zone in previously_affected_zones:
        if each_affected_zone["zone_id"] in currently_affected_zone_ids:
            continue
        else:
            each_affected_zone["alert_entry"] = eval(each_affected_zone["alert_entry"])
            each_affected_zone["alert_exit"] = eval(each_affected_zone["alert_exit"])
            each_affected_zone["alert_stop"] = eval(each_affected_zone["alert_stop"])
            each_affected_zone["zone_map_details"] = eval(each_affected_zone["zone_map_details"])
            zone_details.append(each_affected_zone)
    return zone_details

def get_sqs_dynamo_msg(zone_details,currently_affected_zones):
    custom_zones_flag = []
    dynamo_zone_message = []
    mail_sqs_message = []
    for each_zone in zone_details:
        custom_zones_info = sqs_message_formation(each_zone)
        if any(key in custom_zones_info.keys() for key in ["entry","exit","stop"]):
            custom_zones_flag.append(custom_zones_info)
        dynamo_zone_info = get_dynamo_message(each_zone)
        if len(dynamo_zone_info["alert_entry"]) or len(dynamo_zone_info["alert_exit"]) or len(dynamo_zone_info["alert_stop"]):
            dynamo_zone_message.append(get_dynamo_message(each_zone))
    for each_zone in currently_affected_zones:
        mail_sqs_message.append(get_mail_sqs_message(each_zone,zone_details[0]["tracking_id"],zone_details[0]["company_permalink"]))
    final_sqs_message = get_final_sqs_message(custom_zones_flag,zone_details[0]["tracking_id"])
    final_dynamo_message = get_final_dynamo_message(dynamo_zone_message,zone_details[0]["tracking_id"],zone_details[0]["company_permalink"])
    return final_sqs_message, mail_sqs_message, final_dynamo_message

def zone_computation(checkcall, zone_list):
    zones = []
    for each_zone in zone_list:
        geofence_values = eval(each_zone["geofence_values"])
        if each_zone["geofence_type"] == "isochrone":
            geo_coordinates = get_isochrone_geo_points(each_zone, geofence_values)
            logger.info({"zone_computation" : "get_isochrone_geo_points ", "checkcall" : checkcall, "geopoints" : geo_coordinates })
        elif each_zone["geofence_type"] == "polygonal":
            geo_coordinates = parse_geopoints(geofence_values["geofence_points"])
        else:
            geo_coordinates = (float(geofence_values['latitude']),float(geofence_values['longitude']))
        zone_violation_flag = zone_violation(each_zone["geofence_type"],geo_coordinates,checkcall,each_zone)
        logger.info({"method":"zone_computation", "message" : "{} has violation".format(zone_violation_flag)})
        if zone_violation_flag:
            if any(key in zone_violation_flag.keys() for key in ["entry","exit","stop"]):
                zones.append(zone_data_formation(zone_violation_flag,each_zone))
    if zones:
        zone_details = update_active_alerts({"tracking_id" : checkcall["trackingId"], "company_permalink" : zone_list[0]["company_permalink"], "zones" : zones})
        if zone_details:
            es_sqs_message,mail_sqs_message,dynamo_message = get_sqs_dynamo_msg(zone_details,zones)
            return [es_sqs_message,mail_sqs_message,dynamo_message]
