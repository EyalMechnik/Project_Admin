# this function runs a procedure that uploads a file to a bucket in our cloud storage, and performs and action of tagging/Untagging on the members in that file

import subprocess;
import requests;
import json;
import datetime;
import pandas as pd;
from google.cloud import  bigquery ,storage
from google.cloud.exceptions import NotFound
from oauth2client.client import GoogleCredentials

#this function will limit access to _run_Como_server_scheduled_task_API for only Tag operation
def run_Como_server_scheduled_task_API_tag(bucketName, filepath, filename, location_ID, server, server_token,
                                       path_to_json_keyfile, blob_prefix,action_tag_for_logging,tags):

    timing = str((datetime.datetime.utcnow() + datetime.timedelta(minutes=1)).strftime("%d.%m.%Y %H:%M:%S"))
    actionTags = {"tagType": "custom", "tagValue": action_tag_for_logging}
    smartActions = [{"action": {"type": "tagMembership","tag": tags ,"operation": "Tag","actionTags": actionTags}}]

    return _run_Como_server_scheduled_task_API(bucketName, filepath, filename, smartActions, location_ID, server, server_token,
                                       path_to_json_keyfile, blob_prefix, timing)

#this function will limit access to _run_Como_server_scheduled_task_API for only unTag operation
def run_Como_server_scheduled_task_API_untag(bucketName, filepath, filename, location_ID, server, server_token,
                                       path_to_json_keyfile, blob_prefix,action_tag_for_logging,tags):

    timing = str((datetime.datetime.utcnow() + datetime.timedelta(minutes=1)).strftime("%d.%m.%Y %H:%M:%S"))
    actionTags = {"tagType": "custom", "tagValue": action_tag_for_logging}
    smartActions = [{"action": {"type": "tagMembership","tag": tags ,"operation": "unTag","actionTags": actionTags}}]

    return _run_Como_server_scheduled_task_API(bucketName, filepath, filename, smartActions, location_ID, server, server_token,
                                       path_to_json_keyfile, blob_prefix, timing)


def _run_Como_server_scheduled_task_API(bucketName, filepath, filename, smart_actions, location_ID, server, server_token,
                                       path_to_json_keyfile, blob_prefix, timing):
    # upload file to bucket

    # client = bigquery.client.from_service_account_p12(client_email, path_to_p12_keyfile)
    client = storage.Client.from_service_account_json(path_to_json_keyfile)
    bucket = client.get_bucket(bucketName)
    # blob = bucket.blob('bi-stored-requests/' + filename)
    blob = bucket.blob(blob_prefix + filename)
    blob.upload_from_filename(filepath)

    # command = 'gsutil cp ' + filepath + ' gs://' + bucket + '/bi-stored-requests/';
    # subprocess.call(command, shell=True);
    # print "uploaded file " + filepath +" successfuly to " + bucketName;

    # ---------------------------------------

    #  populate scheduled action POST /schedule/population
    url = server + '/schedule/population?MimeType=application/json&InputType=json&token=' + server_token
    headers = {'content-type': 'application/json'}
    payload = {
        "type": "file",
        "data": {
            "bucketName": bucketName,
            # "objectName": "stored-request-actions/" + filename
            # "objectName": 'bi-stored-requests/' + filename
            "objectName": blob_prefix + filename
        }
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    id = response.json().get("id").encode("ascii")

    # print "schedule population successfull, id : " + id
    sch_pop = "schedule population successfull, id : " + id

    # ---------------------------------------

    # send scheduled action with tag/untag action
    url = server + '/schedule/actions?MimeType=application/json&InputType=json&token=' + server_token
    headers = {'content-type': 'application/json'}
    # operation = 'Tag' if shouldTag else 'UnTag'

    payload = {
        "name": "BI-Automation" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tags": [
            location_ID
        ],
        "status": "enabled",
        "timing": {
            "type": "date",
            "value": timing
        },
        "population": {
            "id": id
        },
        "action": "smartAction",
        "data": {
            "action": {
                "type": "actionFlow",
                "flowType": "all",
                "smartActions": [
                    {
                        "action": {
                            "type": "actionFlow",
                            "flowType": "onlyFirst",
                            "smartActions": [
                                {
                                    "action": {
                                        "type": "actionFlow",
                                        "flowType": "all",
                                        "smartActions": smart_actions
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            "calcs": [
                {
                    "id": "Membership",
                    "calc": {
                        "Type": "SpecialQuery",
                        "queryType": "Membership",
                        "params": {

                        }
                    }
                },
                {
                    "id": "TimeZone",
                    "calc": {
                        "Type": "Compute",
                        "function": "evalExpression",
                        "params": {
                            "expression": {
                                "type": "Literal",
                                "value": "Asia\/Jerusalem",
                                "raw": "'Asia\/Jerusalem'"
                            }
                        }
                    }
                },
                {
                    "id": "dateFormat",
                    "calc": {
                        "Type": "Compute",
                        "function": "evalExpression",
                        "params": {
                            "expression": {
                                "type": "Literal",
                                "value": "DD\/MM\/YYYY",
                                "raw": "'DD\/MM\/YYYY'"
                            }
                        }
                    }
                }
            ]
        }
    }

    # print json.dumps(payload)
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    # print response.json()
    return response.json(), sch_pop


# This function's goal is to parse configuration .ini files
import ConfigParser


def ConfigSectionMap(Config, section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


# # this function is used for querying data from google bigQuery DB
# def exec_query_googleBigQuery(query, full_path_to_bigquery_json_keyfile, use_legacy_sql, timeout):
#     client = bigquery.Client.from_service_account_json(full_path_to_bigquery_json_keyfile)
#     query_results = client.query(query)
#
#     # set configurations for the query execution
#     #query_results.use_legacy_sql = use_legacy_sql  # Use Legacy/standard SQL syntax for query
#     #query_results.timeout_ms = timeout_ms  # determine the timeout of a query execution
#     query_results.result(timeout=timeout)
#
#     #query_results.run()
#
#     rows = pd.DataFrame()
#
#     # Drain the query results by requesting a page at a time.
#     #page_token = None
#
#     while True:
#         # fetched_rows, total_rows, page_token = query_results.fetch_data(page_token=page_token)
#         # print "total_rows: " + str(total_rows)  #!!!!!!!!!!!!!! - remove at the end
#         pd.read_gbq(query, project_id=None, index_col=None, col_order=None, reauth=False, verbose=True,private_key=None, dialect='legacy', **kwargs)[source]
#         fetched_rows_df = pd.DataFrame(fetched_rows)
#         rows = rows.append(fetched_rows_df)
#
#         if not page_token:
#             break
#
#     return rows


# NEW this function is used for querying data from google bigQuery DB
def exec_query_googleBigQuery(query, full_path_to_bigquery_json_keyfile, use_legacy_sql, timeout):

    rows = pd.read_gbq(query, project_id='como-bi', private_key=full_path_to_bigquery_json_keyfile, dialect='standard')

    return rows



# this function creates a Table in bigquery
def create_bq_table(table_ref,schema,bigquery_client):
    table = bigquery.Table(table_ref, schema)
    bigquery_client.create_table(table)  # API request

# this function Checks if a Table exist in bigquery
def is_table_not_exist(table_ref, bigquery_client):
    try:
        bigquery_client.get_table(table_ref)
        return False # Table exists
    except NotFound:
        return True

# this function Saves Query results into an existing bigquery Table
def write_rows_from_query(table_ref,query,method,bigquery_client):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = method
    bigquery_client.query(query, job_config=job_config)

# this function loads rows from a CSV file to a bigquery table, without first row (titles) and no auto-detect
def load_data_from_CSV_file(source_file_name,table_ref,bigquery_client):
    with open(source_file_name, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'CSV'
        job_config.skip_leading_rows = 1
        #job_config.autodetect = True
        bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)

# this functions write a status for the current project admin into the status table in data_admin
def Insert_Project_Status(project_id,project_name,activity_status,full_path_to_bigquery_json_keyfile,business_id_int=None,comment=None):
    bigquery_client = bigquery.Client.from_service_account_json(full_path_to_bigquery_json_keyfile)
    table_ref = bigquery_client.dataset('Data_Admin').table('Project_Admin_Status')
    table = bigquery_client.get_table(table_ref)
    rows_to_insert = [
        {"project_id": project_id,
         "project_name": project_name,
         "business_ID": business_id_int,
         "project_activity_status": activity_status,
         "project_comment": comment,
         "project_activity_datetime": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
    ]
    bigquery_client.insert_rows(table, rows_to_insert)