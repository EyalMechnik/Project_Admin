import ConfigParser
import datetime
import os
import socket
import sys
import pandas as pd
from google.cloud import bigquery

script_path = str(os.path.dirname(os.path.realpath(__file__)))
main_dir = os.path.abspath(os.path.join(script_path,'..'))
project_dir = os.path.abspath(os.path.join(script_path,'..'))

current_host_name = socket.gethostname()

tasks_tracking_path = os.path.join(main_dir , 'Tasks_tracking')
task_tracking_file_name = 'PLTaskTracking_' + str(datetime.datetime.now().date()) + '.txt'  # name for tracking file

if not os.path.exists(tasks_tracking_path):  # Create folder for Task tracking if doesn't exist
    os.makedirs(tasks_tracking_path)

f_logging = open(os.path.join(tasks_tracking_path , task_tracking_file_name),'w') # don't forget to close!!!!!!
f_logging.write('Start' + '\n\n')

#import the mapper for config files, the function for performing smart action on members
generic_python_scripts_path = os.path.abspath(os.path.join(script_path,'..','Utils','Python'))

sys.path.append(os.path.abspath(generic_python_scripts_path))
from Python_Generic_Functions import exec_query_googleBigQuery, ConfigSectionMap, Insert_Project_Status

output_path = os.path.join(main_dir, 'Output')
if not os.path.exists(output_path):  # Create folder for Output if doesn't exist
    os.makedirs(output_path)

Config = ConfigParser.ConfigParser()
Config.read(os.path.join(main_dir , 'Input','config.ini'))

#Parse config file
config_Queries = ConfigSectionMap(Config, "Queries")
config_GoogleCloud = ConfigSectionMap(Config, "GoogleCloud")

#Bring query
PAmoduleQueryFullPath = config_Queries['pamodule_query_full_path']
use_legacy_sql = eval(config_Queries['use_legacy_sql'])
timeout = int(config_Queries['timeout'])
f_PAQueryTemplate = open(os.path.join(main_dir,PAmoduleQueryFullPath,'Project_Run_Query.txt'), "r")
f_PALQueryTemplate = open(os.path.join(main_dir,PAmoduleQueryFullPath,'Project_List_Query.txt'), "r")
f_PASQueryTemplate = open(os.path.join(main_dir,PAmoduleQueryFullPath,'Project_Status_Query.txt'), "r")

PAQueryTemplate = f_PAQueryTemplate.read()
f_PAQueryTemplate.close()

PALQueryTemplate = f_PALQueryTemplate.read()
f_PALQueryTemplate.close()

PASQueryTemplate = f_PASQueryTemplate.read()
f_PASQueryTemplate.close()

# extract google cloud relative paths and compose absolute paths
full_path_to_bigquery_json_keyfile = os.path.join(project_dir ,'Utils', config_GoogleCloud['full_path_to_bigquery_json_keyfile'],config_GoogleCloud['bigquery_json_keyfile_name'])
dataset_name = config_GoogleCloud['dataset_name']
table_name = config_GoogleCloud['table_name']

projectlist = exec_query_googleBigQuery(query=PALQueryTemplate,full_path_to_bigquery_json_keyfile=full_path_to_bigquery_json_keyfile,use_legacy_sql=use_legacy_sql, timeout=timeout)
projectrun = exec_query_googleBigQuery(query=PAQueryTemplate,full_path_to_bigquery_json_keyfile=full_path_to_bigquery_json_keyfile,use_legacy_sql=use_legacy_sql, timeout=timeout)
projectstatus = exec_query_googleBigQuery(query=PASQueryTemplate,full_path_to_bigquery_json_keyfile=full_path_to_bigquery_json_keyfile,use_legacy_sql=use_legacy_sql, timeout=timeout)

projectlist.columns = ['project_id', 'project_name', 'machine_name', 'machine_google_project', 'machine_zone', 'input_folder', 'run_file_command']
projectrun.columns = ['Business_ID','Combined_Business_ID','Business_Name', 'project_name', 'project_id']
projectstatus.columns = ['project_id','project_name','project_activity_status','project_activity_datetime']
projectsforpull = os.path.join(main_dir, 'Input','projects_list.csv')
projectlist.to_csv(projectsforpull, index=False, encoding='utf-8-sig')

projectsBusiness = os.path.join(main_dir, 'Input','projects_businesses_to_run.csv')
projectrun.to_csv(projectsBusiness, index=False, encoding='utf-8-sig')

projectsCurrentStatus = os.path.join(main_dir, 'Input','project_current_status.csv')
projectstatus.to_csv(projectsCurrentStatus, index=False, encoding='utf-8-sig')

#Bring Project and business lists
project_pull = pd.read_csv(os.path.join(main_dir, 'Input','projects_list.csv'))
business_project_pull = pd.read_csv(os.path.join(main_dir, 'Input','projects_businesses_to_run.csv'))
project_status_pull = pd.read_csv(os.path.join(main_dir, 'Input','project_current_status.csv'))


f_logging.write('########### \nProject_List: \n')
for index, rowp in project_pull.iterrows():
    f_logging.write(str(rowp['project_name']) + '\n')

f_logging.write('########### \n\n')

#-------------------------#
#   loop over projects  #
#-------------------------#
for index, rowp in project_pull.iterrows():
    current_project = str(rowp['project_name'])
    current_projec_ID = str(rowp['project_id'])
    try:
        business_criteria = business_project_pull['project_name'] == current_project
        business_pull = business_project_pull[business_criteria]

        project_criteria = project_status_pull['project_name'] == current_project
        project_stat_pull = project_status_pull[project_criteria]

        if project_stat_pull['project_activity_status'].any() == 'DONE':

            if len(business_pull.index)<>0:
                f_logging.write('\n==================================================\n' + '# ' + ' (' + str(current_project) + '):\n')

                for index, rowb in business_pull.iterrows():
                    f_logging.write(str(rowb['Business_ID']) + '\n')

                business_project_path = os.path.join(main_dir, 'Output', str(current_project))
                ForInputFolderPath = os.path.join(business_project_path, 'For_Input')
                ForLogFolderPath = os.path.join(business_project_path, 'For_Log')

                if not os.path.exists(business_project_path):  # Create folder for project if doesn't exist
                    os.makedirs(business_project_path)
                if not os.path.exists(ForInputFolderPath): # Create folder for input if doesn't exist
                    os.makedirs(ForInputFolderPath)
                if not os.path.exists(ForLogFolderPath): # Create folder for logs if doesn't exist
                    os.makedirs(ForLogFolderPath)

                # create business list for project file
                business_filename = 'business_to_run_' + str(current_projec_ID) + '_' + datetime.datetime.utcnow().strftime('%Y%M%d_%H%M%S') + '.csv'
                business_filename_for_input = 'business_to_run_' + str(current_projec_ID) + '.csv'
                business_filename_full_path = os.path.join(business_project_path, ForLogFolderPath, business_filename)
                business_filename__for_input_full_path = os.path.join(business_project_path, ForInputFolderPath, business_filename_for_input)
                business_pull[['Business_ID','Combined_Business_ID','Business_Name']].to_csv(business_filename_full_path, index=False, encoding='utf-8-sig')
                business_pull[['Business_ID','Combined_Business_ID','Business_Name']].to_csv(business_filename__for_input_full_path, index=False, encoding='utf-8-sig')


                # create file transfer command
                file_transfer_command = 'gcloud compute --project "<machine_google_project>" scp --zone "<machine_zone>" "<output_folder>"  <machine_name>:/tmp/ &'
                file_transfer_command = file_transfer_command.replace('<machine_google_project>',rowp['machine_google_project'])
                file_transfer_command = file_transfer_command.replace('<machine_zone>', rowp['machine_zone'])
                file_transfer_command = file_transfer_command.replace('<output_folder>', business_filename__for_input_full_path)
                file_transfer_command = file_transfer_command.replace('<machine_name>', rowp['machine_name'])
                print(file_transfer_command)

                os.system(file_transfer_command) # send file to tmp folder on project machine

                # create file move in machine command
                file_move_command = 'gcloud compute --project "<machine_google_project>" ssh --zone "<machine_zone>" "<machine_name>" --command="sudo mv /tmp/<business_filename> <input_folder>" &'
                file_move_command = file_move_command.replace('<machine_google_project>',rowp['machine_google_project'])
                file_move_command = file_move_command.replace('<machine_zone>', rowp['machine_zone'])
                file_move_command = file_move_command.replace('<machine_name>', rowp['machine_name'])
                file_move_command = file_move_command.replace('<business_filename>', business_filename_for_input)
                file_move_command = file_move_command.replace('<input_folder>', rowp['input_folder'])
                print(file_move_command)

                os.system(file_move_command) # move file inside machine to input folder

                run_project_file_command = 'gcloud compute --project "<machine_google_project>" ssh --zone "<machine_zone>" "<machine_name>" --command="sudo su - como; <run_file_command>" &'
                run_project_file_command = run_project_file_command.replace('<machine_google_project>', rowp['machine_google_project'])
                run_project_file_command = run_project_file_command.replace('<machine_zone>', rowp['machine_zone'])
                run_project_file_command = run_project_file_command.replace('<machine_name>', rowp['machine_name'])
                run_project_file_command = run_project_file_command.replace('<run_file_command>', rowp['run_file_command'])
                print(run_project_file_command)

                os.system(run_project_file_command) # run project

                Insert_Project_Status(project_id=rowp['project_id'] , project_name=rowp['project_name'], activity_status='RUN_REQUEST', full_path_to_bigquery_json_keyfile=full_path_to_bigquery_json_keyfile)



    except Exception as e:
        f_logging.write('*** An error has occured for project - ' + ' (' + str(current_project) + '):\n' + str(e) + '\n\n')
        Insert_Project_Status(project_id=rowp['project_id'], project_name=rowp['project_name'],comment=str(e),activity_status='ERROR_OCCURRED_IN_REQUEST',full_path_to_bigquery_json_keyfile=full_path_to_bigquery_json_keyfile)

f_logging.close()
