# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import jaydebeapi
import os
import csv

import warnings
warnings.filterwarnings('ignore')

start_time = datetime.datetime.now()

current_file_path = os.path.abspath(__file__)
print(current_file_path)
current_file_directory = os.path.dirname(current_file_path)

driverclassname = 'io.trino.jdbc.NonRegisteringTrinoDriver'
classpath = 'C:/Users/2024840/PrestoPRD/PrestoPRD/trino-jdbc-368.jar'
javajrepath = 'C:/Program Files/Java/jre1.8.0_45/bin/server/jvm.dll'
jdbcurl = 'jdbc:trino://hklpaphas070.global.standardchartered.com:8331/hive/crhsrep?SSL=true&SSLTrustStorePath=C:/Users/2024840/PrestoPRD/PrestoPRD/hklpaphas070.jks&SSLKeyStorePath=C:/Users/2024840/PrestoPRD/PrestoPRD/hklpaphas070.jks&SSLKeyStorePassword=changeit&SSLTrustStorePassword=changeit'
userId = '2024840'

# pwd= getpass.getpass('Enter password')
# config_file_path = '../../Code/configs.txt'
config_file_path = os.path.abspath(os.path.join(current_file_directory, r'../../Code/configs.txt'))
with open(config_file_path,'r') as config_file:
    for row in csv.DictReader(config_file):
        vars()[row['variable_name']] = row['variable_value']

# Connect to the database using jaydebeapi
conn = jaydebeapi.connect(driverclassname, jdbcurl, [userId, pwd], classpath)
conn.jconn.setAutoCommit(True)

# # SQL Query to retrieve data
query_dp1 = """ SELECT * FROM hive.crhs_analytics.rcsa_landingpage_staging """

# RCSA data product 1
try:
    df_rcsa_landingpage_all = pd.read_sql(query_dp1, conn)
    print(f'DP1 table records count {df_rcsa_landingpage_all.shape[0]}')
except Exception as e:
    print(f'the error in reading data product 1 database table is {e}')
    # DP1 -  data source - rcsa_landingpage_staging_madeup_data (6) 
    dp1_file_path = os.path.abspath(os.path.join(current_file_directory, '../data/rcsa_landingpage_staging_madeup_data (7).xlsx'))
    df_rcsa_landingpage_all = pd.read_excel(dp1_file_path, dtype='str')
    print(f'DP1 file records count {df_rcsa_landingpage_all.shape[0]}')

# # column names - replace spaces with underscores and convert to lower case
df_rcsa_landingpage_all.rename(columns=lambda col: col.replace(' ', '_').lower(),inplace=True)

# missing values - replace with 'NA'
# Note: This is a placeholder for the actual data cleaning step.
df_rcsa_landingpage_all.fillna('NA', inplace=True)

# data types - convert specific columns to appropriate data types

# columns to convert datatypes to float
columns_to_float = ['last_approved_rr_score', 'calculated_rr_score_when_last_approved', 'latest_calculated_rr_score']

# Convert the selected columns to float type
df_rcsa_landingpage_all[columns_to_float] = df_rcsa_landingpage_all[columns_to_float].astype(float, errors='ignore')

# Convert the selected columns to date type
df_rcsa_landingpage_all[[col for col in df_rcsa_landingpage_all.columns if 'date' in col or col == 'cob']] \
    = df_rcsa_landingpage_all[[col for col in df_rcsa_landingpage_all.columns if 'date' in col or col == 'cob']].apply(pd.to_datetime, errors='ignore')

# load DP1 for latest date based on maximum of dateofloading
dp1_dateofloading = df_rcsa_landingpage_all[df_rcsa_landingpage_all['cob'] != 'NA']['cob'].max()
print(f'rules filtered for date: {dp1_dateofloading}')

df_rcsa_landingpage = df_rcsa_landingpage_all.loc[df_rcsa_landingpage_all['cob'] == dp1_dateofloading]
print(f'DP1 latest records count {df_rcsa_landingpage.shape[0]}')

# rule_data  
rule_data_file_path = os.path.abspath(os.path.join(current_file_directory, '../data/ALL_DRMRCSA_RULEDATA_madeup_20250613.csv'))
df_rule_data = pd.read_csv(rule_data_file_path, dtype='str', keep_default_na=False)

# column names - replace spaces with underscores and convert to lower case
df_rule_data.rename(columns=lambda col_name: col_name.replace(' ', '_').lower(), inplace=True)

# rule_criteria
rule_criteria_file_path = os.path.abspath(os.path.join(current_file_directory, '../data/ALL_DRMRCSA_RULECRITERIA_madeup_20250619.csv'))
df_rule_criteria = pd.read_csv(rule_criteria_file_path, dtype='str', keep_default_na=False)

# column names - replace spaces with underscores and convert to lower case
df_rule_criteria.rename(columns=lambda col_name: col_name.replace(' ', '_').lower(), inplace=True)

# creating folder to save output files
folder_name = f'{datetime.datetime.today().date().strftime('%Y%m%d')}'
if not os.path.exists(os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}'))):
    os.mkdir(os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}')))
    print(f"Folder '../data/{folder_name}' created successfully.")
else:
    print(f"Folder '../data/{folder_name}' already exists.")


# function to parse comparison rules from a DataFrame
def comparison_rule_parser(df_rule_criteria:pd.DataFrame) -> pd.DataFrame:
    """
    Parses the rule criteria DataFrame to generate conditions for each rule_id.
    """
    
    # Create new columns for logical and math operators in Python Pandas syntax
    df_rule_criteria['logical_operator_pd'] = ''
    df_rule_criteria['mathematical_operator_pd'] = ''
    
    # Map logical and math operators to Python Pandas syntax

    df_rule_criteria.loc[df_rule_criteria['logical_operator'] == 'AND','logical_operator_pd'] = '&'
    df_rule_criteria.loc[df_rule_criteria['logical_operator'] == 'OR','logical_operator_pd'] = '|'
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '=','mathematical_operator_pd'] = '=='
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '<>','mathematical_operator_pd'] = '!='
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '<','mathematical_operator_pd'] = '<'
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '>','mathematical_operator_pd'] = '>'
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '<=','mathematical_operator_pd'] = '<='
    df_rule_criteria.loc[df_rule_criteria['mathematical_operator'] == '>=','mathematical_operator_pd'] = '>='

    # Group by rule_id
    grouped_by_rule_id = df_rule_criteria.groupby("rule_id")
    ls_condition = [] 
    rule_ids_1 = []
    # Build conditions
    for rule_id, group in grouped_by_rule_id:
        # Group by operator_priority
        grouped_by_priority = group.groupby("operator_priority")
        condition = []
        for priority, priority_group in grouped_by_priority:
            priority_condition = []

            for _, row in priority_group.iterrows():
                
                if row['rule_id'] not in rule_ids_1:
                    rule_ids_1.append(row['rule_id'])

                # print(f'priority_condition : {priority_condition}')
                if len(condition) > 0 and len(priority_condition) == 0:
                    condition.append(row["logical_operator_pd"])
                    # print(f'condition : {condition}')

                if row['custom_rhs_input_flag'].lower().strip() == 'yes':
                    try:
                        isinstance(float(row['rule_criteria_rhs']), (int, float))
                        rule_condition = f"{row['rule_criteria_lhs']} {row['mathematical_operator_pd']} {row['rule_criteria_rhs']}"
                    except:
                        rule_condition = f"{row['rule_criteria_lhs']} {row['mathematical_operator_pd']} '{row['rule_criteria_rhs']}'"
                else:
                    # Assuming rule_criteria_rhs is a column name in the df_rcsa_landingpage DataFrame
                    rule_condition = f"{row['rule_criteria_lhs']} {row['mathematical_operator_pd']} {row['rule_criteria_rhs']}"

                # rule_condition = f"{row['rule_criteria_lhs']} {row['mathematical_operator_pd']} {row['rule_criteria_rhs']}"

                if priority_condition:
                    rule_condition = f"{row['logical_operator_pd']} {rule_condition}"
                priority_condition.append(rule_condition)
            # Wrap priority group in parentheses
            condition.append(f"({' '.join(priority_condition)})")
        # Combine all priority groups
        final_condition = ' '.join(condition)
        ls_condition.append(final_condition)
        print(f"Rule ID: {rule_id} Condition: {final_condition}")

    df_conditions = pd.DataFrame({'rule_id':rule_ids_1, 'condition':ls_condition})
    return df_conditions

# call the function to parse comparison rules and get DataFrame
print('The math and logical comparison operators are being parsed from the rule criteria DataFrame...')
df_parsed_comparison_rules = comparison_rule_parser(df_rule_criteria)

# Merge the parsed comparison rules DF with the categorical rule DF data to create a full rules DataFrame
df_rule_data_full = pd.merge(df_rule_data, df_parsed_comparison_rules, on='rule_id', how='left')

# function to retrieve categories for a given rule ID and column name
def get_categories(rule_id:str, column_name:str) -> list:
    """
    Retrieves all the categories associated with a given rule ID from the rule data DataFrame.

    """
    column_elements = df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id, column_name].item()
    return [c.lower().strip() for c in column_elements.split(';') if c.strip() != '']

# filter RCSA DP1 to generate alerts (RCSA DP2) based on the rule logic
def filter_logic(df_rcsa_landingpage:pd.DataFrame, df_rule_data:pd.DataFrame, rule_id:str) -> pd.DataFrame:
    """
    Apply the filter logic to the RCSA landing page data based on the specified rule ID.
    """
    
    df_filtered = df_rcsa_landingpage.query(f"process_id.notna() and process_id.str.lower().str.strip().isin({get_categories(rule_id,'process_id')}) \
                            & l1_risk_id.notna() and l1_risk_id.str.lower().str.strip().isin({get_categories(rule_id,'l1_risk_id')}) \
                            & l2_risk_id.notna() and l2_risk_id.str.lower().str.strip().isin({get_categories(rule_id,'l2_risk_id')}) \
                            & l3_risk_id.notna() and l3_risk_id.str.lower().str.strip().isin({get_categories(rule_id,'l3_risk_id')}) \
                            & apply_org_bf_l1_id.notna() and apply_org_bf_l1_id.str.lower().str.strip().isin({get_categories(rule_id,'apply_org_bf_l1_id')}) \
                            & apply_org_bf_l2_id.notna() and apply_org_bf_l2_id.str.lower().str.strip().isin({get_categories(rule_id,'apply_org_bf_l2_id')}) \
                            & apply_org_bf_l3_id.notna() and apply_org_bf_l3_id.str.lower().str.strip().isin({get_categories(rule_id,'apply_org_bf_l3_id')}) \
                            & country_org_id.notna() and country_org_id.str.lower().str.strip().isin({get_categories(rule_id,'country_org_id')}) \
                            & legal_entity_org_id.notna() and legal_entity_org_id.str.lower().str.strip().isin({get_categories(rule_id,'legal_entity_org_id')}) \
                            & ({df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'condition'].item()}) \
                            ")
        
    if df_filtered.shape[0] > 0:
        df_filtered.loc[:,'rule_id'] = rule_id
        df_filtered.loc[:,'rule_name'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_name'].item()
        df_filtered.loc[:,'alert_date'] = datetime.datetime.today().strftime('%Y-%m-%d %X')
        df_filtered.loc[:,'rule_description'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_description'].item()
        df_filtered.loc[:,'type_of_alert'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'type_of_alert'].item()
        df_filtered.loc[:,'alert_status'] = 'Open'
        df_filtered.loc[:,'criteria'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'condition'].item()
        df_filtered.loc[:,'rule_version'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_version'].item()
        df_filtered.loc[:,'rule_approved_date'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_approved_date'].item()
        df_filtered.loc[:,'dateofloading'] = datetime.datetime.today().strftime('%Y-%m-%d')
        df_filtered.loc[:,'cob'] = datetime.datetime.today().strftime('%Y-%m-%d')


    return df_filtered

# columns to be selected for the final DataFrame RCSA DP2
cols_df_final = ['alert_date', 'alert_id', 'alert_status', 'type_of_alert', 'rule_id', 'rule_name', \
    'process_id', 'process_name', 'risk_po_bank_id', 'risk_framework_owner',\
    'apply_org_bf_l1_id', 'apply_org_bf_l1_name', 'apply_org_bf_l2_id', 'apply_org_bf_l2_name', 'apply_org_bf_l3_id', 'apply_org_bf_l3_name', \
    'l1_risk_id', 'l1_risk_name', 'l2_risk_id', 'l2_risk_name', 'l3_risk_id', 'l3_risk_name',  \
    'country_org_id', 'country_name', 'legal_entity_org_id', 'legal_entity_name', 'cause_level1_id', 'cause_level2_id', \
    'inherent_risk_rating_approved_date', 'inherent_risk_rating', 'inherent_risk_rating_value', 'last_approved_rr_ce_rating_date', \
    'latest_calculated_ce_rating', 'last_approved_rr_rating', 'last_approved_rr_score', \
    'last_approved_rr_value', 'calculated_ce_when_last_approved', 'calculated_rr_when_last_approved', 'calculated_rr_score_when_last_approved', \
    'calculated_rr_value_when_last_approved', 'latest_calculated_rr_rating', 'latest_calculated_rr_score', 'latest_calculated_rr_value', \
    'criteria', 'rule_description', 'rule_version', 'rule_approved_date', \
    'ir_stakeholder_impact', 'ir_ics_impact', 'ir_media_impact', 'ir_fnl_loss_impact', 'ir_regulatory_impact', 'ir_safety_sec_impact', \
    'ir_likelihood_impact', 'ir_stakeholder_rationale', 'ir_ics_rationale', 'ir_media_rationale', 'ir_fnl_loss_rationale', 'ir_regulatory_rationale', \
    'ir_safety_sec_rationale', 'ir_likelihood_rationale', 'inherent_risk_rationale', 'residual_risk_rationale',\
    'kcicst_passed', 'kcicst_failed', 'kcicst_insight', 'issues_open', 'issues_closed', 'issues_high', \
    'issues_medium', 'issues_low', 'issues_insight', 'events_high', 'events_medium', 'events_low', 'events_insight', 'ce_rationale',\
    'dateofloading', 'cob']

# call the functions to parse comparison rules and get alerts data
# Get unique rule IDs from the rule data
# ls_rule_ids = df_rule_data_full['rule_id'].unique()
ls_rule_ids = df_rule_data_full.loc[df_rule_data_full['alert_generation_status'].str.lower() == 'active']['rule_id'].to_list()

print(f'Rule Ids tobe executed today: {ls_rule_ids}')
# Initialize an empty DataFrame to hold merged results
df_merged_running = pd.DataFrame()

# Initialize an empty DataFrame to hold rules execution status
df_rules_execution_status = pd.DataFrame(columns = ['rule_id', 'rule_name', 'rule_description', 'rule_version', 'rule_approved_date', \
                'rule_executed_date', 'frequency', \
                'rule_run_start_timestamp', 'no_of_alerts_generated', 'rule_executed_user',\
                'rule_run_status', 'reason_for_run_failure',\
                'dateofloading', 'cob'])

# Ensure the input DataFrames are not empty
if df_rcsa_landingpage.empty or df_rule_data_full.empty:
    raise ValueError("Input DataFrames cannot be empty.")

for rule_id in ls_rule_ids:
    df_filtered = pd.DataFrame()
    print(f'Processing rule ID: {rule_id}')
    try:
        df_filtered = filter_logic(df_rcsa_landingpage, df_rule_data_full, rule_id)
        # # concatination / union of all alerts genrated for each rule_id
        df_merged_running = pd.concat([df_merged_running, df_filtered], ignore_index=True)
        print(f'merged dataframe shape: {df_merged_running.shape}')

        rule_id_execution_status = 'Success'
        print(f'Rule ID {rule_id} executed successfully with {df_filtered.shape[0]} alerts generated.')
    except Exception as e:
        print(f"Exception occurred for rule ID {rule_id}: {e}")
        rule_id_execution_status = 'Failed'
        # continue
    finally:
        # rules executed today
        ls_rules_execution_status = [rule_id, df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_name'].item(), \
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_description'].item(), \
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_version'].item(), \
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_approved_date'].item(),\
                    datetime.datetime.today().strftime('%Y-%m-%d %X'),\
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'frequency'].item(),
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'last_updated_timestamp'].item(),\
                    df_filtered.shape[0],
                    df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'last_updated_by'].item(),\
                    rule_id_execution_status,
                    failure_reason,
                    datetime.datetime.today().date().strftime('%Y-%m-%d'),\
                    datetime.datetime.today().date().strftime('%Y-%m-%d')
                    ]

        df_rules_execution_status.loc[len(df_rules_execution_status)] = ls_rules_execution_status
        print(f'Rule ID {rule_id} execution status: {rule_id_execution_status}')

try:
    df_merged_running['alert_id'] = pd.Series([datetime.datetime.today().strftime('%Y%m%d') + str(x).zfill(2) \
            for x in range(1,len(df_merged_running.index)+1)], index = df_merged_running.index)

    # Save the executed rules to a CSV file
    # os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}/executed_rules_{folder_name}.csv'))
    df_rules_execution_status.to_csv(os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}/executed_rules_{folder_name}.csv')), index=False, encoding='utf-8')
    print(f'Executed rules saved as csv file with shape: {df_rules_execution_status.shape}')

    df_final = df_merged_running[cols_df_final]
    # print(f'final dataframe shape: {df_final.shape}')
    # Save the generated alerts to a CSV file
    # os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}/alerts_data_{folder_name}.csv'))
    df_final.to_csv(os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}/alerts_data_{folder_name}.csv')), index=False, encoding='utf-8')
    print(f'Generated alerts saved as csv file with shape: {df_final.shape}')

except Exception as e:
    print(f'the error in creating and final tables is {e}')

# De-Duplication between Service Bench (SB) and DP2 before sending new alerts feed to SB

# Feed from Service Bench
df_sb_1 = pd.read_csv(f'../data/{folder_name}/final_sb_feed_20250630.csv', dtype='str')
df_sb_1.fillna('NA', inplace=True)

# generate_hash
# 1) It takes an input string.
# 2) generates the SHA-256 hash of the string.
# The hexadecimal number system is a base-16 system that uses the digits 0–9 and the letters A–F to represent numbers.
# 3) converts the hexadecimal representation of the hash to an integer.
# 4) uses the modulo operator (%) to get the last 6/7/8 digits.
# 5) formats the result as a 6/7/8-digit string, padding with leading zeros if necessary.

def generate_hash(data, columns=None):
    '''
    create a SHA256 hash object
    Convert to int 
    modulo to 8 digits
    '''
    data_str = ''.join(str(x) for x in data[columns].values)
    hash_value = int(hashlib.sha256(data_str.encode()).hexdigest(), 16) % 100000000
    return hash_value

columns_for_skey  = ['l1_risk_id', 'l2_risk_id', 'l3_risk_id', 'apply_org_bf_l1_id',
                'apply_org_bf_l2_id', 'apply_org_bf_l3_id', 'country_org_id',
                'legal_entity_org_id', 'process_id' ]

df_sb_1['table_id'] = 'sb'
df_sb_1['row_id'] = pd.Series([x for x in range(1,len(df_sb_1.index)+1)], index = df_sb_1.index)
df_sb_1['alert_date_to_group'] = pd.to_datetime(df_sb_1['alert_date']).dt.date.astype('datetime64[s]')
df_sb_1['alert_id_skey'] = df_sb_1.apply(lambda df: generate_hash(df, columns_for_skey + ['rule_id']), axis=1)

df_final['table_id'] = 'dp2'
df_final['row_id'] = pd.Series([x for x in range(1,len(df_final.index)+1)], index = df_final.index)
df_final['alert_date_to_group'] = pd.to_datetime(df_final['alert_date']).dt.date.astype('datetime64[s]')
df_final['alert_id_skey'] = df_final.apply(lambda df: generate_hash(df, columns_for_skey + ['rule_id']), axis=1)

# combine the SB and new alerts
df_sb_dp2 = pd.concat([df_sb_1, df_final], ignore_index=True)

df_sb_dp2_closed = df_sb_dp2.loc[df_sb_dp2['alert_status'].str.lower() == 'closed'] # Closed alerts 
df_sb_dp2_under_action = df_sb_dp2.loc[df_sb_dp2['alert_status'].str.lower() != 'closed'] # get everything under action from SB other than Closed
# df_sb_dp2_under_action --> remove duplicates, keep all 'status = open' from SB and add records from DP2 which are not in SB

# retain the WIP alerts from SB and add new alerts - de-duplication with SQL window like functionality
df_sb_dp2_under_action['row_num'] = df_sb_dp2_under_action.sort_values(by='alert_date_to_group',ascending=True)\
                                    .groupby(['alert_id_skey']).cumcount() + 1

df_sb_dp2_final = df_sb_dp2_under_action.loc[df_sb_dp2_under_action['row_num'] == 1]

# alert ids, if the script is running for the second time, on the same day
df_sb_same_day_alerts = df_sb_1.loc[pd.to_datetime(df_sb_1['alert_date']).dt.date.astype('str') == datetime.date.today().strftime('%Y-%m-%d')].reset_index(drop=True)
if df_sb_same_day_alerts.shape[0] > 0:
    max_alert_id = df_sb_same_day_alerts['alert_id'].str.slice(9).astype('Int32').max()
    # generate and assign incremental alert ids to each alert
    df_sb_dp2_final.drop(columns=['alert_id'],inplace=True)
    df_sb_dp2_final['alert_id'] = pd.Series([datetime.datetime.today().strftime('%Y%m%d') + str(x).zfill(2) \
                    for x in range(max_alert_id+1,len(df_sb_dp2_final.index)max_alert_id+1)], index = df_sb_dp2_final.index)

# re-arrange and select columns
df_sb_dp2_final = df_sb_dp2_final[cols_df_final]

df_sb_dp2_final.to_csv(os.path.abspath(os.path.join(current_file_directory, f'../data/{folder_name}/alerts_data_final_{folder_name}.csv')), index=False, encoding='utf-8')
print(f'Final alerts data saved as csv file with shape: {df_sb_dp2_final.shape}')

print(f'It took {(datetime.datetime.now() - start_time).seconds} seconds to finish the DP2 script run')
