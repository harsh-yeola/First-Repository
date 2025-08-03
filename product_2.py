# function 1
def comparison_rule_parser(df_rule_criteria:pd.DataFrame) -> pd.DataFrame:
    """
    Parses the rule criteria DataFrame to generate conditions for each rule_id.
    """

    # Map logical and math operators to Python Pandas syntax
    df_rule_criteria['logical_operator_pd'] = df_rule_criteria['logical_operator'].map({'AND': '&', 'OR': '|'})
    df_rule_criteria['mathematical_operator_pd'] = df_rule_criteria['mathematical_operator'].map({
            '=': '==', '<>': '!=', '<': '<', '>': '>', '<=': '<=', '>=': '>='})

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
                        rule_condition = f"{row['rule_criteria_lhs']}.str.lower().str.strip() {row['mathematical_operator_pd']} '{row['rule_criteria_rhs'].strip().lower()}'"
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
# function 2
def get_categories(df_rule_data:pd.DataFrame, rule_id:str, column_name:str) -> list:
    """
    Retrieves all the categories associated with a given rule ID from the rule data DataFrame.

    """
    column_elements = df_rule_data.loc[df_rule_data['rule_id'] == rule_id, column_name].item()
    return [c.lower().strip() for c in column_elements.split(';') if c.strip() != '']
# function 3
def filter_logic(df_landingpage:pd.DataFrame, df_rule_data:pd.DataFrame, rule_id:str) -> pd.DataFrame:
    """
    Apply the filter logic to the landing page data based on the specified rule ID.
    """
    
    df_filtered = df_landingpage.query(f"process_id.notna() and process_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'process_id')}) \
                            & l1_risk_id.notna() and l1_risk_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'l1_risk_id')}) \
                            & l2_risk_id.notna() and l2_risk_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'l2_risk_id')}) \
                            & l3_risk_id.notna() and l3_risk_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'l3_risk_id')}) \
                            & apply_org_bf_l1_id.notna() and apply_org_bf_l1_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'apply_org_bf_l1_id')}) \
                            & apply_org_bf_l2_id.notna() and apply_org_bf_l2_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'apply_org_bf_l2_id')}) \
                            & apply_org_bf_l3_id.notna() and apply_org_bf_l3_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'apply_org_bf_l3_id')}) \
                            & country_org_id.notna() and country_org_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'country_org_id')}) \
                            & legal_entity_org_id.notna() and legal_entity_org_id.str.lower().str.strip().isin({get_categories(df_rule_data,rule_id,'legal_entity_org_id')}) \
                            & ({df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'condition'].item()}) \
                            ")
        
    if df_filtered.shape[0] > 0:
        df_filtered.loc[:,'rule_id'] = rule_id
        df_filtered.loc[:,'rule_name'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_name'].item()
        df_filtered.loc[:,'alert_date'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %X')
        df_filtered.loc[:,'rule_description'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_description'].item()
        df_filtered.loc[:,'type_of_alert'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'type_of_alert'].item()
        df_filtered.loc[:,'alert_status'] = 'Open'
        df_filtered.loc[:,'criteria'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'condition'].item()
        df_filtered.loc[:,'rule_version'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_version'].item()
        df_filtered.loc[:,'rule_approved_date'] = df_rule_data.loc[df_rule_data['rule_id'] == rule_id,'rule_approved_date'].item()
        df_filtered.loc[:,'dateofloading'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        df_filtered.loc[:,'cob'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')


    return df_filtered
# function 4
def alert_generation(df_landingpage: pd.DataFrame, df_rule_data_full: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """    Generate alerts based on the landing page data and rule data.
    Args:
        df_landingpage (pd.DataFrame): DataFrame containing landing page data.
        df_rule_data_full (pd.DataFrame): DataFrame containing rule data with parsed conditions.
    Returns:
        df_final (pd.DataFrame): DataFrame containing generated alerts.
        df_rules_execution_status (pd.DataFrame): DataFrame containing rules execution status.
    """
    
    ls_rule_ids = df_rule_data_full['rule_id'].to_list()

    print(ls_rule_ids)
    # Initialize an empty DataFrame to hold merged results
    df_merged_running = pd.DataFrame()

    # Initialize an empty DataFrame to hold rules execution status
    df_rules_execution_status = pd.DataFrame(columns = ['rule_id', 'rule_name', 'rule_description', 'rule_version', 'rule_approved_date', \
                    'rule_executed_date', 'frequency', \
                    'rule_run_start_timestamp', 'no_of_alerts_generated', 'rule_executed_user',\
                    'rule_run_status', 'reason_for_run_failure',\
                    'dateofloading', 'cob'])


    for rule_id in ls_rule_ids:
        df_filtered = pd.DataFrame()
        print(f'Processing rule ID: {rule_id}')
        try:
            df_filtered = filter_logic(df_landingpage, df_rule_data_full, rule_id)
            # # concatination / union of all alerts genrated for each rule_id
            df_merged_running = pd.concat([df_merged_running, df_filtered], ignore_index=True)
            print(f'merged dataframe shape: {df_merged_running.shape}')

            rule_id_execution_status = 'Completed'
            failure_reason = ''
            print(f'Rule ID {rule_id} executed successfully with {df_filtered.shape[0]} alerts generated.')
        except Exception as e:
            print(f"Exception occurred for rule ID {rule_id}: {e}")
            rule_id_execution_status = 'Failed'
            failure_reason = f'{e}'
            # continue
        finally:
            # rules executed today
            ls_rules_execution_status = [rule_id, df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_name'].item(), \
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_description'].item(), \
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_version'].item(), \
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'rule_approved_date'].item(),\
                        datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %X'),\
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'frequency'].item(),
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'last_updated_timestamp'].item(),\
                        df_filtered.shape[0],
                        df_rule_data_full.loc[df_rule_data_full['rule_id'] == rule_id,'last_updated_by'].item(),\
                        rule_id_execution_status,
                        failure_reason,
                        datetime.datetime.now(datetime.timezone.utc).date().strftime('%Y-%m-%d'),\
                        datetime.datetime.now(datetime.timezone.utc).date().strftime('%Y-%m-%d')
                        ]

            df_rules_execution_status.loc[len(df_rules_execution_status)] = ls_rules_execution_status
            print(f'Rule ID {rule_id} execution status: {rule_id_execution_status}')


    df_merged_running['alert_id'] = pd.Series([datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d') + str(x).zfill(2) \
            for x in range(1,len(df_merged_running.index)+1)], index = df_merged_running.index)

    df_final = df_merged_running[cols_df_final]

    return df_final, df_rules_execution_status
