# creating folder to save output files
folder_name = f'{datetime.datetime.today().date().strftime('%Y%m%d')}'
if not os.path.exists(f'./{folder_name}'): 
    os.mkdir(f'./{folder_name}')
    print(f"Folder './{folder_name}' created successfully.")
else:
    print(f"Folder './{folder_name}' already exists.")

# get list of the Rule Ids to be executed today, based on various criterions

run_date = datetime.date.today()

# daily rule ids
daily_rule_ids = df_drm_rules.loc[(df_drm_rules['alert_generation_status'].str.lower() == 'active') & \
                            (df_drm_rules['frequency'].str.lower().isin(['daily']))  ]\
                            ['rule_id'].to_list()

# ad-hoc rule ids
ad_hoc_rule_ids = df_drm_rules.loc[(df_drm_rules['alert_generation_status'].str.lower() == 'active') & \
                            (df_drm_rules['frequency'].str.lower().isin(['ad-hoc'])) & \
                            (df_drm_rules['execute_now_flag'].str.lower() == 'yes') ]\
                            ['rule_id'].to_list()

# weekly rule ids
weekly_rule_ids = df_drm_rules.loc[(df_drm_rules['alert_generation_status'].str.lower() == 'active') & \
                            (df_drm_rules['frequency'].str.lower() == 'weekly') & \
                            (run_date.strftime("%A") == 'Friday')    ]\
                            ['rule_id'].to_list()

# monthly rule ids
monthly_rule_ids = df_drm_rules.loc[(df_drm_rules['alert_generation_status'].str.lower() == 'active') & \
                            (df_drm_rules['frequency'].str.lower() == 'monthly') & \
                            (run_date == (run_date.replace(day=1) + pd.offsets.BMonthEnd()).date())    ]\
                            ['rule_id'].to_list()
                            
# quarterly rule ids
quarterly_rule_ids = df_drm_rules.loc[(df_drm_rules['alert_generation_status'].str.lower() == 'active') & \
                            (df_drm_rules['frequency'].str.lower() == 'quarterly') & \
                            (run_date == get_end_of_quarter(run_date).date())    ]\
                            ['rule_id'].to_list()

# all rules
ls_rule_ids = daily_rule_ids + ad_hoc_rule_ids + weekly_rule_ids + monthly_rule_ids + quarterly_rule_ids
# print(f'Number of rules to execute today: {len(ls_rule_ids)}')
# print(ls_rule_ids)
print(f'Number of rules to execute today: {len(ls_rule_ids)}', f"The rule Ids to run: {', '.join([str(a) for a in sorted(ls_rule_ids)])}",sep='\n')

  
# get all rule ids from drm rules dataframe where alert_generation_status = active 
ls_rule_ids = df_drm_rules[df_drm_rules['alert_generation_status'].str.lower() == 'active']['rule_id'].to_list()
df_merged_running = pd.DataFrame()
df_rules_execution_status = pd.DataFrame(columns = ['rule_id', 'rule_name', 'rule_description', 'rule_version', \
                'rule_approved_date', 'rule_executed_date', 'frequency', \
                'rule_run_start_timestamp', 'no_of_alerts_generated', 'rule_executed_user',\
                'rule_run_status', 'reason_for_run_failure',\
                'dateofloading', 'cob' ])

cols_df_final = ['alert_id', 'alert_date', 'type_of_alert', 'rule_id','rule_name', 'rule_description', 'rule_version','criteria', 'risk_type', \
    'rating_type', 'factor', 'country', 'assessment_unit', 'segment', 'sub_segment', 'current_value', 'previous_value', \
    'current_rating_calculated_date', 'previous_rating_calculated_date' ,'alert_status', 'dateofloading', 'cob']
columns_for_skey  = ['risk_type', 'rating_type', 'factor', 'country','assessment_unit', 'segment', 'sub_segment']

for rule_id in ls_rule_ids:
    
    print(f'Running Rule Id {rule_id}')
    # get comparison operator from drm rules dataframe
    t1 = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'mathematical_operator'].item()
    print(t1)
    rule_id_execution_status = ''
    failure_reason = ''
    df_filtered = pd.DataFrame()
  
    try:
        if df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'custom_udl_field_two_input_flag'].item().lower() == 'yes':

            if t1 == '<>':
                try:
                    math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    != int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())
                except:
                    math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()].str.lower() \
                    != df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item().lower()
            if t1 == '=':
                try:
                    math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    == int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())
                except:
                    math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()].str.lower() \
                    == df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item().lower()
            if t1 == '<=':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    <= int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())
            if t1 == '<':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    < int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())
            if t1 == '>=':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    >= int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())
            if t1 == '>':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    > int(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item())                

        else:
            if t1 == '<>':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    != df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]
            if t1 == '=':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    == df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]
            if t1 == '<=':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    <= df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]
            if t1 == '<':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    < df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]
            if t1 == '>=':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    >= df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]
            if t1 == '>':
                math_operation = df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_one'].item()] \
                    > df_dp_1[df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'udl_data_two'].item()]

    except Exception as e:
        math_operation = None
        # rule_id_execution_status = f'Failed; with error {e}'
        rule_id_execution_status = 'Failed'
        failure_reason = f'{e}'
        print(f'the error in math_operation creation is {e}')
        # continue  

    try:

        if math_operation is not None:
            try:
                df_filtered = df_dp_1[(df_dp_1['risk_type'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'risk_type'].item().split(';')])) \
                        & (df_dp_1['rating_type'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rating_type'].item().split(';')]))\
                        & (df_dp_1['factor'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'factor'].item().split(';')]))\
                        & (df_dp_1['country'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'country'].item().split(';')]))\
                        & (df_dp_1['assessment_unit'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'assessment_unit'].item().split(';')]))\
                        & (df_dp_1['segment'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'segment'].item().split(';')]))\
                        & (df_dp_1['sub_segment'].str.lower().isin([c.lower() for c in df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'sub_segment'].item().split(';')])) \
                        & (math_operation)
                        ]
                
                rule_id_execution_status = 'Completed'
                # failure_reason = 'None'
                failure_reason = ''
            except Exception as e:
                # df_filtered = pd.DataFrame()
                rule_id_execution_status = 'Failed'
                failure_reason = f'{failure_reason} ; {e}'
                print(f'the error in generating alerts is {e}') 

        # rules executed today
        ls_rules_execution_status = [rule_id, df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_name'].item(), \
                    df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_description'].item(), \
                    df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_version'].item(), \
                    df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_approved_date'].item(),\
                    datetime.datetime.today().strftime('%Y-%m-%d %X'),\
                    df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'frequency'].item(),                    
                    np.where(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'frequency'].str.lower() == 'ad-hoc', df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'last_updated_timestamp'],datetime.datetime.today().strftime('%Y-%m-%d %X')).item(),\
                    df_filtered.shape[0],
                    np.where(df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'frequency'].str.lower() == 'ad-hoc', df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'last_updated_by'],'Default_System').item(),\
                    rule_id_execution_status,
                    failure_reason,
                    datetime.datetime.today().date().strftime('%Y-%m-%d'),\
                    datetime.datetime.today().date().strftime('%Y-%m-%d')
                    ]

        df_rules_execution_status.loc[len(df_rules_execution_status)] = ls_rules_execution_status

        if df_filtered.shape[0] == 0:
            print('alerts not generated')
            df_filtered.loc[0,'rule_id'] = rule_id
            df_filtered.loc[0,'rule_name'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_name'].item()
            df_filtered.loc[0,'alert_date'] = datetime.datetime.today().strftime('%Y-%m-%d %X') 
            df_filtered.loc[0,'rule_description'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_description'].item()
            df_filtered.loc[0,'type_of_alert'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'type_of_alert'].item()
            df_filtered.loc[0,'alert_status'] = 'Not Generated'
            df_filtered.loc[0,'risk_type'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'risk_type'].item()
            df_filtered.loc[0,'rating_type'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rating_type'].item()
            df_filtered.loc[0,'country'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'country'].item()
            df_filtered.loc[0,'segment'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'segment'].item()
            df_filtered.loc[0,'factor'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'factor'].item()
            df_filtered.loc[0,'sub_segment'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'sub_segment'].item()
            df_filtered.loc[0,'assessment_unit'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'assessment_unit'].item()
            df_filtered.loc[0,'criteria'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'criteria'].item()
            df_filtered.loc[0,'rule_version'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_version'].item()
            df_filtered.loc[0,'rule_approved_date'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_approved_date'].item()

        else:
            df_filtered.loc[:,'rule_id'] = rule_id
            df_filtered.loc[:,'rule_name'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_name'].item()
            df_filtered.loc[:,'alert_date'] = datetime.datetime.today().strftime('%Y-%m-%d %X')
            df_filtered.loc[:,'rule_description'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_description'].item()
            df_filtered.loc[:,'type_of_alert'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'type_of_alert'].item()
            df_filtered.loc[:,'alert_status'] = 'Open'
            df_filtered.loc[:,'criteria'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'criteria'].item()
            df_filtered.loc[:,'rule_version'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_version'].item()
            df_filtered.loc[:,'rule_approved_date'] = df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_approved_date'].item()

        # rules executed today
        # ls_rules_execution_status = [rule_id, df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_name'].item(), \
        #             df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_description'].item(), \
        #             df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_version'].item(), \
        #             df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'rule_approved_date'].item(),\
        #             datetime.datetime.today().strftime('%Y-%m-%d %X'),\
        #             df_drm_rules.loc[df_drm_rules['rule_id'] == rule_id,'frequency'].item(),
        #             rule_id_execution_status,
        #             df_filtered.shape[0],
        #             datetime.datetime.today().date().strftime('%Y-%m-%d'),\
        #             datetime.datetime.today().date().strftime('%Y-%m-%d')
        #             ]

        # df_rules_execution_status.loc[len(df_rules_execution_status)] = ls_rules_execution_status

        print(f'filtered dataframe shape: {df_filtered.shape}')

        # # concatination / union of all alerts genrated for each rule_id
        df_merged_running = pd.concat([df_merged_running, df_filtered], ignore_index=True)
        print(f'merged dataframe shape: {df_merged_running.shape}')

    except Exception as e:
        print(f'the error in creating DP2 tables is {e}') 
        
try:
   
    # alerts generated
    df_merged_1 = df_merged_running.loc[df_merged_running['alert_status'] == 'Open']
    
    # fixing the data types 
    df_merged_1 = (df_merged_1
        .assign(rule_id = df_merged_1.rule_id.astype('int'),
        rule_version = df_merged_1.rule_version.astype('int32'),
        current_rating_calculated_date = df_merged_1.current_rating_calculated_date.astype('datetime64[s]'),
        previous_rating_calculated_date = df_merged_1.previous_rating_calculated_date.astype('datetime64[s]'),
        dateofloading = df_merged_1.dateofloading.astype('datetime64[s]'),
        cob = df_merged_1.cob.astype('datetime64[s]')
            )
        )

    # alerts not-generated
    print('alerts not-generated')
    df_merged_2 = df_merged_running.loc[df_merged_running['alert_status'] != 'Open']
    df_merged_2['alert_id'] = '-'
    df_merged_2 = df_merged_2[cols_df_final]
    print(f'alerts not generated dataframe shape: {df_merged_2.shape}')
    df_merged_2.to_csv(f'./{folder_name}/alerts_not_generated_{folder_name}.csv',index=False)
    
    # # #  hashing 8 columns to select only one latest record 
    # df_merged_1['row_id_skey'] = df_merged_1.apply(lambda df: generate_hash(df, columns_for_skey), axis=1)
    # df_merged_1['row_num'] = df_merged_1.sort_values(by='current_rating_calculated_date',ascending=False)\
    #                                 .groupby(['row_id_skey']).cumcount() + 1
    # df_merged_1 = df_merged_1.loc[df_merged_1['row_num'] == 1]

    # generate and assign alert ids to each alert
    df_merged_1['alert_id'] = pd.Series([datetime.datetime.today().strftime('%Y%m%d') + str(x).zfill(2) \
                    for x in range(1,len(df_merged_1.index)+1)], index = df_merged_1.index)
                                
    print(f'merged before final dataframe shape: {df_merged_1.shape}')

    # re-arrange columns
    df_final = df_merged_1[cols_df_final]
    
    # alerts
    print(f'final dataframe shape: {df_final.shape}')
    df_final.to_csv(f'./{folder_name}/final_alerts_output_{folder_name}.csv',index=False)

    # rules executed
    print(f'rules execution status dataframe shape: {df_rules_execution_status.shape}')
    df_rules_execution_status.to_csv(f'./{folder_name}/rules_executed_today_{folder_name}.csv',index=False)

except Exception as e:
        print(f'the error in creating and final tables is {e}')
