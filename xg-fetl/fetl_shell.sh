#!/bin/bash
tables=(event_pollution_source event_message task_log thirdparty_send_log task_view_log push_target_and_pollution_source push_message push_answer_po task_receive_log thirdparty_push_task score_calculation extract_check_key report enterprise_policy_list factory_dlbj_push_record area compliance_feedback_record event_push_rule city_border_tenant tenant_dynamic thirdparty_push_event_log scoring_items dict_item problem_mold problem_mold_description thirdparty_push_rule tenant_area pollution_type pollution_template feedback_template heavy_area_station_rule pollution_constant pollution_type_details extraction_rules tenant_dynamic_tag event_timeout_rule dict_catalog pollution_category client_certain_pollution thirdparty_receive_log certain_pollution_source test_monitor flyway_schema_history fixed_source_template fixed_pollution_rule)
for tab_name in ${tables[@]}; do
  java -jar xg-fetl.jar selectexp /data/xg-fetl/pro_am_air_dust_source pro_am_air_dust_source.${tab_name} "select * from pro_am_air_dust_source.${tab_name} limit 100"
done

#!/bin/bash
tables=(event_pollution_source event_message task_log thirdparty_send_log task_view_log push_target_and_pollution_source push_message push_answer_po task_receive_log thirdparty_push_task score_calculation extract_check_key report enterprise_policy_list factory_dlbj_push_record area compliance_feedback_record event_push_rule city_border_tenant tenant_dynamic thirdparty_push_event_log scoring_items dict_item problem_mold problem_mold_description thirdparty_push_rule tenant_area pollution_type pollution_template feedback_template heavy_area_station_rule pollution_constant pollution_type_details extraction_rules tenant_dynamic_tag event_timeout_rule dict_catalog pollution_category client_certain_pollution thirdparty_receive_log certain_pollution_source test_monitor flyway_schema_history fixed_source_template fixed_pollution_rule)
for tab_name in ${tables[@]}; do
  java -jar xg-fetl.jar impt /data/xg-fetl/pro_am_air_dust_source/pro_am_air_dust_source.${tab_name} pro_am_air_dust_source.${tab_name}
done

nohup sh pro_am_air_dust_source_exp.sh >>pro_am_air_dust_source_exp.log 2>&1 &
nohup sh pro_am_air_dust_source_impt.sh >>pro_am_air_dust_source_impt.log 2>&1 &





# 改写


#!/bin/bash
tables=(table1 table2 table3)
for tab_name in ${tables[@]}; do
  # /data/xg-fetl/sysdba 为导出数据目录; SYSDBA.${tab_name} 为表的变量名
  # 部分导出
  java -jar xg-fetl.jar selectexp /data/xg-fetl/sysdba SYSDBA.${tab_name} "select * from SYSDBA.${tab_name} limit 100"
  #或者 全部导出
  java -jar xg-fetl.jar exp /DATA1/ljc/xg-fetl/SYSDBA SYSDBA.${tab_name}
done



#!/bin/bash
tables=(table1 table2 table3)
for tab_name in ${tables[@]}; do
  # 需要在目标端建立同样的表名及表结构; SYSDBA.${tab_name} 为表的变量名
  java -jar xg-fetl.jar impt /data/xg-fetl/sysdba/SYSDBA.${tab_name} SYSDBA.${tab_name}
done