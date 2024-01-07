delete FROM  STORAGE_DATABASE.CPW_DATA.SQL_DEPLOYMENT_HISTORY
WHERE FILE_NAME in (
'1_clone_master_angler.sql',
'2_create_master_angler_table.sql',
'3_insert_date_from_temp_to_master.sql'
)
and sprint_folder_name = '2_sprint1_fy25'