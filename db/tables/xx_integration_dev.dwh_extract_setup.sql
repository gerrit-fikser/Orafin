DECLARE
   v_table_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_table_exists
   FROM all_tables
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV';

   IF v_table_exists = 0 THEN
      EXECUTE IMMEDIATE 'create table DWH_EXTRACT_SETUP (
				ID NUMBER GENERATED ALWAYS AS IDENTITY,
				name varchar2(50) not null,
				description varchar2(200),
				bi_report varchar2(200),
				bi_parameters VARCHAR2(4000) CHECK (bi_parameters IS JSON),
            automation_static_id varchar2(100),
            scheduler_job_name varchar2(100),
				store_data varchar2(1),
				store_data_table varchar2(50),
				file_name varchar2(100),
				file_type varchar2(10),
				compress_flag varchar2(1),
				oci_bucket varchar2(50),
				oci_bucket_path varchar2(150),
				oci_bucket_prefix varchar2(50),
				oci_archive varchar2(1),
				oci_archive_path varchar2(50),
				creation_date date,
				created_by varchar2(30),
				last_update_date date,
				last_updated_by varchar2(30),
            CONSTRAINT "DWH_EXTRACT_SETUP_U1" UNIQUE ("NAME")
				)';
   END IF;
END;
/

DECLARE
   v_column_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_column_exists
   FROM all_tab_cols
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV'
   and column_name = 'FILE_NAME';

   IF v_column_exists = 0 THEN
	EXECUTE IMMEDIATE 'alter table DWH_EXTRACT_SETUP add FILE_NAME varchar2(100)';
   END IF;
END;
/

DECLARE
   v_column_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_column_exists
   FROM all_tab_cols
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV'
   and column_name = 'FILE_TYPE';

   IF v_column_exists = 0 THEN
	EXECUTE IMMEDIATE 'alter table DWH_EXTRACT_SETUP add FILE_TYPE varchar2(10)';
   END IF;
END;
/
DECLARE
   v_column_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_column_exists
   FROM all_tab_cols
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV'
   and column_name = 'COMPRESS_FLAG';

   IF v_column_exists = 0 THEN
	EXECUTE IMMEDIATE 'alter table DWH_EXTRACT_SETUP add COMPRESS_FLAG varchar2(1)';
   END IF;
END;
/
DECLARE
   v_column_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_column_exists
   FROM all_tab_cols
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV'
   and column_name = 'AUTOMATION_STATIC_ID';

   IF v_column_exists = 0 THEN
	EXECUTE IMMEDIATE 'alter table DWH_EXTRACT_SETUP add AUTOMATION_STATIC_ID varchar2(100)';
   END IF;
END;
/

DECLARE
   v_column_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_column_exists
   FROM all_tab_cols
   WHERE table_name = 'DWH_EXTRACT_SETUP'
   AND owner = 'XX_INTEGRATION_DEV'
   and column_name = 'SCHEDULER_JOB_NAME';

   IF v_column_exists = 0 THEN
	EXECUTE IMMEDIATE 'alter table DWH_EXTRACT_SETUP add SCHEDULER_JOB_NAME varchar2(100)';
   END IF;
END;
/
