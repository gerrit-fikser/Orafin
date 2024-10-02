DECLARE
   v_table_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_table_exists
   FROM all_tables
   WHERE table_name = 'ENS_TREE_VERSIONS'
   AND owner = 'XX_INTEGRATION_DEV';

   IF v_table_exists = 0 THEN
      EXECUTE IMMEDIATE 'create table ENS_TREE_VERSIONS (
				ID NUMBER GENERATED ALWAYS AS IDENTITY,
            tree_name varchar2(100) NOT NULL,
            tree_code varchar2(100) NOT NULL,
				tree_version_name varchar2(100) NOT NULL,
            effective_start_date date NOT NULL,
            effective_end_date date,
            current_flag varchar2(1),
            upload_flag varchar2(1),
				creation_date date,
				created_by varchar2(30),
				last_update_date date,
				last_updated_by varchar2(30),
            CONSTRAINT tree_version_uk UNIQUE (tree_code, tree_version_name)
				)';
   END IF;
END;
/

