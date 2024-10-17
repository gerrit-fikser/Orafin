DECLARE
   v_table_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_table_exists
   FROM all_tables
   WHERE table_name = 'CON_WF_ADMIN_USERS'
   AND owner = 'XX_INTEGRATION_DEV';

   IF v_table_exists = 0 THEN
      EXECUTE IMMEDIATE 'create table CON_WF_ADMIN_USERS
							(username varchar2(100) not null,
							 application_id number not null,
                      workflow_static_id varchar2(100),
							 wf_administrator varchar2(1),
							 wf_owner varchar2(1),
							 creation_date date,
							 created_by varchar2(150),
							 last_update_date date,
							 last_updated_by varchar2(100),
                      CONSTRAINT CON_WF_ADMIN_USERS_U1 UNIQUE (USERNAME, APPLICATION_ID,WORKFLOW_STATIC_ID)
							 )';
   END IF;
END;
/