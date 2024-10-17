CREATE OR REPLACE TRIGGER "XX_INTEGRATION_DEV"."CON_WF_ADMIN_USERS_BIU" 
BEFORE INSERT OR UPDATE ON xx_integration_dev.con_wf_admin_users
FOR EACH ROW
BEGIN
IF inserting THEN
    :new.created_by := coalesce(sys_context('APEX$SESSION','APP_USER'),user);
    :new.creation_date := SYSDATE;
END IF;

:new.last_updated_by := coalesce(sys_context('APEX$SESSION','APP_USER'),user);
:new.last_update_date := SYSDATE;

END;
/