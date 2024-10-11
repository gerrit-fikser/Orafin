CREATE OR REPLACE TRIGGER xx_integration_dev.dwh_extract_setup_biu
BEFORE INSERT OR UPDATE ON xx_integration_dev.dwh_extract_setup
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