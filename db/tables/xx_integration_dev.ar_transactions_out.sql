DECLARE
   v_table_exists NUMBER;
BEGIN
   SELECT COUNT(*)
   INTO v_table_exists
   FROM all_tables
   WHERE table_name = 'AR_TRANSACTIONS_OUT'
   AND owner = 'XX_INTEGRATION_DEV';

   IF v_table_exists = 0 THEN
      EXECUTE IMMEDIATE 'CREATE TABLE XX_INTEGRATION_DEV.AR_TRANSACTIONS_OUT 
      (	ID NUMBER GENERATED ALWAYS AS IDENTITY,
			MSG_ID RAW(32), 
			BLOB_DATA BLOB, 
			BASE64 CLOB COLLATE USING_NLS_COMP, 
			STATUS VARCHAR2(15 BYTE) COLLATE USING_NLS_COMP, 
			CREATION_DATE DATE, 
			LAST_UPDATE_DATE DATE, 
			ERP_DOCUMENT_ID NUMBER, 
			ERP_IMPORT_REQ_ID NUMBER, 
			WF_INSTANCE_ID NUMBER, 
			ERP_AUTOINVOICE_REQ_ID NUMBER
		)';

   END IF;
END;
/