CREATE OR REPLACE PACKAGE BODY "XX_INTEGRATION_DEV"."AR_TRANSACTIONS_PKG" as
--*****************************************************************************
--Module      : AR - Receivables Transactions
--Type        : PL/SQL - Package
--Author      : Aldis Lagzdins
--Version     : 1.0
--
--
-- Description: AR Module specific package to handle transaction validation, 
-- mapping, formating and import to Oracle ERP.
--
-- *****************************************************************************

-- -------------------------------------------------
-- Change log
-- Date        Author          Version     Comment
----------------------------------------------------
-- 11.07.2024  Aldis Lagzdins  1.0         Created
-- -------------------------------------------------

/*===========================================
============= Global Variables ==============
===========================================*/

g_pkg           constant varchar2(20)  := 'AR_TRANSACTIONS_PKG';
g_proc                   varchar2(50)  := '';
g_step                   varchar2(100) := '';
g_callback_url  constant varchar2(200) := 'https://g5c283cad42763c-gnlf29ztv3s1am8v.adb.eu-frankfurt-1.oraclecloudapps.com/ords/integration_dev/erpCallBack/receiveEssJobStatus';

/*===========================================
==== PRIVATE Procedures and Functions =======
===========================================*/

--Log and debug
PROCEDURE log(
    p_msg_id  in dbo_msg_inbound.msg_id%type,
    p_message in varchar2,
    p_status  in varchar2 default 'ERROR'
) IS 
    l_msg_pref varchar2(200);
BEGIN
    l_msg_pref := g_pkg||'.'||g_proc||'>>'||g_step||':';
    if p_msg_id is not null then --does not support error loging without MSG_ID
        dbo_msg_pkg.log_error(p_msg_id, substr(l_msg_pref||p_message,1,2000), p_status);
        if p_message is not null then
            dbo_msg_pkg.debug_msg(p_msg_id,'AR',substr(l_msg_pref||p_message,1,2000));
        end if;
    end if;
END log;

--Update status in all AR integration related tables
PROCEDURE update_status(
    p_msg_id    in dbo_msg_inbound.msg_id%type,
    p_status    in varchar2,
    p_ar_out_id in number default null
) IS
BEGIN
    
    if p_msg_id is not null then
        
        --update message status
        dbo_msg_pkg.update_status(p_msg_id, p_status);
        
        --update ar transaction status
        update ra_interface_lines_all 
        set x_apex_status = p_status,
            x_last_update_date = sysdate
        where x_msg_id = p_msg_id;
        
        update ra_interface_distributions_all 
        set x_apex_status = p_status,
            x_last_update_date = sysdate
        where x_msg_id = p_msg_id;

        --update ar transaction out status
        update ar_transactions_out 
        set status = p_status,
            last_update_date = sysdate
        where msg_id = p_msg_id
        and id = nvl(p_ar_out_id,id);

    end if;
EXCEPTION
    WHEN others THEN
        log(p_msg_id,substr(SQLERRM,1,2000));
END;

FUNCTION BLOB_TO_BASE64(p_blob IN BLOB)
RETURN CLOB
IS
  l_clob         CLOB;
  l_blob_len     NUMBER := DBMS_LOB.getlength(p_blob);
  l_chunk_size   INTEGER := 32000; -- Define the size of each chunk to read
  l_buffer       RAW(32000);
  l_output       CLOB;
  l_encoded_buf  RAW(32000);
  l_start_pos    INTEGER := 1;
BEGIN
  -- Initialize the output CLOB
  DBMS_LOB.createtemporary(l_output, TRUE);

  -- Process the BLOB in chunks
  WHILE l_start_pos <= l_blob_len LOOP
    -- Read a chunk of the BLOB
    DBMS_LOB.read(p_blob, l_chunk_size, l_start_pos, l_buffer);
    -- Encode the chunk using Base64
    l_encoded_buf := UTL_ENCODE.BASE64_ENCODE(l_buffer);

    -- Convert the RAW result to a CLOB
    l_clob := UTL_RAW.cast_to_varchar2(l_encoded_buf);
    -- Append the encoded chunk to the output CLOB
    DBMS_LOB.writeappend(l_output, LENGTH(l_clob), l_clob);
    -- Move to the next chunk
    l_start_pos := l_start_pos + l_chunk_size;
  END LOOP;

  RETURN l_output;
EXCEPTION
  WHEN OTHERS THEN
    -- Handle any exceptions
    IF DBMS_LOB.istemporary(l_output) = 1 THEN
      DBMS_LOB.freetemporary(l_output);
    END IF;
    RAISE;
END BLOB_TO_BASE64;

--Create CSV based on interface table name
PROCEDURE generate_dynamic_csv (
    p_msg_id     in  dbo_msg_inbound.msg_id%type,
    p_table_name in  user_tab_columns.table_name%type,
    p_csv_output out clob
) IS
    l_sql                varchar2(32767);
    l_col_list           varchar2(32767);
    l_col_header         varchar2(32767);
    l_csv_output         clob;
    l_first_col          boolean         := TRUE;
    l_add_header         boolean         := FALSE;

    l_col_sep      CONSTANT VARCHAR2(1)  := ',';
    l_col_concat   CONSTANT VARCHAR2(10) := q'[||','||]';

    CURSOR c_columns IS
        SELECT COLUMN_NAME
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = UPPER(p_table_name)
        AND COLUMN_NAME NOT LIKE 'X\_%' ESCAPE '\'  
        ORDER BY COLUMN_ID;

    c_csv SYS_REFCURSOR;
    
    l_data_row VARCHAR2(4000);
    l_col_count INTEGER := 0;

BEGIN

     --Initialize CLOB to collect CSV rows
    DBMS_LOB.CREATETEMPORARY(l_csv_output, TRUE);

    -- Construct the CSV header
    FOR rec IN c_columns LOOP
        IF l_first_col THEN
            l_first_col := FALSE;
        ELSE
            l_col_list := l_col_list || l_col_concat;
            l_col_header := l_col_header || l_col_sep;
        END IF;
        l_col_list := l_col_list || rec.COLUMN_NAME;
        l_col_header := l_col_header || rec.COLUMN_NAME;
    END LOOP;

    -- Construct the SQL query to fetch data
    l_sql := 'SELECT '|| l_col_list ||' as data '||
             ' FROM ' || p_table_name ||
             ' WHERE x_msg_id = '''||p_msg_id||'''';
    
    --add heaader
    IF l_add_header THEN
        DBMS_LOB.WRITEAPPEND(l_csv_output, LENGTH(l_col_header), l_col_header || CHR(10));
    END IF;

    -- Open the cursor and fetch data
    OPEN c_csv FOR l_sql;
    LOOP
        FETCH c_csv INTO l_data_row;
        EXIT WHEN c_csv%NOTFOUND;

        DBMS_LOB.WRITEAPPEND(l_csv_output, LENGTH(l_data_row||'END' || CHR(10) ), l_data_row||'END' || CHR(10) );
    END LOOP;
    CLOSE c_csv;

    -- Free temporary CLOB
    IF DBMS_LOB.ISTEMPORARY(p_csv_output) = 1 THEN
        DBMS_LOB.FREETEMPORARY(p_csv_output);
    END IF;

    p_csv_output := l_csv_output;

EXCEPTION
    WHEN OTHERS THEN
        IF c_csv%ISOPEN THEN
            CLOSE c_csv;
        END IF;

        -- Free temporary CLOB
        IF DBMS_LOB.ISTEMPORARY(p_csv_output) = 1 THEN
            DBMS_LOB.FREETEMPORARY(p_csv_output);
        END IF;

        RAISE;
END generate_dynamic_csv;

 --Store zip file blob and base64 ready for sending to ERP
PROCEDURE save_out_file(
    p_msg_id         in dbo_msg_inbound.msg_id%type,
    p_zip_file       in ar_transactions_out.blob_data%type,
    p_wf_instance_id in ar_transactions_out.wf_instance_id%type)
IS
    l_file_as_base64 clob;

BEGIN
    -- Initialize CLOB to store base64 of the zip file
    DBMS_LOB.CREATETEMPORARY(l_file_as_base64, TRUE);
    -- base64 encode
    l_file_as_base64:= BLOB_TO_BASE64(p_zip_file);

    --  Remove New Line and Carriage Return in base64
    l_file_as_base64 := REPLACE(l_file_as_base64, CHR(10), '');
    l_file_as_base64 := REPLACE(l_file_as_base64, CHR(13), '');
        
    MERGE INTO ar_transactions_out ato
    USING (SELECT p_msg_id AS msg_id, 
                  p_zip_file AS blob_data, 
                  l_file_as_base64 AS base64, 
                  'NEW' AS status, 
                  sysdate AS creation_date, 
                  sysdate AS last_update_date, 
                  p_wf_instance_id AS wf_instance_id
            FROM dual) src
            ON (ato.msg_id = src.msg_id)
    WHEN MATCHED THEN 
        UPDATE 
        SET ato.blob_data = src.blob_data,
            ato.base64 = src.base64,
            ato.status = src.status,
            ato.last_update_date = src.last_update_date,
            ato.wf_instance_id = src.wf_instance_id
    WHEN NOT MATCHED THEN 
        INSERT (msg_id, blob_data, base64, status, creation_date, last_update_date, wf_instance_id)
        VALUES (src.msg_id, src.blob_data, src.base64, src.status, src.creation_date, src.last_update_date, src.wf_instance_id);


    -- Free temporary CLOB
    IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_file_as_base64);
    END IF;

    EXCEPTION
        WHEN others THEN
        -- Free temporary CLOB
        IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_file_as_base64);
        END IF;
        RAISE;
END save_out_file;

--COPY of DBO_ERP_UTILS_PKG.rest_upload_file_to_ucm
--Takes content zip file in base64 instead of CSV
PROCEDURE rest_upload_file_to_ucm(p_msg_id           IN dbo_msg_inbound.msg_id%TYPE
                                , p_base64_content   IN  CLOB
                                , p_document_account IN  VARCHAR2
								, p_module			 IN  VARCHAR2
								, p_rest_endpoint	 IN  VARCHAR2
                                , p_file_name        IN  VARCHAR2
                                , p_content_type     IN  VARCHAR2
                                , p_status_code      OUT NUMBER
                                , p_document_id      OUT NUMBER
                                , p_error_message    OUT VARCHAR2) 
IS
  --security and server
  l_username VARCHAR2(100);
  l_password VARCHAR2(100);
  l_servername VARCHAR2(100);
  --web_service variables
  l_body CLOB;
  l_response CLOB;
  l_json_obj JSON_OBJECT_T;

  e_erp_credentials exception;
BEGIN
  --init
  g_proc := 'rest_upload_file_to_ucm';
  p_error_message:='';
  p_document_id:=null;
  p_status_code:=null;
  

  -- Get the ERP credentials
  BEGIN
      CON_SECURITY_PKG.get_erp_credentials(l_username, l_password, l_servername);
      IF l_username is null THEN 
        RAISE e_erp_credentials;
      END IF;
  EXCEPTION
  WHEN e_erp_credentials THEN
      p_error_message := 'ERP credentions not found.';
      p_status_code := -1; --error
      RAISE; 
  WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      p_status_code := -1; --error
      RAISE; 
  END;
      
  -- Make the REST request
  APEX_WEB_SERVICE.g_request_headers.delete();
  APEX_WEB_SERVICE.g_request_headers(1).name:='Content-Type';
  APEX_WEB_SERVICE.g_request_headers(1).value:='application/json';
  BEGIN

    -- Create a new JSON object
    l_json_obj := JSON_OBJECT_T();

    -- Add key-value pairs to the JSON object
    l_json_obj.put('OperationName', 'uploadFileToUCM');
    l_json_obj.put('DocumentContent', p_base64_content);
    l_json_obj.put('DocumentAccount', p_document_account);
    l_json_obj.put('ContentType', p_content_type);
    l_json_obj.put('FileName', p_file_name);
    l_json_obj.put_null('DocumentId');

    -- Convert the JSON object to a CLOB string
    l_body := l_json_obj.to_clob();

    --call ERP REST API
    l_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => l_servername || p_rest_endpoint,
      p_http_method => 'POST',
      p_username => l_username,
      p_password => l_password,
      p_body => l_body
    );
    
    p_status_code:=APEX_WEB_SERVICE.g_status_code;  

    IF p_status_code=201 THEN
        APEX_JSON.parse(l_response);
        p_document_id := APEX_JSON.get_varchar2(p_path => 'DocumentId');
    ELSE
        p_error_message := 'Error calling REST:'||l_response;
    END IF;
	
  EXCEPTION
  WHEN OTHERS THEN
    p_error_message := SQLERRM;
    p_status_code:=APEX_WEB_SERVICE.g_status_code;
    RAISE;
  END;
END rest_upload_file_to_ucm;


--COPY of DBO_ERP_UTILS_PKG.rest_submit_ess_job_request
--In addition takes parameter callBackUrl parameter
PROCEDURE rest_submit_ess_job_request(p_msg_id           IN dbo_msg_inbound.msg_id%TYPE
                                    , p_module			 IN  VARCHAR2
                                    , p_rest_endpoint	 IN  VARCHAR2 
                                    , p_job_package_name IN  VARCHAR2 
                                    , p_job_def_name     IN  VARCHAR2 
                                    , p_parameter_str    IN  VARCHAR2 
                                    , p_callback_url     IN  VARCHAR2
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2)  
IS
  l_username VARCHAR2(100);
  l_password VARCHAR2(100);
  l_servername VARCHAR2(100);
  l_response CLOB;
  l_body CLOB;
  l_json_obj JSON_OBJECT_T;

BEGIN
  -- Get the ERP credentials
  BEGIN
    CON_SECURITY_PKG.get_erp_credentials(l_username, l_password, l_servername);
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials.' ||l_username||'  '||l_servername);   
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_submit_ess_job_request. Error credentials. '||p_error_message);   
  END;

  APEX_WEB_SERVICE.g_request_headers.delete();
  APEX_WEB_SERVICE.g_request_headers(1).name:='Content-Type';
  APEX_WEB_SERVICE.g_request_headers(1).value:='application/json';

  BEGIN

     -- Create a new JSON object
    l_json_obj := JSON_OBJECT_T();

    -- Add key-value pairs to the JSON object
    l_json_obj.put('OperationName', 'submitESSJobRequest');
    l_json_obj.put('JobPackageName', p_job_package_name);
    l_json_obj.put('JobDefName', p_job_def_name);
    l_json_obj.put('ESSParameters', p_parameter_str);
    l_json_obj.put('CallbackURL', p_callback_url);

    -- Convert the JSON object to a CLOB string
    l_body := l_json_obj.to_clob();

    l_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => l_servername || p_rest_endpoint, 
      p_http_method => 'POST',
      p_username => l_username,
      p_password => l_password,
      p_body => l_body);

  EXCEPTION
  WHEN OTHERS THEN
      p_error_message := 'Unexpected error: ' || SQLERRM;
      dbo_msg_pkg.debug_msg(p_msg_Id,p_module,'Apex_web_service fail: '||p_error_Message);
      dbo_msg_pkg.log_error(p_msg_id, p_error_message, 'ERROR');
  END;
  p_status_code:=APEX_WEB_SERVICE.g_status_code;  
  
  dbo_msg_pkg.debug_msg(p_msg_id,'CUR','Finished essJobRequest with status:'||p_status_code);

  --Parse Requestid
  BEGIN
    APEX_JSON.parse(l_response);
    p_requestid := APEX_JSON.get_varchar2(p_path => 'ReqstId');
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Requestid parsed:'|| p_requestid);          
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Unexpected error parsing reqstid: ' || SQLERRM;
      dbo_msg_pkg.debug_msg(p_msg_id, p_module, p_error_message);
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
  END;

END rest_submit_ess_job_request;


/*===========================================
===== PUBLIC Procedures and Functions =======
===========================================*/

--Store received payload (json). Invoked from receivables REST API
PROCEDURE store_json_payload (
    p_data          in  clob,
    p_msg_id        out dbo_msg_inbound.msg_id%type,
    p_status_code   out number,
    p_error_message out varchar2
) IS
    l_operation_name          varchar2(50);
    l_source_ref              dbo_msg_inbound.msg_source_ref%TYPE;
    l_is_msg_stored           varchar2(1) default 'N';

    l_message_type   constant dbo_msg_inbound.msg_type%TYPE := 'AR';

    e_invalid_operation EXCEPTION;
BEGIN
    p_msg_id := sys_guid();
    l_operation_name := json_value(p_data, '$.OperationName');
    l_source_ref := json_value(p_data, '$.SourceRef');

    dbo_msg_pkg.store_msg(p_msg_id => p_msg_id,
                          p_payload => p_data,
                          p_msg_type => l_message_type,
                          p_source_ref => l_source_ref);
    
    l_is_msg_stored := 'Y';

    --expected OperationName=ImportARTransaction, else abort the process
    IF nvl(l_operation_name,'NULL') != 'ImportARTransaction' THEN
        RAISE e_invalid_operation;
    END IF;

    --apex_json.parse(p_data);

    p_status_code := 201; --Created

EXCEPTION
    WHEN e_invalid_operation THEN
        p_error_message:='Invalid operation name. Expected ImportARTransaction, received ' || l_operation_name;
        dbo_msg_pkg.debug_msg(p_msg_id, l_message_type, p_error_message);
        dbo_msg_pkg.log_error(p_msg_id, p_error_message);
        p_status_code:=400;
    WHEN OTHERS THEN
        p_error_message:=SQLERRM;
        p_status_code:=400;
        if l_is_msg_stored = 'Y' then
          dbo_msg_pkg.debug_msg(p_msg_id, l_message_type, p_error_message);
          dbo_msg_pkg.log_error(p_msg_id, p_error_message);
        end if;

END store_json_payload;

--Validate received message before further processing. Invoked from WF.
PROCEDURE validate_message (
    p_msg_id out dbo_msg_inbound.msg_id%type
) IS
BEGIN

    select msg_id 
    into p_msg_id
    from dbo_msg_inbound
    where msg_type = 'AR'
    and msg_status = 'RECEIVED'
    order by creation_date
    fetch first 1 row only;

    dbo_msg_pkg.update_status(p_msg_id => p_msg_id, p_msg_status => 'PROCESSING');
EXCEPTION
    WHEN no_data_found THEN
        p_msg_id := null;
END;

--Insert message data into interface table. Invoked from WF.
PROCEDURE msg_json_to_interface(
    p_msg_id in dbo_msg_inbound.msg_id%type
) IS

BEGIN

    insert into ra_interface_lines_all (
                    X_MSG_ID,
                    X_APEX_STATUS,
                    X_CREATION_DATE,
                    X_LAST_UPDATE_DATE,
                    BU_NAME,
                    BATCH_SOURCE_NAME,
                    CUST_TRX_TYPE_NAME,
                    TERM_NAME,
                    TRX_DATE,
                    GL_DATE,
                    TRX_NUMBER,
                    ORIG_SYSTEM_BILL_CUSTOMER_ID,
                    ORIG_SYSTEM_BILL_ADDRESS_ID,
                    LINE_TYPE,
                    DESCRIPTION,
                    CURRENCY_CODE,
                    CONVERSION_TYPE,
                    CONVERSION_DATE,
                    CONVERSION_RATE,
                    AMOUNT,
                    QUANTITY,
                    INTERFACE_LINE_CONTEXT,
                    INTERFACE_LINE_ATTRIBUTE1,
                    INTERFACE_LINE_ATTRIBUTE2,
                    INTERFACE_LINE_ATTRIBUTE3,
                    TAX_CODE,
                    UOM_CODE,
                    TAX_EXEMPT_FLAG,
                    OVERRIDE_AUTO_ACCOUNTING_FLAG,
                    HEADER_ATTRIBUTE1,
                    HEADER_ATTRIBUTE2
                    )
    select p_msg_id, 'NEW', sysdate, sysdate,
           jt.BusinessUnitName, jt.TransactionBatchSourceName, jt.TransactionTypeName, jt.PaymentTerms,
           jt.TransactionDate, jt.AccountingDate, jt.TransactionNumber,
           jt.BillToCustomerId, jt.BillToCustomerAddressId, jt.TransactionLineType, jt.TransactionLineDescription,
           jt.CurrencyCode, jt.CurrencyConversionType, jt.CurrencyConversionDate, jt.CurrencyConversionRate,
           jt.TransactionLineAmount, jt.TransactionLineQuantity, jt.LineTransactionsFlexfieldContext, jt.LineTransactionsFlexfieldSegment1,
           jt.LineTransactionsFlexfieldSegment2, jt.LineTransactionsFlexfieldSegment3, jt.TaxClassificationCode, jt.UnitOfMeasure,
           jt.TaxExemptionFlag, jt.OverrideAutoAccountingFlag, jt.InvoiceTransactionsFlexfieldSegment1, jt.InvoiceTransactionsFlexfieldSegment2
    from dbo_msg_inbound dmi, 
         JSON_TABLE(
           dmi.msg_payload,
           '$'
           COLUMNS (
             OperationName VARCHAR2(100) PATH '$.OperationName',
             SourceRef VARCHAR2(100) PATH '$.SourceRef',
             NESTED PATH '$.Item[*]'
               COLUMNS (
                   BusinessUnitName VARCHAR2(240) PATH '$.BusinessUnitName',
                   TransactionBatchSourceName VARCHAR2(50) PATH '$.TransactionBatchSourceName',
                   TransactionTypeName VARCHAR2(20) PATH '$.TransactionTypeName',
                   PaymentTerms VARCHAR2(15) PATH '$.PaymentTerms',
                   TransactionDate DATE PATH '$.TransactionDate',
                   AccountingDate DATE PATH '$.AccountingDate',
                   TransactionNumber VARCHAR2(20) PATH '$.TransactionNumber',
                   BillToCustomerId NUMBER(18) PATH '$.BillToCustomerId',
                   BillToCustomerAddressId NUMBER(18) PATH '$.BillToCustomerAddressId',
                   TransactionLineType VARCHAR2(20) PATH '$.TransactionLineType',
                   TransactionLineDescription VARCHAR2(240) PATH '$.TransactionLineDescription',
                   CurrencyCode VARCHAR2(15) PATH '$.CurrencyCode',
                   CurrencyConversionType VARCHAR2(30) PATH '$.CurrencyConversionType',
                   CurrencyConversionDate DATE PATH '$.CurrencyConversionDate',
                   CurrencyConversionRate NUMBER PATH '$.CurrencyConversionRate',
                   TransactionLineAmount NUMBER PATH '$.TransactionLineAmount',
                   TransactionLineQuantity NUMBER PATH '$.TransactionLineQuantity',
                   LineTransactionsFlexfieldContext VARCHAR2(30) PATH '$.LineTransactionsFlexfieldContext',
                   LineTransactionsFlexfieldSegment1 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment1',
                   LineTransactionsFlexfieldSegment2 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment2',
                   LineTransactionsFlexfieldSegment3 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment3',
                   TaxClassificationCode VARCHAR2(30) PATH '$.TaxClassificationCode',
                   UnitOfMeasure VARCHAR2(3) PATH '$.UnitOfMeasure',
                   TaxExemptionFlag VARCHAR2(1) PATH '$.TaxExemptionFlag',
                   OverrideAutoAccountingFlag VARCHAR2(1) PATH '$.OverrideAutoAccountingFlag',
                   InvoiceTransactionsFlexfieldSegment1 VARCHAR2(150) PATH '$.InvoiceTransactionsFlexfieldSegment1',
                   InvoiceTransactionsFlexfieldSegment2 VARCHAR2(150) PATH '$.InvoiceTransactionsFlexfieldSegment2'
             )
           )
         ) jt
    where msg_id = p_msg_id
    and msg_status = 'PROCESSING';


    INSERT INTO ra_interface_distributions_all (
                    X_MSG_ID,
                    X_APEX_STATUS,
                    X_CREATION_DATE,
                    X_LAST_UPDATE_DATE,
                    BU_NAME,
                    ACCOUNT_CLASS,
                    AMOUNT,
                    PERCENT,
                    INTERFACE_LINE_CONTEXT,
                    INTERFACE_LINE_ATTRIBUTE1,
                    INTERFACE_LINE_ATTRIBUTE2,
                    INTERFACE_LINE_ATTRIBUTE3,
                    SEGMENT1,
                    SEGMENT2,
                    SEGMENT3,
                    SEGMENT4,
                    SEGMENT5,
                    SEGMENT6,
                    SEGMENT7,
                    SEGMENT8,
                    SEGMENT9,
                    SEGMENT10,
                    SEGMENT11,
                    SEGMENT12
    )
    select p_msg_id, 'NEW', sysdate, sysdate,
           jt.BusinessUnitName, jt.AccountClass, jt.Amount, jt.Percent, jt.LineTransactionsFlexfieldContext,
           jt.LineTransactionsFlexfieldSegment1, jt.LineTransactionsFlexfieldSegment2, LineTransactionsFlexfieldSegment3,
           jt.AccountingFlexfieldSegment1, jt.AccountingFlexfieldSegment2, jt.AccountingFlexfieldSegment3, jt.AccountingFlexfieldSegment4, 
           jt.AccountingFlexfieldSegment5, jt.AccountingFlexfieldSegment6, jt.AccountingFlexfieldSegment7, jt.AccountingFlexfieldSegment8, 
           jt.AccountingFlexfieldSegment9, jt.AccountingFlexfieldSegment10, jt.AccountingFlexfieldSegment11, jt.AccountingFlexfieldSegment12 
    from dbo_msg_inbound dmi, 
         JSON_TABLE(
           dmi.msg_payload,
           '$'
           COLUMNS (
             OperationName VARCHAR2(100) PATH '$.OperationName',
             SourceRef VARCHAR2(100) PATH '$.SourceRef',
             NESTED PATH '$.Item[*].receivablesInvoiceDistributions[*]'
               COLUMNS (
                   BusinessUnitName VARCHAR2(240) PATH '$.BusinessUnitName',
                   AccountClass VARCHAR2(50) PATH '$.AccountClass',
                   Amount NUMBER PATH '$.Amount',
                   Percent NUMBER PATH '$.Percent', 
                   LineTransactionsFlexfieldContext VARCHAR2(30) PATH '$.LineTransactionsFlexfieldContext',
                   LineTransactionsFlexfieldSegment1 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment1',
                   LineTransactionsFlexfieldSegment2 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment2',
                   LineTransactionsFlexfieldSegment3 VARCHAR2(30) PATH '$.LineTransactionsFlexfieldSegment3',
                   AccountingFlexfieldSegment1 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment1',
                   AccountingFlexfieldSegment2 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment2',
                   AccountingFlexfieldSegment3 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment3',
                   AccountingFlexfieldSegment4 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment4',
                   AccountingFlexfieldSegment5 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment5',
                   AccountingFlexfieldSegment6 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment6',
                   AccountingFlexfieldSegment7 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment7',
                   AccountingFlexfieldSegment8 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment8',
                   AccountingFlexfieldSegment9 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment9',
                   AccountingFlexfieldSegment10 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment10',
                   AccountingFlexfieldSegment11 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment11',
                   AccountingFlexfieldSegment12 VARCHAR2(25) PATH '$.AccountingFlexfieldSegment12'
             )
           )
         ) jt

    where msg_id = p_msg_id
    and msg_status = 'PROCESSING'
    ;


END msg_json_to_interface;

-- Prepare CSV files and Zip them before sending to ERP. Invoked form WF.
PROCEDURE zip_csv_files (
    p_msg_id         in dbo_msg_inbound.msg_id%type,
    p_wf_instance_id in ar_transactions_out.wf_instance_id%type
) IS
    l_lines_csv_output clob;
    l_dist_csv_output  clob;
    l_csv_raw          raw(32000);
    l_lines_csv_file   blob;
    l_dist_csv_file    blob;
    l_csv_file         blob;
    l_zip_file         blob;

    l_ra_lines_filename constant varchar2(30) := 'RaInterfaceLinesAll.csv';
    l_ra_dist_filename  constant varchar2(50) := 'RaInterfaceDistributionsAll.csv';

    e_empty_csv EXCEPTION;

begin
    
    --Prepare AR Lines file
    generate_dynamic_csv(
        p_msg_id     => p_msg_id,
        p_table_name => 'RA_INTERFACE_LINES_ALL',
        p_csv_output => l_lines_csv_output);
    
     --Prepare AR Distributions file
    generate_dynamic_csv(
        p_msg_id     => p_msg_id,
        p_table_name => 'RA_INTERFACE_DISTRIBUTIONS_ALL',
        p_csv_output => l_dist_csv_output);

    IF length(l_lines_csv_output) = 0 or length(l_dist_csv_output) = 0 THEN
        RAISE e_empty_csv;
    ELSE

        -- Convert Lines CSV to RAW data
        l_csv_raw := UTL_RAW.CAST_TO_RAW(l_lines_csv_output);

        --Initialize BLOB that will be zipped later
        DBMS_LOB.CREATETEMPORARY(l_lines_csv_file, TRUE);

        -- Write the CSV data to the BLOB
        DBMS_LOB.WRITE(l_lines_csv_file, UTL_RAW.LENGTH(l_csv_raw), 1, l_csv_raw);
        
        --add RA Lines csv file to zip
        apex_zip.add_file (
            p_zipped_blob => l_zip_file,
            p_file_name   => l_ra_lines_filename,
            p_content     => l_lines_csv_file);


         -- Convert Distributions CSV to RAW data
        l_csv_raw := UTL_RAW.CAST_TO_RAW(l_dist_csv_output);

        --Initialize BLOB that will be zipped later
        DBMS_LOB.CREATETEMPORARY(l_dist_csv_file, TRUE);

        -- Write the CSV data to the BLOB
        DBMS_LOB.WRITE(l_dist_csv_file, UTL_RAW.LENGTH(l_csv_raw), 1, l_csv_raw);
        
        --add RA Distributions csv file to zip
        apex_zip.add_file (
            p_zipped_blob => l_zip_file,
            p_file_name   => l_ra_dist_filename,
            p_content     => l_dist_csv_file);

        --Close zip 
        apex_zip.finish(p_zipped_blob => l_zip_file );

        --Store out file ready for sending to ERP
        save_out_file(p_msg_id, l_zip_file, p_wf_instance_id);

    END IF;

    -- Free temporary BLOB
    IF DBMS_LOB.ISTEMPORARY(l_lines_csv_file) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_lines_csv_file);
    END IF;

    -- Free temporary BLOB
    IF DBMS_LOB.ISTEMPORARY(l_dist_csv_file) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_dist_csv_file);
    END IF;

EXCEPTION
    WHEN e_empty_csv THEN
        RAISE_APPLICATION_ERROR(-20001, 'zip_csv_files: Empty CSV');
    WHEN OTHERS THEN
        -- Free temporary BLOB
        IF DBMS_LOB.ISTEMPORARY(l_lines_csv_file) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_lines_csv_file);
        END IF;
        IF DBMS_LOB.ISTEMPORARY(l_dist_csv_file) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_dist_csv_file);
        END IF;

        RAISE;
END zip_csv_files;

--Take base64 from AR transactions out table and upload it to ERP. Invoked from WF.
PROCEDURE upload_zip_file_to_erp (
    p_msg_id        in  dbo_msg_inbound.msg_id%type,
    p_document_id   out ar_transactions_out.erp_document_id%type,
    p_status_code   out number,
    p_error_message out varchar2
)IS
    e_rest_error     exception;
    e_doc_id_missing exception;

    cursor c_out_msg is 
        select id, base64
        from ar_transactions_out 
        where msg_id = p_msg_id;

BEGIN
    --init
    g_proc := 'upload_zip_file_to_erp';
   
    for r_out_msg in c_out_msg 
    loop
        rest_upload_file_to_ucm(
            p_msg_id,
            r_out_msg.base64,
            'fin/receivables/import',
            'AR',
            '/fscmRestApi/resources/11.13.18.05/erpintegrations',
            'AR_'||to_char(sysdate,'YYYYMMDDHH24MISS')||'_'||p_msg_id||'.zip',
            'zip',
            p_status_code,
            p_document_id,
            p_error_message);

        if nvl(p_status_code,-1) != 201 then
            raise e_rest_error;
        elsif p_document_id is null then
            raise e_doc_id_missing;
        else
            update ar_transactions_out
            set erp_document_id = p_document_id,
                last_update_date = sysdate
            where msg_id = p_msg_id
            and id = r_out_msg.id;

            update_status(p_msg_id, 'LOADED_TO_UCM', r_out_msg.id);
        end if;
     end loop;
EXCEPTION
    WHEN e_rest_error THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, 'REST status code:'||p_status_code||':'||p_error_message, 'ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'REST status code:'||p_status_code||':'||p_error_message);
    WHEN e_doc_id_missing THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, 'Missing Document Id.','ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Missing Document Id.');
    WHEN others THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, SQLERRM);
        RAISE;
END upload_zip_file_to_erp;


--Start ERP load process to import data from zip file to interface table. Invoked from WF.
PROCEDURE submit_erp_interface_loader(
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_document_id in  ar_transactions_out.erp_document_id%type,
    p_request_id  out ar_transactions_out.erp_import_req_id%type
) IS
  l_error_message VARCHAR2(255);
  l_status_code NUMBER(10);

  e_doc_id_missing exception;
  e_loader_ess     exception;
  e_erp_offline    exception;
BEGIN
    --init
    g_proc := 'submit_erp_interface_loader';
  
    IF dbo_erp_utils_pkg.erp_is_online THEN
    
        IF p_document_id IS NULL THEN
            raise e_doc_id_missing;
        ELSE

            --start ERP interfaceLoader ESS job 
            --dbo_erp_utils_pkg.rest_submit_ess_job_request( --created copy of procedure to include callBackURL parameter
            rest_submit_ess_job_request(
                p_msg_id,
                'AR',
                '/fscmRestApi/resources/11.13.18.05/erpintegrations',
                'oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader/',
                'InterfaceLoaderController',
                '2,'||p_document_id||',N,N',
                g_callback_url,
                l_status_code,
                p_request_id,
                l_error_message);
            
            IF l_status_code=201 and p_request_id IS NOT NULL THEN
                
                update ar_transactions_out
                set erp_import_req_id = p_request_id,
                    last_update_date = sysdate
                where msg_id = p_msg_id
                and erp_document_id = p_document_id;

                update_status(p_msg_id,'WAITING');
            ELSE
                raise e_loader_ess;
            END IF;
        END IF;
    ELSE
        raise e_erp_offline;
    END IF;
EXCEPTION
    WHEN e_doc_id_missing THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id,'Document ID is missing.','ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Document ID is missing.');
    WHEN e_loader_ess THEN           
        update_status(p_msg_id,'ERROR');
        log(p_msg_id,l_status_code||':'||l_error_message,'ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Error running ERP Loader ESS (request_id='||p_request_id||'). Error: '||l_status_code||':'||l_error_message);
    WHEN e_erp_offline THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, 'ERP is in Offline state. When Online resume workflow from AR Application.');
        RAISE_APPLICATION_ERROR(-20001, 'ERP is in Offline state. When Online resume workflow from AR Application.');
    WHEN others THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, SQLERRM);
        RAISE;
END submit_erp_interface_loader;


--Receive and store ERP ESS Job CallBack message
PROCEDURE store_erp_callback(
    p_data in ar_callback_msg.message_data%type
) IS 
    l_json                  clob;
    l_request_id            number;
    l_status                varchar2(10);
    l_error                 varchar2(30) := 'ERROR';
    l_activity_params       wwv_flow_global.vc_map;
    l_msg_id                dbo_msg_inbound.msg_id%type;
    l_wf_instance_id        number;
    l_security_group_id     number;
    l_callback_id           number;
    l_job_path              VARCHAR2(200);
    l_wf_activity_static_id VARCHAR2(50);

    e_wf_not_found          exception;
    e_unknown_ess           exception;

BEGIN
    --int
    g_proc := 'store_erp_callback';

    g_step := 'Store callback message';
    insert into ar_callback_msg (message_data, status, creation_date, last_update_date)
    values (p_data, 'RECEIVED', sysdate, sysdate) returning id into l_callback_id;

    COMMIT;

    BEGIN
        g_step := 'Parse calback XML';
        WITH xml_data AS (
            SELECT XMLTYPE(p_data) AS xml_content
            FROM dual
            )
        SELECT result_message
        INTO l_json
        FROM xml_data,
         XMLTABLE(
             XMLNAMESPACES(
                 'http://schemas.xmlsoap.org/soap/envelope/' AS "env",
                 'http://xmlns.oracle.com/scheduler' AS "ns0"
             ),
             '/env:Envelope/env:Body/ns0:onJobCompletion'
             PASSING xml_content
             COLUMNS result_message CLOB PATH 'resultMessage'
         );
    EXCEPTION
        WHEN others THEN
            l_error := 'ERR_XML';
            RAISE;
    END;

    IF l_json is not null THEN
        BEGIN
            g_step := 'Parse callback json inside xml';

            select jt.RequestId, jt.Status, jt.JobPath
            into l_request_id, l_status, l_job_path
            from dual, 
             JSON_TABLE(
               l_json,
               '$'
               COLUMNS (
                 NESTED PATH '$.JOBS[*]'
                   COLUMNS (
                       DocumentName VARCHAR2(240) PATH '$.DOCUMENTNAME',
                       JobName VARCHAR2(240) PATH '$.JOBNAME',
                       JobPath VARCHAR2(500) PATH '$.JOBPATH',
                       RequestId NUMBER PATH '$.REQUESTID',
                       Status VARCHAR2(10) PATH '$.STATUS'
                 )
               )
             ) jt;

        EXCEPTION
            WHEN others THEN
                l_error := 'ERR_JSON';
                RAISE;
        END;

        update ar_callback_msg
        set json_content = l_json, last_update_date=sysdate, erp_request_id=l_request_id, erp_request_status=l_status
        where id = l_callback_id;

        commit;

    END IF;


    g_step := 'Update AR Transaction in Out table';
    BEGIN
        
        if l_job_path = '/oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader' then 
            select msg_id, wf_instance_id
            into l_msg_id, l_wf_instance_id
            from ar_transactions_out
            where erp_import_req_id = l_request_id;

            l_wf_activity_static_id := 'AR_WAIT_ERP_LOADER_CALLBACK';

        elsif l_job_path = '/oracle/apps/ess/financials/receivables/transactions/autoInvoices' then 
            select msg_id, wf_instance_id
            into l_msg_id, l_wf_instance_id
            from ar_transactions_out
            where erp_autoinvoice_req_id = l_request_id;

            l_wf_activity_static_id := 'AR_WAIT_ERP_AUTOINVOICE_CALLBACK';

        else
            RAISE e_unknown_ess;
        end if;

        update ar_callback_msg
        set wf_instance_id = l_wf_instance_id, original_msg_id = l_msg_id
        where id = l_callback_id; 
        
        commit;

        --TODO: handle error status and if no data found
        if l_wf_instance_id is null then
            RAISE e_wf_not_found;
        end if;
    EXCEPTION
        WHEN e_unknown_ess THEN
            l_error := 'ERR_UNKNOWN_ESS';
            RAISE;
        WHEN e_wf_not_found THEN
            l_error := 'ERR_WF_ID_NULL';
            RAISE;
        WHEN others THEN
            l_error := 'ERR_FIND_WF';
            RAISE;
    END;

    g_step := 'Continue workflow wait activity';
    BEGIN

        --TODO: where to store init session parameters
        apex_session.create_session (
            p_app_id   => 407,
            p_page_id  => 1,
            p_username => 'rest_test_al' );

         l_security_group_id := apex_util.find_security_group_id (p_workspace => 'XX_INTEGRATION_DEV');
         apex_util.set_security_group_id (p_security_group_id => l_security_group_id);
         
         --l_activity_result('TASK_OUTCOME')   :=  'APPROVED';
         apex_workflow.continue_activity(
              p_instance_id          => l_wf_instance_id,
              p_static_id            => l_wf_activity_static_id,
              p_activity_params      => l_activity_params
              );

    EXCEPTION
        WHEN others THEN
           l_error := 'ERR_WAIT';
           RAISE;
    END;

EXCEPTION
    when others then
        IF l_msg_id is not null THEN
            log(l_msg_id, SQLERRM, 'ERROR');
        END IF;
        update ar_callback_msg
        set status = l_error, last_update_date=sysdate
        where id = l_callback_id;
        commit;

END store_erp_callback;

--Start ERP Import Autoinvoice to load data from ERP interface tables to ERP base tables. Invoked from WF.
PROCEDURE submit_erp_autoinvoice(
    p_msg_id          in  dbo_msg_inbound.msg_id%type,
    p_load_request_id in  ar_transactions_out.erp_import_req_id%type,
    p_autoinv_req_id  out ar_transactions_out.erp_autoinvoice_req_id%type
) IS
  l_error_message VARCHAR2(255);
  l_status_code NUMBER(10);
  l_ess_parameter_string VARCHAR2(300);

  e_load_req_id_missing exception;
  e_autoinvoice_ess     exception;
  e_erp_offline         exception;
BEGIN
    --init
    g_proc := 'submit_erp_autoinvoice';
  
    IF dbo_erp_utils_pkg.erp_is_online THEN

        if p_load_request_id is not null then

            g_step := 'concat parameter string';
            l_ess_parameter_string := '1,300000001990101,300000002223716,'||to_char(sysdate,'YYYY-MM-DD')||
                                        ',#NULL,#NULL,#NULL,#NULL,#NULL,#NULL,#NULL,#NULL,#NULL,#NULL'||
                                        ',#NULL,#NULL,#NULL,#NULL,#NULL,#NULL'||
                                        ',#NULL,#NULL,Y,#NULL,#NULL,'||p_load_request_id;

            g_step := 'submit Autoinvoice';
            --start ERP interfaceLoader ESS job 
            --dbo_erp_utils_pkg.rest_submit_ess_job_request( --created copy of procedure to include callBackURL parameter
            rest_submit_ess_job_request(
                p_msg_id,
                'AR',
                '/fscmRestApi/resources/11.13.18.05/erpintegrations',
                '/oracle/apps/ess/financials/receivables/transactions/autoInvoices',
                'AutoInvoiceMasterEss',
                l_ess_parameter_string,
                g_callback_url,
                l_status_code,
                p_autoinv_req_id,
                l_error_message);
    
            IF l_status_code=201 and p_autoinv_req_id IS NOT NULL THEN

                g_step := 'update status';                
                update ar_transactions_out
                set erp_autoinvoice_req_id = p_autoinv_req_id,
                    last_update_date = sysdate
                where msg_id = p_msg_id;

                update_status(p_msg_id,'WAITING');
            ELSE
                raise e_autoinvoice_ess;
            END IF;
        ELSE
            raise e_load_req_id_missing;
        END IF;

    ELSE
        raise e_erp_offline;
    END IF;
EXCEPTION
    WHEN e_load_req_id_missing THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id,'Loader Request ID is missing.','ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Document ID is missing.');
    WHEN e_autoinvoice_ess THEN           
        update_status(p_msg_id,'ERROR');
        log(p_msg_id,l_status_code||':'||l_error_message,'ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Error running ERP Autoinvoice ESS (request_id='||p_autoinv_req_id||'). Error: '||l_status_code||':'||l_error_message);
    WHEN e_erp_offline THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, 'ERP is in Offline state. When Online resume workflow from AR Application.');
        RAISE_APPLICATION_ERROR(-20001, 'ERP is in Offline state. When Online resume workflow from AR Application.');
    WHEN others THEN
        update_status(p_msg_id,'ERROR');
        log(p_msg_id, SQLERRM);
        RAISE;
END submit_erp_autoinvoice;

--update all receivables integration related tables with final status. Invoked from WF.
PROCEDURE set_final_status (
    p_msg_id in dbo_msg_inbound.msg_id%type
) IS

BEGIN
    update_status(p_msg_id,'PROCESSED',null);
END set_final_status;


END AR_TRANSACTIONS_PKG;
/