create or replace PACKAGE BODY dbo_erp_utils_pkg AS


--         _  _                                                     _    _  _                     _           
--      __| || |__    ___           ___  _ __  _ __          _   _ | |_ (_)| | ___         _ __  | | __  __ _ 
--     / _` || `_ \  / _ \         / _ \| `__|| `_ \        | | | || __|| || |/ __|       | `_ \ | |/ / / _` |
--    | (_| || |_) || (_) |       |  __/| |   | |_) |       | |_| || |_ | || |\__ \       | |_) ||   < | (_| |
--     \__,_||_.__/  \___/  _____  \___||_|   | .__/  _____  \__,_| \__||_||_||___/ _____ | .__/ |_|\_\ \__, |
--                         |_____|            |_|    |_____|                       |_____||_|           |___/ 
--   

--Used in run_report   
   c_role_run_report    CONSTANT   varchar2(100) := 'RUN_BI_REPORT'; 
   c_role_catalog       CONSTANT   varchar2(100) := 'RUN_BI_REPORT'; 
   e_rest_failed        EXCEPTION;
   e_fault_exists       EXCEPTION;
   e_setup_error        EXCEPTION;
   e_token_error        EXCEPTION;

 --Used for jwt signature in callback parameter and decoding
   l_iss        CONSTANT varchar2(100)      := 'EBS Consulting AS';  ---Issuer
   l_aud        CONSTANT varchar2(100)      := 'KDA Apex Integration'; --Audience
   l_exp_sec    CONSTANT PLS_INTEGER        := 28800; --Varighet 8 timer på jwt token
   l_signature_key CONSTANT varchar2(100)   := 'i2319dkao93se2adlkjADSfk9Jdj33whgjsalki232ksal';


/* -----------------------------------------------------------------------------
*   procedure     : rest_upload_file_to_ucm
*   description   : Upload the given file to UCM using erpintegrations. The 
*                   procedure will base64 encode the data before upload
*
*   scope         : public
*   arguments
*        in       : p_msg_id - The message id that identifies this upload
*                   p_file_contents - The clob containing the file contents to be 
*                                     uploaded
*                   p_document_account - The document account to upload the file to
*                   p_file_name - The name of the file to be uploaded
*                   p_content_type - The file content, e.g. csv
*        in/out   :
*        out      : p_status_code - The status code returned from the REST service
*                   p_document_id - The document id for the uploaded file
*                   p_error_message - The error message if any error occurred
*        return   : 
*
* Change log
*  Date       Author    Description
*  ---------  --------- -------------------------------------------------------
*  2024.05.22 PETSTR    Moved from cur_daily_rate_pkg and made generic
*  2024.05.24 HAVSTA    Made it more generic and added parameters to support debug
*  2024.08.08 HAVSTA    Made a new version of rest_submitt_ess_job_request named rest_submitt_ess_job_request_callback that takes relative call back parameter
*  2024.09.03 ALDLAG    Procedure rest_upload_file_to_ucm new parameter p_encode_to_base64 to control if file content needs to be encoded to BASE64 (TRUE) or the calling program has encoded it already (FALSE)
*  2024.09.27 ALDLAG    Created new version (run_report_v2) of run_report. Substituted single report paramaters with parameter table type.
*-----------------------------------------------------------------------------*/
PROCEDURE rest_upload_file_to_ucm(p_msg_id           IN  RAW
                                , p_file_contents    IN  CLOB
                                , p_document_account IN  VARCHAR2
								, p_module			 IN  VARCHAR2
								, p_rest_endpoint	 IN  VARCHAR2
                                , p_file_name        IN  VARCHAR2
                                , p_content_type     IN  VARCHAR2
                                , p_encode_to_base64 IN  BOOLEAN DEFAULT TRUE
                                , p_status_code      OUT NUMBER
                                , p_document_id      OUT NUMBER
                                , p_error_message    OUT VARCHAR2) 
IS
  --security and server
  l_web_credential varchar2(400);
  l_servername VARCHAR2(100);
  l_file_as_base64 clob;
  --web_service response
  l_response CLOB;
BEGIN
  --enter procedure
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Enter rest_upload_file_to_ucm. Account:'||p_document_account||' Endpoint:'||p_rest_endpoint||' File Name:'||p_file_name);   
  --init
  p_error_message:='';
  p_document_id:=null;
  p_status_code:=null;

  -- Get the ERP credentials
  BEGIN
      CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, l_servername);
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials. ' ||l_servername);   
  EXCEPTION
  WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_upload_file_to_ucm. Error credentials. '||p_error_message);   
  END;
      
  DBMS_LOB.CREATETEMPORARY(l_file_as_base64, TRUE);

  IF p_encode_to_base64 THEN
  
      -- base64 encode av data
      SELECT UTL_ENCODE.TEXT_ENCODE(p_file_contents, 'AL32UTF8', UTL_ENCODE.BASE64)
      INTO l_file_as_base64
      FROM DUAL;

      dbo_msg_pkg.debug_msg(p_msg_id,p_module, 'Finished base64 output');   

      l_file_as_base64 := REPLACE(l_file_as_base64, CHR(10), '');
      l_file_as_base64 := REPLACE(l_file_as_base64, CHR(13), '');

      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Finished lineremoval in base64');
  ELSE
      l_file_as_base64 := p_file_contents;
  END IF;

  -- Make the REST request
  APEX_WEB_SERVICE.g_request_headers.delete();
  APEX_WEB_SERVICE.g_request_headers(1).name:='Content-Type';
  APEX_WEB_SERVICE.g_request_headers(1).value:='application/json';
  BEGIN
    l_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => l_servername || p_rest_endpoint,  -- '/fscmRestApi/resources/11.13.18.05/erpintegrations',
      p_http_method => 'POST',
      p_credential_static_id => l_web_credential,
      p_body => '{
                    "OperationName":"uploadFileToUCM",
                    "DocumentContent":"' || l_file_as_base64 || '",
                    "DocumentAccount":"' || p_document_account || '",
                    "ContentType":"' || p_content_type || '",
                    "FileName":"' || p_file_name || '",
                    "DocumentId":null
                  }'
    );
    
    p_status_code:=APEX_WEB_SERVICE.g_status_code;  
	
    --debug webkall
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Finished web call, status: '||p_status_code||chr(10)||chr(13)||'respons: '||l_response);   
    
	  IF p_status_code=201 THEN
       APEX_JSON.parse(l_response);
       p_document_id := APEX_JSON.get_varchar2(p_path => 'DocumentId');
       dbo_msg_pkg.debug_msg(p_msg_Id,p_module,'Finished documentid parsing:' ||p_document_id);  
    ELSE
      p_error_message := APEX_WEB_SERVICE.g_reason_phrase;
    END IF;
	
    IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
       DBMS_LOB.FREETEMPORARY(l_file_as_base64);
    END IF;

  EXCEPTION
  WHEN OTHERS THEN
    p_error_message := SQLERRM;
    p_status_code:=APEX_WEB_SERVICE.g_status_code;
    --debug feil på web call
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Exception status:'||p_status_code||' error:'||p_error_message);   
    dbo_msg_pkg.log_error(p_msg_id,'Inside exception block rest_upload_file_to_ucm. Status:'|| p_status_code||' error:'||p_error_message);
      
    IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
       DBMS_LOB.FREETEMPORARY(l_file_as_base64);
    END IF;
  END;
END rest_upload_file_to_ucm;


--generic submit ess job request
PROCEDURE rest_submit_ess_job_request(p_msg_id           IN RAW
                                    , p_module			 IN  VARCHAR2
                                    , p_rest_endpoint	 IN  VARCHAR2  --'/fscmRestApi/resources/11.13.18.05/erpintegrations'
                                    , p_job_package_name IN  VARCHAR2 -- oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader/
                                    , p_job_def_name     IN  VARCHAR2 -- InterfaceLoaderController
                                    , p_parameter_str    IN  VARCHAR2 -- 71'||','||p_documentid||',N,N,null'||'
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2)  
IS
  l_web_credential varchar2(400);
  v_servername VARCHAR2(100);
  v_response CLOB;
BEGIN
  -- Get the ERP credentials
  BEGIN
    CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, v_servername);
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials.' ||V_servername);   
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
    v_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => v_servername || p_rest_endpoint, 
      p_http_method => 'POST',
      p_credential_static_id => l_web_credential,
      p_body => '{
          "OperationName":"submitESSJobRequest",
          "JobPackageName": "'|| p_job_package_name ||'",
          "JobDefName": "'|| p_job_def_name ||'",
          "ESSParameters": "'|| p_parameter_str ||'"
      }');
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
    APEX_JSON.parse(v_response);
    p_requestid := APEX_JSON.get_varchar2(p_path => 'ReqstId');
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Requestid parsed:'|| p_requestid);          
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Unexpected error parsing reqstid: ' || SQLERRM;
      dbo_msg_pkg.debug_msg(p_msg_id, p_module, p_error_message);
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
  END;

END rest_submit_ess_job_request;

--generic submit essjob request with callback option
PROCEDURE rest_submit_ess_job_request_callback(p_msg_id           IN  RAW
                                    , p_module			 IN  VARCHAR2
                                    , p_rest_endpoint	 IN  VARCHAR2
                                    , p_job_package_name IN  VARCHAR2
                                    , p_job_def_name     IN  VARCHAR2
                                    , p_parameter_str    IN  VARCHAR2
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2
                                    , p_callback         IN  VARCHAR2)
IS
  l_web_credential VARCHAR2(400);
  v_servername VARCHAR2(100);
  v_response CLOB;
  v_callback_url varchar2(2000);
  l_jwt_value varchar2(32767);
BEGIN
  -- Get the ERP credentials
  BEGIN
    CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, v_servername);
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials.' ||V_servername);   
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_submit_ess_job_request. Error credentials. '||p_error_message);   
  END;
  
  --json token
  l_jwt_value := apex_jwt.encode (
                       p_iss => l_iss,
                       p_aud => l_aud,
                       p_exp_sec => l_exp_sec, --gyldig i 8 timer
                       p_other_claims => '"MSG_ID": '||apex_json.stringify(p_msg_id)||
                                         ',"MODULE": '||apex_json.stringify(p_module),
                       p_signature_key => sys.UTL_RAW.cast_to_raw(l_signature_key));
  
  begin
    CON_SECURITY_PKG.GET_CALLBACK_SETTINGS(v_callback_url);
    dbo_msg_pkg.debug_msg(p_msg_id, p_module,'Get Basic Call Back URL: '||v_callback_url);
    if v_callback_url='' then
      dbo_msg_pkg.log_error(p_msg_id,'Callback url is empty and must be configured in app ERP Cloud Connect.','ERROR');
      RAISE_APPLICATION_ERROR(-20001, 'Callback url is empty and must be configured in app ERP Cloud Connect.');
    end if;
    v_callback_url:=v_callback_url||p_callback||'?X01='||l_jwt_value;
  exception
    when others then
      p_error_message := 'Error getting Callback url: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_submit_ess_job_request_callback. Error callback url. '||p_error_message);   
  end;

  APEX_WEB_SERVICE.g_request_headers.delete();
  APEX_WEB_SERVICE.g_request_headers(1).name:='Content-Type';
  APEX_WEB_SERVICE.g_request_headers(1).value:='application/json';

  BEGIN
    v_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => v_servername || p_rest_endpoint, 
      p_http_method => 'POST',
      p_credential_static_id => l_web_credential,
      p_body => '{
          "OperationName":"submitESSJobRequest",
          "JobPackageName": "'|| p_job_package_name ||'",
          "JobDefName": "'|| p_job_def_name ||'",
          "CallbackURL": "'||v_callback_url||'",
          "ESSParameters": "'|| p_parameter_str ||'"
      }');
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
    APEX_JSON.parse(v_response);
    p_requestid := APEX_JSON.get_varchar2(p_path => 'ReqstId');
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Requestid parsed:'|| p_requestid);          
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Unexpected error parsing reqstid: ' || SQLERRM;
      dbo_msg_pkg.debug_msg(p_msg_id, p_module, p_error_message);
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
  END;

END rest_submit_ess_job_request_callback;

PROCEDURE rest_submit_ess_job_request_callback_ct(p_msg_id           IN  RAW
                                    , p_module			 IN  VARCHAR2       --CUR, ENS, AR, ....
                                    , p_rest_endpoint	 IN  VARCHAR2       
                                    , p_job_package_name IN  VARCHAR2
                                    , p_job_def_name     IN  VARCHAR2
                                    , p_parameter_str    IN  VARCHAR2
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2
                                    , p_callback         IN  VARCHAR2       --/erpCallBack/callBack 
                                    , p_ct               IN  VARCHAR2) IS    --CUSTOM TOKEN
l_web_credential VARCHAR2(400);
  v_servername VARCHAR2(100);
  v_response CLOB;
  v_callback_url varchar2(2000);
  l_jwt_value varchar2(32767);
BEGIN
  -- Get the ERP credentials
  BEGIN
    CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, v_servername);
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials.' ||V_servername);   
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_submit_ess_job_request. Error credentials. '||p_error_message);   
  END;
  
  --json token
  l_jwt_value := apex_jwt.encode (
                       p_iss => l_iss,
                       p_aud => l_aud,
                       p_exp_sec => l_exp_sec, --gyldig i 8 timer
                       p_other_claims => '"MSG_ID": '||apex_json.stringify(p_msg_id)||
                                         ',"MODULE": '||apex_json.stringify(p_module)||
                                         ',"CT": '  ||apex_json.stringify(p_ct),
                       p_signature_key => sys.UTL_RAW.cast_to_raw(l_signature_key));
  
  begin
    CON_SECURITY_PKG.GET_CALLBACK_SETTINGS(v_callback_url);
    dbo_msg_pkg.debug_msg(p_msg_id, p_module,'Get Basic Call Back URL: '||v_callback_url);
    if v_callback_url='' then
      dbo_msg_pkg.log_error(p_msg_id,'Callback url is empty and must be configured in app ERP Cloud Connect.','ERROR');
      RAISE_APPLICATION_ERROR(-20001, 'Callback url is empty and must be configured in app ERP Cloud Connect.');
    end if;
    v_callback_url:=v_callback_url||p_callback||'?X01='||l_jwt_value;
  exception
    when others then
      p_error_message := 'Error getting Callback url: ' || SQLERRM;
      p_status_code := -1; --error
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block rest_submit_ess_job_request_callback. Error callback url. '||p_error_message);   
  end;

  APEX_WEB_SERVICE.g_request_headers.delete();
  APEX_WEB_SERVICE.g_request_headers(1).name:='Content-Type';
  APEX_WEB_SERVICE.g_request_headers(1).value:='application/json';

  BEGIN
    v_response := APEX_WEB_SERVICE.make_rest_request(
      p_url => v_servername || p_rest_endpoint, 
      p_http_method => 'POST',
      p_credential_static_id => l_web_credential,
      p_body => '{
          "OperationName":"submitESSJobRequest",
          "JobPackageName": "'|| p_job_package_name ||'",
          "JobDefName": "'|| p_job_def_name ||'",
          "CallbackURL": "'||v_callback_url||'",
          "ESSParameters": "'|| p_parameter_str ||'"
      }');
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
    APEX_JSON.parse(v_response);
    p_requestid := APEX_JSON.get_varchar2(p_path => 'ReqstId');
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Requestid parsed:'|| p_requestid);          
  EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Unexpected error parsing reqstid: ' || SQLERRM;
      dbo_msg_pkg.debug_msg(p_msg_id, p_module, p_error_message);
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
  END;

END rest_submit_ess_job_request_callback_CT;



--               _____________________________________________________________________________________        
--      ________|                                                                                     |_______
--      \       |    getESSExecutionDetails takes requestid from Oracle ERP.                          |      /
--       \      |    Returns clob with json respons from Oracle ERP. p_status=200 if ok, -1 else.     |     / 
--       /      |_____________________________________________________________________________________|     \ 
--      /__________)                                                                               (_________\
--
--  Created 12.05.2024 Takes p_requestid and returns p_response as clob from Oracle ERP

procedure getESSExecutionDetails(p_msg_id IN RAW, p_module IN VARCHAR2, p_requestid IN number, p_response OUT clob, p_status out number, p_error_message out varchar2) IS
    --erp username/password/url
	l_web_credential VARCHAR2(400);
    l_servername VARCHAR2(255);
BEGIN
	--INIT
	p_response:='';
	p_error_message:='';
	p_status:=-1;
    dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Enter getESSExecutionDetails for '||p_module||' requestid: '||p_requestid);
    -- Get the ERP credentials
    BEGIN
      CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, l_servername);
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Get Credentials.' ||l_servername);   
    EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Error getting ERP credentials: ' || SQLERRM;
      dbo_msg_pkg.log_error(p_msg_id,p_error_message,'ERROR');
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'Inside exception block getESSExecutionDetails. Error credentials. '||p_error_message);   
    END;
    
    -- Make the REST request
    BEGIN
      p_response := APEX_WEB_SERVICE.make_rest_request(
        p_url => l_servername || '/fscmRestApi/resources/11.13.18.05/erpintegrations?finder=ESSExecutionDetailsRF;requestId=' || p_requestid,
        p_http_method => 'GET',
        p_credential_static_id => l_web_credential
      );
	  p_status:=APEX_WEB_SERVICE.g_status_code; 
      dbo_msg_pkg.debug_msg(p_msg_id,p_module,'ESSexecutionDetailsRF success with status:'||p_status||'respons:'||p_response);
    EXCEPTION
    WHEN OTHERS THEN
      p_error_message := 'Error making REST request: ' || SQLERRM;
      p_status:=APEX_WEB_SERVICE.g_status_code; 
      dbo_msg_pkg.debug_msg(p_msg_Id,p_module, p_error_Message||'status:'||p_status);
      dbo_msg_pkg.log_error(p_msg_id, p_error_message||' status: '||p_status,'ERROR');
    END;
   
    
EXCEPTION
  WHEN OTHERS THEN
    p_error_message := 'Unexpected error: ' || SQLERRM;
    dbo_msg_pkg.log_error(p_msg_id, p_error_message||' status: '||p_status,'ERROR');
END getESSExecutionDetails;


--               _____________________________________________________________________________        
--      ________|                                                                             |_______
--      \       |    erp_is_online                                                            |      /
--       \      |    Returns status true/false if ERP is online and processing can go on.     |     / 
--       /      |_____________________________________________________________________________|     \ 
--      /__________)                                                                       (_________\

  FUNCTION erp_is_online return boolean
  IS
    v_on number(1);
  BEGIN
    select erp_is_on into v_on from dbo_erp_settings;
      if v_on=1 then
        return true;
      else
        return false;
      end if;
    exception when NO_DATA_FOUND then
      return false; --settings missing. erp is assumed not on.
  END erp_is_online;



--               _____________________________________________________________________________        
--      ________|                                                                             |_______
--      \       |    fetchErrorDocumentAndStore                                               |      /
--       \      |    Takes msg_id and requestid and download error for given requesstid .     |     / 
--       /      |_____________________________________________________________________________|     \ 
--      /__________)                                                                       (_________\

  --Download errormessage from ERP for given requestid and store it connected to msg_id
procedure fetchErrorDocumentAndStore(p_msg_id IN RAW, p_requestid IN number) is
    l_web_credential VARCHAR2(400);
    v_servername VARCHAR2(100);
    v_document BLOB;
	v_document_content CLOB;
    v_response CLOB;
    
BEGIN
   
    -- Get the ERP credentials
    BEGIN
      CON_SECURITY_PKG.GET_WEB_ERP_CREDENTIALS (l_web_credential, v_servername);
    EXCEPTION
    WHEN OTHERS THEN
      dbo_msg_pkg.log_error(p_msg_id,  'Error getting ERP credentials: ' || SQLERRM);
    END;

    -- Make the REST request to fetch the document
    BEGIN      
      v_response := APEX_WEB_SERVICE.make_rest_request(
        p_url => v_servername || '/fscmRestApi/resources/11.13.18.05/erpintegrations/?finder=ESSJobExecutionDetailsRF;requestId='||p_requestid||',fileType=ALL',
        p_http_method => 'GET',
        p_credential_static_id => l_web_credential
      );

    EXCEPTION
    WHEN OTHERS THEN
    dbo_msg_pkg.log_error(p_msg_id,  'Error making REST request: ' || SQLERRM);
    END;

      APEX_JSON.parse(v_response);

    -- Extract the DocumentContent value
    V_document_content := APEX_JSON.get_varchar2(p_path => 'items[1].DocumentContent');

    -- Convert the CLOB to a BLOB
    v_document := dbo_utils_pkg.decode_base64(v_document_content); 


    -- Store the fetched document in the table
    BEGIN
      insert into DBO_MSG_ERP_ERROR (
        MSG_ID, REQUESTID,  ERP_ERROR
      )
      values (p_msg_id,p_requestid, v_document);
     

    EXCEPTION
    WHEN OTHERS THEN
      dbo_msg_pkg.log_error(p_msg_id,  'Error updating DBO_MSG_ERP_ERROR with error-message: ' || SQLERRM);
    END;

EXCEPTION
  WHEN OTHERS THEN
    dbo_msg_pkg.log_error(p_msg_id,  'Unexpected error: ' || SQLERRM);
  
end fetchErrorDocumentAndStore;


--               _____________________________________________________________________________        
--      ________|                                                                             |_______
--      \       |    download_file                                                            |      /
--       \      |    Takes msg_id and requestid and download file to local computer           |     / 
--       /      |_____________________________________________________________________________|     \ 
--      /__________)                                                                       (_________\

procedure download_file(p_msg_id in raw, p_requestid in number) is 
	v_file_name     varchar2(500);
	v_mime_type     varchar2(500);
	v_blob          blob;   
begin
  SELECT ERP_ERROR, p_msg_id||'_'||p_requestid||'.zip' INTO v_BLOB, v_file_name FROM DBO_MSG_ERP_ERROR  where msg_id=p_msg_id and requestid=p_requestid;  
  v_mime_type := 'application/zip';
	
	owa_util.mime_header(v_mime_type, false);
	htp.p('Content-Length: ' || dbms_lob.getlength(v_blob));
	htp.p('Content-Disposition: attachment; filename="' || v_file_name || '"');
	owa_util.http_header_close;
	wpg_docload.download_file(v_blob);
	apex_application.stop_apex_engine;

end download_file;

--               _____________________________________________________        
--      ________|                                                     |_______
--      \       |    store_erp_callback. Takes xml data from ERP.     |      /
--       \      |    Returns 201 Created to ERP                       |     / 
--       /      |_____________________________________________________|     \ 
--      /__________)                                               (_________\

procedure store_erp_callback(p_data in CLOB, p_token_return in varchar2, p_status_code OUT NUMBER) is
  v_callback_id raw(16); 
  v_callback_json clob;

  l_token apex_jwt.t_token;
  l_msg_id raw(16);
  l_module varchar2(100);  
  l_ct varchar2(100);
BEGIN
   begin  
     l_token := apex_jwt.decode (p_value => p_token_return );

     apex_jwt.validate (p_token => l_token, p_aud => l_aud, p_iss => l_iss);

     apex_json.parse(l_token.payload);
     --Extract values of MSG_ID and MODULE from the payload
     l_msg_id := apex_json.get_varchar2('MSG_ID');
     l_module := apex_json.get_varchar2('MODULE');
     l_ct     := apex_json.get_varchar2('CT');

  EXCEPTION
    WHEN VALUE_ERROR THEN
      p_status_code:=401;
      return;
    WHEN OTHERS THEN
      p_status_code:=401;
      return;
  END;

  --token is valid and msg_id and module are known, continue.

  --init callback id
  v_callback_id:=sys_guid();

  BEGIN
    WITH xml_data AS (
      SELECT XMLTYPE(p_data) AS xml_content FROM dual)
        SELECT result_message
        INTO v_callback_json
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
      RAISE;
  END;

  --save raw callback
  insert into DBO_CALLBACK_MSG (callback_id,message_data, json_content, msg_id, module, custom_token)
  values (v_callback_id,p_data, v_callback_json,l_msg_id, l_module, l_ct);

  COMMIT;
   
  p_status_code:=201;  

  if l_module = 'CUR' THEN
    cur_daily_rate_pkg.process_callback_request(l_msg_id,v_callback_json);
  elsif l_module = 'ENS' and l_ct = 'VAL' then
    ens_msg_workflow_pkg.process_callback_request(l_msg_id,v_callback_json);
  elsif l_module = 'ENS' and l_ct = 'HIE' then --Segment Hierarchies import 
    ens_hierarchies_pkg.process_erp_callback(
        p_msg_id      => l_msg_id,
        p_callback_id => v_callback_id);
  elsif l_module ='SUP' THEN
    sup_supplier_pkg.process_callback_request(l_msg_id,v_callback_json, l_ct);
  elsif l_module = 'AR' then --Accounts Receivables Invoices Load and Autoinvoice ESS jobs
    ar_transactions_pkg.process_erp_callback(
        p_msg_id      => l_msg_id,
        p_callback_id => v_callback_id,
        p_ct          => l_ct);
  end if;

END store_erp_callback;


--               __________________________        
--      ________|                          |_______
--      \       |    Private routines.     |      /
--       \      |                          |     / 
--       /      |__________________________|     \ 
--      /__________)                    (_________\

function fault_exists(p_xml     in  xmltype
                     ,p_type    in  varchar2 
                     ,p_message out varchar2)
        return boolean;


--               _____________________________________________________________        
--      ________|                                                             |_______
--      \       |    Run_Report kjører navngitt erp bi rapport og lagrer      |      /
--       \      |    innholdet i lokal tabell.                                |     / 
--       /      |_____________________________________________________________|     \ 
--      /__________)                                                       (_________\
--			   01.04.2020  Gerrit Nijdam       	Opprettet
--			   31.08.2021  Håvard Standal 		Tilpasset til GLJ
--             30.08.2024  Håvard Standal       Tilpasset til xxebsintegration 

procedure run_report          ( p_path            in   varchar2
                               ,p_1name           in   varchar2 default null
							   ,p_1value          in   varchar2 default null
                               ,p_2name           in   varchar2 default null
							   ,p_2value          in   varchar2 default null
                               ,p_3name           in   varchar2 default null
							   ,p_3value          in   varchar2 default null
                               ,p_4name           in   varchar2 default null
							   ,p_4value          in   varchar2 default null
                               ,p_5name           in   varchar2 default null
							   ,p_5value          in   varchar2 default null
                               ,p_6name           in   varchar2 default null
							   ,p_6value          in   varchar2 default null
                               ,p_7name           in   varchar2 default null
							   ,p_7value          in   varchar2 default null
                               ,p_8name           in   varchar2 default null
							   ,p_8value          in   varchar2 default null
                               ,p_9name           in   varchar2 default null
							   ,p_9value          in   varchar2 default null
                               ,p_10name           in   varchar2 default null
							   ,p_10value          in   varchar2 default null
                               ,p_11name           in   varchar2 default null
							   ,p_11value          in   varchar2 default null
                               ,p_12name           in   varchar2 default null
							   ,p_12value          in   varchar2 default null
                               ,p_13name           in   varchar2 default null
							   ,p_13value          in   varchar2 default null
                               ,p_14name           in   varchar2 default null
							   ,p_14value          in   varchar2 default null
                               ,p_15name           in   varchar2 default null
							   ,p_15value          in   varchar2 default null
                               ,p_16name           in   varchar2 default null
							   ,p_16value          in   varchar2 default null
                               ,p_17name           in   varchar2 default null
							   ,p_17value          in   varchar2 default null
                               ,p_18name           in   varchar2 default null
							   ,p_18value          in   varchar2 default null
                               ,p_19name           in   varchar2 default null
							   ,p_19value          in   varchar2 default null
                               ,p_20name           in   varchar2 default null
							   ,p_20value          in   varchar2 default null
                               ,p_21name           in   varchar2 default null
							   ,p_21value          in   varchar2 default null
                               ,p_22name           in   varchar2 default null
							   ,p_22value          in   varchar2 default null
                               ,p_23name           in   varchar2 default null
							   ,p_23value          in   varchar2 default null
                               ,p_24name           in   varchar2 default null
							   ,p_24value          in   varchar2 default null
                               ,p_25name           in   varchar2 default null
							   ,p_25value          in   varchar2 default null
                               ,p_26name           in   varchar2 default null
							   ,p_26value          in   varchar2 default null
                               ,p_27name           in   varchar2 default null
							   ,p_27value          in   varchar2 default null
                               ,p_28name           in   varchar2 default null
							   ,p_28value          in   varchar2 default null
                               ,p_29name           in   varchar2 default null
							   ,p_29value          in   varchar2 default null
                               ,p_30name           in   varchar2 default null
							   ,p_30value          in   varchar2 default null
                               ,p_31name           in   varchar2 default null
							   ,p_31value          in   varchar2 default null
                               ,p_32name           in   varchar2 default null
							   ,p_32value          in   varchar2 default null
                               ,p_33name           in   varchar2 default null
							   ,p_33value          in   varchar2 default null
                               ,p_34name           in   varchar2 default null
							   ,p_34value          in   varchar2 default null
                               ,p_35name           in   varchar2 default null
							   ,p_35value          in   varchar2 default null							   
							   ,p_reportdata       in   clob     default null
							   ,p_storage_type     in   varchar2
							   ,p_store_in_table   in   varchar2
							   ,x_blob             out  blob
							   ,x_xml              out  xmltype
							   ,x_clob             out  clob
							   ,x_result           out  varchar2
							   ,x_message          out  varchar2
                              ) is
 l_method         varchar2(100) := '/xmlpserver/services/v2/ReportService';
 l_url            varchar2(100);
 l_xml            xmltype;
 l_envelope       clob;
 l_blob           blob;
 l_clob           clob;
 l_clob2          clob;
 l_id             number;
 
 --ERP credentials varaibles
 l_username       varchar2(50);
 l_password       varchar2(50);
 l_servername     varchar2(100);
 
 l_parameters     clob;
 l_reportdata     clob;
 l_message        varchar2(4000);
 my_string        varchar2(200);
BEGIN
    --dbms_output.put_line('Start run report');
   
	--get erp credentials
	BEGIN
      con_security_pkg.get_erp_credentials(l_username, l_password, l_servername);
	  
	  --Missing setup?
	  if l_username is null then
	    l_message := 'Usertype  '||c_role_run_report||' wrong or missing';
        
	    raise e_setup_error;
	  end if;
	  
    EXCEPTION
    WHEN OTHERS THEN
      l_message := 'Error getting ERP credentials: ' || SQLERRM;
      
      raise e_setup_error;
    END;
	
	--report data variable assignment	
	if p_reportdata is not null then
     	l_reportData := '<v2:reportData>'||p_reportdata||'</v2:reportData>';
	end if;
	
	--p1 og p35 variable assignment
    if p_1name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_1name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_1value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_2name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_2name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_2value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_3name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_3name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_3value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_4name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_4name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_4value||'</v2:item>
                </v2:values>
             </v2:item>';
    end if;
    if p_5name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_5name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_5value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_6name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_6name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_6value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_7name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_7name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_7value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_8name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_8name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_8value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_9name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_9name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_9value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_10name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_10name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_10value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_11name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_11name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_11value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_12name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_12name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_12value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_13name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_13name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_13value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_14name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_14name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_14value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_15name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_15name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_15value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_16name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_16name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_16value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_17name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_17name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_17value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_18name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_18name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_18value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_19name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_19name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_19value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_20name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_20name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_20value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_21name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_21name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_21value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_22name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_22name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_22value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_23name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_23name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_23value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_24name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_24name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_24value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_25name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_25name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_25value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_26name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_26name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_26value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_27name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_27name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_27value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_28name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_28name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_28value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_29name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_29name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_29value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_30name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_30name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_30value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_31name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_31name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_31value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_32name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_32name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_32value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_33name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_33name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_33value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_34name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_34name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_34value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
    if p_35name is not null then
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||p_35name||'</v2:name>
                <v2:values>
                   <v2:item>'||p_35value||'</v2:item>
                </v2:values>
             </v2:item>';
	end if;
	--end assignment of p1 to p35 variables
	
    l_envelope :=
    '<?xml version="1.0" encoding="utf-8"?>
	<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
     <soapenv:Header/>
     <soapenv:Body>
       <v2:runReport>
           <v2:reportRequest>
              <v2:parameterNameValues>
                <v2:listOfParamNameValues>'
                ||l_parameters||
				'</v2:listOfParamNameValues>
              </v2:parameterNameValues>
              <v2:reportAbsolutePath>'||p_path||'</v2:reportAbsolutePath>'
			  ||l_reportData||
          '</v2:reportRequest>
           <v2:userID>'||l_username||'</v2:userID>
           <v2:password>'||l_password||'</v2:password>
        </v2:runReport>
      </soapenv:Body>
     </soapenv:Envelope>';
    
    --dbms_output.put_line(l_servername||l_method);
    --dbms_output.put_line(l_envelope);
    
	
    begin
        l_xml := apex_web_service.make_request(p_url               => l_servername||l_method
                                              ,p_action            => ''
                                              ,p_envelope          => l_envelope
											  ,p_transfer_timeout  => 1200
    									      );
											  
	
    
    
    exception when others then
	    l_message := 'webservice call failed '||sqlerrm;
    	raise e_rest_failed;
    end;
	
    if fault_exists(l_xml,'XMLPSERVER',l_message) then
       	raise	e_fault_exists;
    end if;	
	
	--1. extract reportbytes from response
    l_clob := apex_web_service.parse_xml_clob(p_xml => l_xml,
                                          p_xpath => ' //runReportReturn/reportBytes/text()',
                                          p_ns=>'xmlns="http://xmlns.oracle.com/oxp/service/v2"');
    
	
    if    p_storage_type = 'CLOB' then
	    l_clob2 := l_clob;
	    
		if p_store_in_table = 'Y' then
             
             insert into DBO_CALL_RESPONSE (call_id,page_id,clob_data) values     (SYS_GUID(),1,l_clob);
             
	    end if;
    elsif p_storage_type = 'CSV' then
	    l_blob := apex_web_service.clobbase642blob(l_clob);
    else
	    --2. decode clob
        l_blob := apex_web_service.clobbase642blob(l_clob);
        --3. blob2clob
        l_clob := DBO_UTILS_PKG.blob2clob(l_blob);
        --4. create xml storable in xmltype
        l_xml := SYS.XMLTYPE.createXML(l_clob);
		dbms_lob.freetemporary(l_clob);
        --5. store into table
	    if p_store_in_table = 'Y' then
             insert into DBO_CALL_RESPONSE (call_id,page_id,xml_data) values     (SYS_GUID(),1,l_xml);	
	    end if;		
	end if;
    x_blob     := l_blob;
	x_xml      := l_xml;
	x_clob     := l_clob2;
 	x_result   := 'Success';
	x_message  := 'Rapporten er kjørt';

    
exception
    when e_rest_failed then
        x_result  := 'Error';
	    x_message := l_message;
	when e_setup_error then
		x_result := 'Error';
	    x_message := l_message;
when e_fault_exists then
        x_result := 'Error';
	    x_message := l_message;
	when others then
        x_result  := 'Error';
	    x_message := sqlerrm;
end run_report;


------------------------------------------------------------------------------------------------
--   Run_Report_V2 runs erp bi report, returns output and/or stores its contents in local table.
------------------------------------------------------------------------------------------------
procedure run_report_v2(
    p_path             in   varchar2
   ,p_parameters       in 	dwh_extract_parameter_tab						   
   ,p_reportdata       in   clob     default null
   ,p_storage_type     in   varchar2
   ,p_store_in_table   in   varchar2
   ,x_blob             out  blob
   ,x_xml              out  xmltype
   ,x_clob             out  clob
   ,x_result           out  varchar2
   ,x_message          out  varchar2
) is
 l_method         varchar2(100) := '/xmlpserver/services/v2/ReportService';
 l_url            varchar2(100);
 l_xml            xmltype;
 l_envelope       clob;
 l_blob           blob;
 l_clob           clob;
 l_clob2          clob;
 l_id             number;
 
 --ERP credentials varaibles
 l_username       varchar2(50);
 l_password       varchar2(50);
 l_servername     varchar2(100);
 
 l_parameters     clob;
 l_reportdata     clob;
 l_message        varchar2(4000);
 my_string        varchar2(200);
BEGIN
    --dbms_output.put_line('Start run report');
   
	--get erp credentials
	BEGIN
      con_security_pkg.get_erp_credentials(l_username, l_password, l_servername);
	  
	  --Missing setup?
	  if l_username is null then
	    l_message := 'Usertype  '||c_role_run_report||' wrong or missing';
        
	    raise e_setup_error;
	  end if;
	  
    EXCEPTION
    WHEN OTHERS THEN
      l_message := 'Error getting ERP credentials: ' || SQLERRM;
      
      raise e_setup_error;
    END;
	
	--report data variable assignment	
	if p_reportdata is not null then
     	l_reportData := '<v2:reportData>'||p_reportdata||'</v2:reportData>';
	end if;
	
    for par_rec in (select * from table(p_parameters))
    loop 
        l_parameters := l_parameters ||
            '<v2:item>
                <v2:name>'||par_rec.name||'</v2:name>
                <v2:values>
                   <v2:item>'||par_rec.value||'</v2:item>
                </v2:values>
             </v2:item>';
	end loop;
	
    l_envelope :=
    '<?xml version="1.0" encoding="utf-8"?>
	<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
     <soapenv:Header/>
     <soapenv:Body>
       <v2:runReport>
           <v2:reportRequest>
              <v2:parameterNameValues>
                <v2:listOfParamNameValues>'
                ||l_parameters||
				'</v2:listOfParamNameValues>
              </v2:parameterNameValues>
              <v2:reportAbsolutePath>'||p_path||'</v2:reportAbsolutePath>'
			  ||l_reportData||
          '</v2:reportRequest>
           <v2:userID>'||l_username||'</v2:userID>
           <v2:password>'||l_password||'</v2:password>
        </v2:runReport>
      </soapenv:Body>
     </soapenv:Envelope>';
    
    begin
        l_xml := apex_web_service.make_request(p_url               => l_servername||l_method
                                              ,p_action            => ''
                                              ,p_envelope          => l_envelope
											  ,p_transfer_timeout  => 1200
    									      );
											  
	
    
    
    exception when others then
	    l_message := 'webservice call failed '||sqlerrm;
    	raise e_rest_failed;
    end;
	
    if fault_exists(l_xml,'XMLPSERVER',l_message) then
       	raise	e_fault_exists;
    end if;	
	
	--1. extract reportbytes from response
    l_clob := apex_web_service.parse_xml_clob(p_xml => l_xml,
                                          p_xpath => ' //runReportReturn/reportBytes/text()',
                                          p_ns=>'xmlns="http://xmlns.oracle.com/oxp/service/v2"');
    
	
    if    p_storage_type = 'CLOB' then
	    l_clob2 := l_clob;
	    
		if p_store_in_table = 'Y' then
             
             insert into DBO_CALL_RESPONSE (call_id,page_id,clob_data) values     (SYS_GUID(),1,l_clob);
             
	    end if;
    elsif p_storage_type = 'CSV' then
	    l_blob := apex_web_service.clobbase642blob(l_clob);
    else
	    --2. decode clob
        l_blob := apex_web_service.clobbase642blob(l_clob);
        --3. blob2clob
        l_clob := DBO_UTILS_PKG.blob2clob(l_blob);
        --4. create xml storable in xmltype
        l_xml := SYS.XMLTYPE.createXML(l_clob);
		dbms_lob.freetemporary(l_clob);
        --5. store into table
	    if p_store_in_table = 'Y' then
             insert into DBO_CALL_RESPONSE (call_id,page_id,xml_data) values     (SYS_GUID(),1,l_xml);	
	    end if;		
	end if;
    x_blob     := l_blob;
	x_xml      := l_xml;
	x_clob     := l_clob2;
 	x_result   := 'Success';
	x_message  := 'Report completed successfuly';

    
exception
    when e_rest_failed then
        x_result  := 'Error';
	    x_message := l_message;
	when e_setup_error then
		x_result := 'Error';
	    x_message := l_message;
    when e_fault_exists then
        x_result := 'Error';
	    x_message := l_message;
	when others then
        x_result  := 'Error';
	    x_message := sqlerrm;
end run_report_v2;

-- *************************************************************
-- *  FAULT_EXISTS
-- *
-- *************************************************************
function fault_exists(p_xml     in  xmltype
                     ,p_type    in  varchar2 
                     ,p_message out varchar2)
        return boolean IS
begin
    if p_type = 'XMLPSERVER' then
       select xmlcast(xmlquery('declare namespace soapenv="http://schemas.xmlsoap.org/soap/envelope/"; (: :)
                         /soapenv:Envelope/soapenv:Body/soapenv:Fault/faultstring'
        PASSING p_xml returning content) as varchar2(1000)) text
        into p_message
        from dual
        where XMLExists('declare namespace soapenv="http://schemas.xmlsoap.org/soap/envelope/"; (: :)
                   /soapenv:Envelope/soapenv:Body/soapenv:Fault'
            PASSING p_xml);
	elsif p_type = 'FSCMSERVICE' then
        select xmlcast(xmlquery('declare namespace env="http://www.w3.org/2003/05/soap-envelope"; (: :)
                         /env:Envelope/env:Body/env:Fault/env:Reason/env:Text'
           PASSING p_xml returning content) as varchar2(1000)) text
        into p_message
        from dual
        where XMLExists('declare namespace env="http://www.w3.org/2003/05/soap-envelope"; (: :)
                   /env:Envelope/env:Body/env:Fault'
            PASSING p_xml);	
	end if;
	return (true);
exception 
   when NO_DATA_FOUND then
        return(false);
end fault_exists;
  
end dbo_erp_utils_pkg;
/