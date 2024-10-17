create or replace PACKAGE dbo_erp_utils_pkg AS

--         _  _                                                     _    _  _                     _           
--      __| || |__    ___           ___  _ __  _ __          _   _ | |_ (_)| | ___         _ __  | | __  __ _ 
--     / _` || `_ \  / _ \         / _ \| `__|| `_ \        | | | || __|| || |/ __|       | `_ \ | |/ / / _` |
--    | (_| || |_) || (_) |       |  __/| |   | |_) |       | |_| || |_ | || |\__ \       | |_) ||   < | (_| |
--     \__,_||_.__/  \___/  _____  \___||_|   | .__/  _____  \__,_| \__||_||_||___/ _____ | .__/ |_|\_\ \__, |
--                         |_____|            |_|    |_____|                       |_____||_|           |___/ 
--   
--	12.05.2024	Created/Modified from earlier EBS package	Håvard Standal	/ EBS Consulting AS

-- -------------------------------------------------
-- Change log
-- 2024.05.22 Petter Strand     Added rest_upload_file_to_ucm
--                              and rest_submit_ess_job_request  
-- 2024.05.27 Håvard Standal    Finalized rest_upload_file_to_ucm, rest_submit_ess_job_request, getESSExecutionDetails and erp_is_online
-- 2024.08.08 Håvard Standal    Made a new version of rest_submitt_ess_job_request named rest_submitt_ess_job_request_callback that takes relative call back parameter
-- 2024.08.10 Håvard Standal    Added fetchErrorDeocumentAndStore procedure. It will download error from erp for given requestid and store it connected to ta messeage.
-- 2024.08.11 Håvard Standal    Added Download_file procedure.
-- 2024.09.30 Håvard Standal    Added Run report procedure
-- 2024.09.31 Håvard Standal    Added common callback routine with support for jwt
-- 2024.09.03 Aldis Lagzdins    Procedure rest_upload_file_to_ucm new parameter p_encode_to_base64 to control if file content needs to be encoded to BASE64 (TRUE) or the calling program has encoded it already (FALSE)
-- 2024.09.27 Aldis Lagzdins    Created new version (run_report_v2) of run_report. Substituted single report paramaters with parameter table type.
-- -------------------------------------------------

--upload a file to ucm
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
                                , p_error_message    OUT VARCHAR2)  ;

--send essjob request 
PROCEDURE rest_submit_ess_job_request(p_msg_id           IN  RAW
                                    , p_module			 IN  VARCHAR2
                                    , p_rest_endpoint	 IN  VARCHAR2
                                    , p_job_package_name IN  VARCHAR2
                                    , p_job_def_name     IN  VARCHAR2
                                    , p_parameter_str    IN  VARCHAR2
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2);

--send essjob request with callback
PROCEDURE rest_submit_ess_job_request_callback(p_msg_id           IN  RAW
                                    , p_module			 IN  VARCHAR2       --CUR, ENS, AR, ....
                                    , p_rest_endpoint	 IN  VARCHAR2       
                                    , p_job_package_name IN  VARCHAR2
                                    , p_job_def_name     IN  VARCHAR2
                                    , p_parameter_str    IN  VARCHAR2
                                    , p_status_code      OUT NUMBER
                                    , p_requestid        OUT NUMBER
                                    , p_error_message    OUT VARCHAR2
                                    , p_callback         IN  VARCHAR2);    --/erpCallBack/callBack  

--send essjob request with callback
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
                                    , p_ct               IN  VARCHAR2);    


--Receive and store ERP ESS Job CallBack message. Called from the REST API
procedure store_erp_callback(p_data in CLOB, p_token_return in varchar2, p_status_code OUT NUMBER);
                                                                

--getESSExecutionDetails. Get the status from the Oracle ERP scheduler how a requestid was handled
procedure getESSExecutionDetails(p_msg_id IN RAW, p_module IN VARCHAR2, p_requestid IN number, p_response OUT clob, p_status out number, p_error_message out varchar2);	

-- returns 1 if ERP is up and ready to receive data.
function erp_is_online return boolean;

--Download errormessage from ERP for given requestid and store it connected to msg_id
procedure fetchErrorDocumentAndStore(p_msg_id IN RAW, p_requestid IN number); 

--Download error file
procedure download_file(p_msg_id in raw, p_requestid in number);

--Procedure to run repors in oracle erp cloud through reportservice
procedure run_report          ( p_path             in   varchar2
												   
                               ,p_1name            in   varchar2 default null
							   ,p_1value           in   varchar2 default null
                               ,p_2name            in   varchar2 default null
							   ,p_2value           in   varchar2 default null
                               ,p_3name            in   varchar2 default null
							   ,p_3value           in   varchar2 default null
                               ,p_4name            in   varchar2 default null
							   ,p_4value           in   varchar2 default null
                               ,p_5name            in   varchar2 default null
							   ,p_5value           in   varchar2 default null
                               ,p_6name            in   varchar2 default null
							   ,p_6value           in   varchar2 default null
                               ,p_7name            in   varchar2 default null
							   ,p_7value           in   varchar2 default null
                               ,p_8name            in   varchar2 default null
							   ,p_8value           in   varchar2 default null
                               ,p_9name            in   varchar2 default null
							   ,p_9value           in   varchar2 default null
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
                             );


--   Run_Report_V2 runs erp bi report, returns output and/or stores its contents in local table.
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
);

end dbo_erp_utils_pkg;
/