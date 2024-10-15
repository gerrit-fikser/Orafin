CREATE OR REPLACE PACKAGE "XX_INTEGRATION_DEV"."AR_TRANSACTIONS_PKG" as
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


  --Store received payload (json). Invoked from receivables REST API
  PROCEDURE store_json_payload (
    p_data IN clob,
    p_msg_id out dbo_msg_inbound.msg_id%type,
    p_status_code OUT NUMBER,
    p_error_message out varchar2
  );
  
  --Validate received message before further processing. Invoked from WF.
  PROCEDURE validate_message (
    p_msg_id out dbo_msg_inbound.msg_id%type
  );

  --Insert message data into interface table. Invoked from WF.
  PROCEDURE msg_json_to_interface(
    p_msg_id in dbo_msg_inbound.msg_id%type
  );

  -- Prepare CSV files and Zip them before sending to ERP. Invoked form WF.
  PROCEDURE zip_csv_files(
    p_msg_id         in dbo_msg_inbound.msg_id%type,
    p_wf_instance_id in ar_transactions_out.wf_instance_id%type
  );

 --Take base64 from AR transactions out table and upload it to ERP. Invoked from WF.
PROCEDURE upload_zip_file_to_erp (
    p_msg_id        in  dbo_msg_inbound.msg_id%type,
    p_document_id   out ar_transactions_out.erp_document_id%type,
    p_status_code   out number,
    p_error_message out varchar2
 );

 --Start ERP load process to import data from zip file to interface table. Invoked from WF.
PROCEDURE submit_erp_interface_loader(
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_document_id in  ar_transactions_out.erp_document_id%type,
    p_request_id  out ar_transactions_out.erp_import_req_id%type
);

--Receive and store ERP ESS Job CallBack message. Invoked from erpCallBack REST API
PROCEDURE store_erp_callback(
    p_data in ar_callback_msg.message_data%type
);

--Start ERP Import Autoinvoice to load data from ERP interface tables to ERP base tables. Invoked from WF.
PROCEDURE submit_erp_autoinvoice(
    p_msg_id          in  dbo_msg_inbound.msg_id%type,
    p_load_request_id in  ar_transactions_out.erp_import_req_id%type,
    p_autoinv_req_id  out ar_transactions_out.erp_autoinvoice_req_id%type
);

--update all receivables integration related tables with final status. Invoked from WF.
PROCEDURE set_final_status (
    p_msg_id in dbo_msg_inbound.msg_id%type
);

end "AR_TRANSACTIONS_PKG";
/

