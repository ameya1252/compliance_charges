 

 

 

 

 

 

DMC Compliance Charges – Snowpark Implementation Handbook 

Documentation for Scripts: DMC 702, DMC 706, DMC 710 and Combined Chargebacks 

Author: Ameya Deshmukh 

Date: July 31, 2025 

Version: 1.4 

 

 

 

 

 

 

 

 

 

 

 

 

 

Table of Contents 

1  Introduction 

2  DMC 702 Compliance Chargeback 

3  DMC 710 Compliance Chargeback 

4  DMC 706 Compliance Chargeback 

5  Combined Chargeback Aggregation 

Appendix A  Revision History 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

Introduction 

 

 

This handbook provides detailed technical and operational documentation for the DMC compliance‑charge scripts implemented in Snowflake using Python Snowpark. It is intended for developers, data engineers, and analysts who need to understand, maintain, or enhance the existing implementations. 

 

 

Covered scripts: 

DMC 702 – Expedited Shipment Chargeback 

DMC 706 – Bill of Lading Compliance Chargeback 

DMC 710 – Cancellation Outside SLA Chargeback 

Combined Chargeback Aggregator – build_combined_chargebacks 

 

Each rule’s Python stored procedure has an associated Snowflake Task that schedules the weekly execution for the previous Sunday‑to‑Saturday window (America/Chicago). 

 

 

 

 

 

 

 

 

 

 

 

 

 

DMC 702 Compliance Chargeback 

 

Rule ID: DMC702 

 

Script Location: Snowflake Python Worksheet · Snowpark Stored Procedure run_dmc702_chargeback 

 

 

2.1  Purpose & Scope 

Re‑creates the KNIME “702 compliance charge” logic for expedited shipments inside Snowflake using Python + Snowpark. The rule identifies expedited shipments that violated cost guidelines and applies a fixed chargeback. 

 

 

2.2  High‑Level Workflow 

Determine reporting window – previous Sunday → previous Saturday. 

Create MKT_DB.OMNICHANNEL.EXPEDITED_AUDIT temp table from VW_FREIGHT_SHIPPING_COST and related joins. 

Filter out shipments from ship nodes 800554 & 800715 and zero‑amount rows. 

Enrich rows with metadata (RULE_NUMBER, AUDIT_TIMESTAMP, etc.) and convert chargeback amount to absolute value. 

Construct AUDIT_COMMENTS using tracking number & compliance text. 

Drop intermediate columns (SHIPMENT_SHIP_NODE, TRACKING_NUMBER). 

Re‑order final columns for Finance and persist to DMC702_OUT. 

 

 

 

 

 

 

 

 

 

 

 

2.3  Input Data Sources 

OMNICHANNEL.VW_FREIGHT_SHIPPING_COST – Freight & cost metrics 

EDW.VW_OMS_ORDER_LINE – Carrier information 

EDW.SC_FDX_SHIPMENT_INVOICE – FedEx service‑type lookup 

EDW.VW_SC_UPS_INVOICE – UPS zone lookup 

EDW.VW_TIME_DIMENSION – Calendar mapping 

 

 

 

2.4  Parameters & Constants 

Date Range: Previous Sun–Sat window (auto‑calculated) 

Excluded Ship Nodes: 800554, 800715 

Rule Metadata: RULE_NUMBER=DMC702, SITE=5509, VIOLATE_FLAG=Y, ANCHOR_CONSTANT=x 

 

 

 

2.5  Output Schema 

Column  

Type  

Notes  

AUDIT_TIMESTAMP  

VARCHAR(8)  

YYYYMMDD run date  

PO_NUMBER  

VARCHAR  

Purchase Order number  

RULE_NUMBER  

VARCHAR  

Always `DMC702`  

VIOLATE_FLAG  

CHAR(1)  

Always `Y`  

AUDIT_COMMENTS  

VARCHAR  

Trackingbased message  

CHARGEBACK_AMOUNT  

NUMBER(18,2)  

Absolute chargeback  

ANCHOR_CONSTANT  

CHAR(1)  

Always `x`  

SITE  

NUMBER  

5509  

 

 

 

 

 

 

2.6  Code Listing (Excerpt) 

import snowflake.snowpark as snowpark  
from snowflake.snowpark import functions as F  
  
def main(session: snowpark.Session):  
    session.sql("""CREATE OR REPLACE TEMP TABLE EXPEDITED_AUDIT AS ...""").collect()  
    audit_df = session.table("EXPEDITED_AUDIT")  
    filtered_df = (audit_df  
        .filter(~F.col("SHIPMENT_SHIP_NODE").isin("800554", "800715"))  
        .filter(F.col("CHARGEBACK_AMOUNT") != 0))  
    enriched_df = (filtered_df  
        .withColumn("RULE_NUMBER", F.lit("DMC702"))  
        .withColumn("AUDIT_TIMESTAMP",  
                    F.regexp_replace(F.to_char(F.current_date(), "YYYY-MM-DD"), "-", ""))  
        # additional transformations ...  
    )  
    return enriched_df.select("AUDIT_TIMESTAMP", "PO_NUMBER", ...)  

 

 

2.7  Scheduling 

Snowflake Task run_dmc702_at_0800am – 08:00 every Monday (America/Chicago) 

 

 

2.8  Maintenance & Troubleshooting 

Maintain the audit SQL as a view. 

Parameterize date windows and store ship‑node list in reference tables. 

Cross‑check chargeback totals versus KNIME output after changes. 

 

 

DMC 710 Compliance Chargeback 

 

Rule ID: DMC710 

 

Script Location: Snowflake Python Worksheet · Snowpark Stored Procedure run_dmc710_chargeback 

 

3.1  Purpose & Scope 

Implements the KNIME “710 compliance charge” for order cancellations outside the SLA, charging 3 % of the cancelled item cost. 

 

 

3.2  High‑Level Workflow 

Determine reporting window – previous Sunday → previous Saturday. 

Create staging tables: CANCELLATION_EXEMPT_QUERY and CHARGEBACK_CANCELS_QUERY. 

Filter exemptions and exclusion lists. 

Calculate chargeback as 3 % of LINE_TOTAL and add metadata. 

Aggregate by PO, deduplicate, and persist to DMC710_OUT. 

 

 

3.3  Input Data Sources 

EDW.VW_OMS_ORDER, EDW.VW_OMS_ORDER_LINE, EDW.VW_OMS_ORDER_RELEASE_STATUS – Order lifecycle 

EDW.VW_OMS_ORDER_AUDIT, EDW.VW_OMS_NOTE – Audit reasons & notes 

EDW.VW_PURCHASE_ORDER_LINE – Item cost 

 

 

3.4  Parameters & Constants 

Reporting Window: auto‑calculated previous Sun–Sat 

CHARGEBACK_RATE: 0.03 

Rule Metadata: RULE_NUMBER=DMC710, SITE=5509, VIOLATE_FLAG=Y, ANCHOR_CONSTANT=x 

 

3.5  Output Schema 

Column  

Type / Notes  

Details  

SITE  

NUMBER  

5509  

AUDIT_TIMESTAMP  

VARCHAR(8)  

Run date YYYYMMDD  

PO_NUMBER  

VARCHAR  

Purchase Order  

RULE_NUMBER  

VARCHAR  

Always `DMC710`  

VIOLATE_FLAG  

CHAR(1)  

Always `Y`  

AUDIT_COMMENTS  

VARCHAR  

Order & cancellation dates  

ANCHOR_CONSTANT  

CHAR(1)  

Always `x`  

CHARGEBACK_AMOUNT  

NUMBER(18,2)  

3 % of cost  

 

3.6  Code Listing (Excerpt) 

START_DATE_STR = '2025-03-30'  
END_DATE_STR   = '2025-04-06'  
  
def main(session):  
    session.sql("""CREATE OR REPLACE TEMP TABLE CANCELLATION_EXEMPT_QUERY AS ...""").collect()  
    session.sql("""CREATE OR REPLACE TEMP TABLE CHARGEBACK_CANCELS_QUERY AS ...""").collect()  
    df_cancels = session.table('CHARGEBACK_CANCELS_QUERY')  
    df_final = (df_cancels.filter(...)  
                .with_column('CHARGEBACK_AMOUNT',  
                             abs_(col('LINE_TOTAL')) * lit(0.03)))  
    return df_final.select('SITE', 'AUDIT_TIMESTAMP', ...)  

 

3.7  Scheduling 

Snowflake Task run_dmc710_at_0800am – 08:00 every Monday (America/Chicago) 

3.8  Maintenance & Troubleshooting 

Store exclusion lists in a control table for easy updates. 

Cluster OMS tables on OMS_ORDER_LINE_KEY to improve performance. 

Reconcile outputs against KNIME baseline to detect regressions. 

DMC 706 Compliance Chargeback 

 

Rule ID: DMC706 

4.1  Purpose & Scope 

Rule DMC706 enforces correct Bill of Lading (BOL) labelling for LTL shipments. If the shipper fails to place the words “Tractor Supply Company” at the start of the Ship From section, the carrier removes TSC’s negotiated discount and Finance issues a chargeback equal to 65 % of the total freight cost. 

 

4.2  Solution Variants 

706 V1 – Offshore Input Builder (OMNICHANNEL.BOL_706_INPUT) 

706 V2 – Chargeback Calculator (OMNICHANNEL.BOL_706_FINAL_OUTPUT) 

 

4.3  High‑Level Workflow 

V1 builds input and saves to …_INPUT. 

Offshore team flags violations and writes to …_OFFSHORE_OUTPUT. 

V2 reads flagged data, calculates chargebacks, and writes to …_FINAL_OUTPUT. 

 

 

4.4  Input Data Sources 

EDW.VW_SHIPMENT_LTL_CONTAINER_YW 

EDW.VW_SHIPMENT_LTL_CONTAINER 

OMNICHANNEL.BOL_706_OFFSHORE_OUTPUT 

 

 

4.5  Parameters & Constants 

DATE_FROM / DATE_TO: Pickup‑date window (configurable) 

DISCOUNT_PCT: 0.65 

RULE_NUM: DMC706 

SITE_ID: 5509 

ANCHOR_CONST: x 

 

 

4.6  Output Schema (V2) 

Column  

Type  

Notes  

SITE  

NUMBER  

5509  

AUDIT_TIMESTAMP  

VARCHAR(8)  

Rundate YYYYMMDD  

PO_NUMBER  

VARCHAR  

Purchase Order  

RULE_NUMBER  

VARCHAR  

Always `DMC706`  

VIOLATE_FLAG  

CHAR(1)  

`Y` if violated  

AUDIT_COMMENTS  

VARCHAR  

PRO + explanatory text  

CHARGEBACK_AMOUNT  

NUMBER(18,2)  

65 % of total charge  

ANCHOR_CONST  

CHAR(1)  

Always `x`  

 

 

 

4.7  Code Listings (Excerpts) 

# V1 – Offshore Input Builder 
CREATE OR REPLACE TABLE OMNICHANNEL.BOL_706_INPUT AS SELECT … 
 
# V2 – Chargeback Calculator 
df = session.table('OMNICHANNEL.BOL_706_OFFSHORE_OUTPUT') 
df_out.write.mode('overwrite').save_as_table('OMNICHANNEL.BOL_706_FINAL_OUTPUT') 

 

 

4.8  Maintenance & Troubleshooting 

Parameterise date windows for flexible reruns. 

Maintain contract with offshore team—do not duplicate their flag logic. 

Reconcile weekly chargeback totals versus carrier invoices. 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

Combined Chargeback Aggregation 

 

Utility procedure build_combined_chargebacks merges any set of DMC<rule>_OUT tables the same way KNIME’s Concatenate node does. 

 

5.1  Procedure Details 

Input: charge_list_str – comma‑separated list (e.g., '702,710') 

Output Table: MKT_DB.OMNICHANNEL.DMC_CHARGEBACK_COMBINED 

Logic: 

Validate each rule output table exists. 

Union columns into a superset, aligning NULLs for missing columns. 

Deduplicate RowID (or PO_NUMBER) – duplicates get _dup suffix. 

Overwrite combined table. 

 

 

 

5.2  Scheduling 

Snowflake Task combine_chargebacks_monday_0900 – 09:00 every Monday (America/Chicago) 

 

 

5.3  Maintenance Notes 

Update the charge list when new rule tables are added or retired. 

Monitor INFORMATION_SCHEMA.TASK_HISTORY for successful concatenation. 

Validate combined row counts versus individual rule outputs. 

 

 

 

 

 

 

 

 

 

 

Appendix A  Revision History 

Version 

Date 

Author 

Description 

1.0 

2025‑07‑09 

Ameya Deshmukh 

Initial handbook covering DMC 702 and DMC 710 

1.1 

2025‑07‑09 

Ameya Deshmukh 

Minor edits, formatting 

1.2 

2025‑07‑28 

Ameya Deshmukh 

Added full DMC 706 documentation; updated TOC 

1.3 

2025‑07‑28 

Ameya Deshmukh 

Formal formatting cleanup and merged full original content 

1.4 

2025‑07‑31 

Ameya Deshmukh 

Added Snowpark excerpts, task schedules, and combined chargeback aggregation 

 
