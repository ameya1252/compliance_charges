-- ──────────────────────────────────────────────────────────────────────────────
-- DMC-702 COMPLIANCE CHARGEBACK – SNOWPARK IMPLEMENTATION
--
-- Purpose
-- -------
-- Re-creates the KNIME “702 compliance charge” logic for expedited shipments
-- inside Snowflake using Python + Snowpark. The script:
--   1. Loads the expedited audit data into a temporary table (replace the
--      placeholder SELECT with your working query).
--   2. Filters out shipments from specific ship nodes (e.g., 800554, 800715).
--   3. Adds rule metadata, audit timestamp, constants, and converts the amount
--      to absolute value.
--   4. Constructs an AUDIT_COMMENTS field using the tracking number and
--      a fixed compliance message.
--   5. Drops intermediate fields (e.g., ship node, tracking number).
--   6. Reorders final columns to match the finance reporting interface.
--
-- Output
-- ------
-- Returns a Snowpark DataFrame with columns:
--   AUDIT_TIMESTAMP | PO_NUMBER | RULE_NUMBER | VIOLATE_FLAG
--   AUDIT_COMMENTS | CHARGEBACK_AMOUNT | ANCHOR_CONSTANT | SITE
--
-- Author : Ameya Deshmukh
-- Created: 2025-06-24
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE run_dmc702_chargeback()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as F

def main(session: snowpark.Session):
    from datetime import date, timedelta

    today = date.today()
    days_since_last_sunday = (today.weekday() + 1) % 7
    
    prev_sunday = today - timedelta(days=days_since_last_sunday + 7)
    prev_saturday = prev_sunday + timedelta(days=6)
    
    start_date = prev_sunday.strftime('%Y-%m-%d')
    end_date = prev_saturday.strftime('%Y-%m-%d')

    
    # 1.  Excel Reader  →  create temp table from your existing SQL query
    session.sql(f"""
        CREATE OR REPLACE TABLE MKT_DB.OMNICHANNEL.EXPEDITED_AUDIT AS 
        SELECT DISTINCT
          FSC.PO_ORDER_NO AS PO_NUMBER,
          FSC.ORDER_NO,
          FSC.ORDER_DATE,
          OL.OMS_ORDER_LINE_SCAC,
          OL.OMS_ORDER_LINE_CARRIER_SERVICE_CODE,
          OL.OMS_SHIP_NODE_KEY AS SHIPMENT_SHIP_NODE,
          FSC.SCAC,
          FSC.TRACKING_NO AS TRACKING_NUMBER,
          FSC.TRANS_AMT,
          (FSC.TRANS_AMT + 20) AS CHARGEBACK_AMOUNT,
          FDX.SC_FDX_SERVICE_TYPE,
          UPS.SC_UPS_ZONE
        FROM OMNICHANNEL.VW_FREIGHT_SHIPPING_COST AS FSC
        LEFT OUTER JOIN EDW.VW_OMS_ORDER_LINE AS OL ON OL.OMS_ORDER_LINE_KEY = FSC.ORDER_LINE_KEY
        LEFT OUTER JOIN EDW.SC_FDX_SHIPMENT_INVOICE AS FDX ON FSC.TRACKING_NO = FDX.SC_FDX_EXPRESS_OR_GROUND_TRACKING_ID
        LEFT OUTER JOIN EDW.VW_SC_UPS_INVOICE AS UPS ON UPS.SC_UPS_TRACKING_NO = FSC.TRACKING_NO
        LEFT OUTER JOIN EDW.VW_TIME_DIMENSION AS TD ON FSC.ORDER_DATE = TD.CAL_DATE
        WHERE OL.OMS_ORDER_LINE_CARRIER_SERVICE_CODE IN ('Ground', 'FedEx Ground')
          AND (
            FDX.SC_FDX_SERVICE_TYPE NOT IN (
              'Fedex Ground','FedEx Ground','FedEx Ground Economy',
              'FedEx Home Delivery (Ground)','FedEx Intl. Economy Export',
              'FedEx Intl. Economy Freight','FedEx Intl. Economy Import'
            )
            OR UPS.SC_UPS_ZONE NOT IN (
              '2','3','4','5','6','7','8','002','003','004','005','006','007','008',
              '44','45','46','044','045','046'
            )
          )
          AND FSC.PO_ORDER_NO IS NOT NULL
          AND FSC.ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
          
    """).collect()


    audit_df = session.table("MKT_DB.OMNICHANNEL.EXPEDITED_AUDIT")


    # 2.  Row Filter  – remove Ship Nodes 800554 / 800715
    filtered_df = (
        audit_df
        .filter(~F.col("SHIPMENT_SHIP_NODE").isin("800554", "800715"))
        .filter(F.col("CHARGEBACK_AMOUNT") != 0)
    )

    # 3.  Column Expressions  – add constants + absolute Chargeback
    enriched_df = (
        filtered_df
        .withColumn("RULE_NUMBER",        F.lit("DMC702"))
        .withColumn("AUDIT_TIMESTAMP",    F.regexp_replace(F.to_char(F.current_date(), "YYYY-MM-DD"), "-", ""))
        .withColumn("VIOLATE_FLAG",       F.lit("Y"))
        .withColumn("ANCHOR_CONSTANT",    F.lit("x"))
        .withColumn("SITE",               F.lit(5509))
        .withColumn("CHARGEBACK_AMOUNT",  F.abs(F.col("CHARGEBACK_AMOUNT")))
    )

    # 4.  Column Expressions  – build Audit Comments
    with_comments_df = (
        enriched_df
        .withColumn(
            "AUDIT_COMMENTS",
            F.concat(
                F.lit("DMC702 Tracking: "),
                F.coalesce(F.col("TRACKING_NUMBER").cast("string"), F.lit("")),
                F.lit(" Vendor expedited shipment without prior authorization from TSC's Dropship team. Please ship using the requested method on the PO")
            )
        )

    )

    # 5.  Column Filter  – drop intermediate columns
    pruned_df = (
        with_comments_df
        .drop("SHIPMENT_SHIP_NODE",
              "TRACKING_NUMBER")
    )

    # 6.  Column Resorter  – final order    
    final_df = pruned_df.select(
        "AUDIT_TIMESTAMP",
        "PO_NUMBER",
        "RULE_NUMBER",
        "VIOLATE_FLAG",
        "AUDIT_COMMENTS",
        "CHARGEBACK_AMOUNT",
        "ANCHOR_CONSTANT",
        "SITE"
    )

    final_df.write.mode("overwrite").save_as_table("MKT_DB.OMNICHANNEL.DMC702_OUT")

    return final_df
$$;


-- schedule task
CREATE OR REPLACE TASK run_dmc702_at_0800am
WAREHOUSE = OMS_WH
SCHEDULE = 'USING CRON 00 08 * * 1 America/Chicago'
AS
CALL run_dmc702_chargeback();



--start task
ALTER TASK run_dmc702_at_0800am RESUME;



--debugging

SHOW TASKS LIKE 'run_dmc702_at_0800am';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'RUN_DMC702_AT_0800AM',
    RESULT_LIMIT => 5
))
ORDER BY SCHEDULED_TIME DESC;


--checking output table
select * from MKT_DB.OMNICHANNEL.DMC702_OUT;






