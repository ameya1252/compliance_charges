-- ──────────────────────────────────────────────────────────────────────────────
-- DMC-710 COMPLIANCE CHARGEBACK – SNOWPARK IMPLEMENTATION
--
-- Purpose
-- -------
-- Re-creates the KNIME “710 compliance charge” logic (cancellations outside SLA)
-- inside Snowflake using Python + Snowpark. The script:
--   1. Defines a global reporting window using START_DATE_STR and END_DATE_STR.
--   2. Creates two staging tables:
--        • CANCELLATION_EXEMPT_QUERY – flags orders with valid DSV exceptions.
--        • CHARGEBACK_CANCELS_QUERY  – captures potential violations with demand,
--          ship node, audit, and cost information.
--   3. Excludes exempted rows based on flags, audit reason codes, and note text.
--   4. Calculates the chargeback (3% of item cost), adds rule metadata, and
--      builds audit comments using order and cancel dates.
--   5. Removes intermediate columns and reorders final output for Finance use.
--
-- Output
-- ------
-- Returns a Snowpark DataFrame with columns:
--   SITE | AUDIT_TIMESTAMP | PO_NUMBER | RULE_NUMBER | VIOLATE_FLAG
--   AUDIT_COMMENTS | ANCHOR_CONSTANT | CHARGEBACK_AMOUNT
--
-- Author : Ameya Deshmukh
-- Created: 2025-06-24
-- ──────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE run_dmc710_chargeback()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import (
    col,
    concat,
    lit,
    coalesce,
    abs as abs_,
    regexp_replace,
    substr,
    concat_ws,
    sum as sum_,
    current_timestamp,
)

A_EXCLUDE_REASONS = [
    "Customer Requests Cancellation witin SLA",
    "Customer Requests Cancellation within SLA",
    "Customer Requests Cancellation in SLA",
    "Customer Request Cancellation within SLA",
    "TSC Requests Cancellation Exempt",
    "TSC Requests Cancellation  Exempt",
    "Vendor Can't Ship to Address",
    "PO Not Received",
    "Weather Delay",
]

NOTE_EXCLUDE_REASONS = [
    "Closed for Federal Holiday",
    "Vendor closed for Inventory Audit",
    "PO received with 0$ cost",
]

def main(session: snowpark.Session):

    from datetime import date, timedelta

    # Calculate previous Sunday and Saturday
    today = date.today()
    days_since_last_sunday = (today.weekday() + 1) % 7
    
    prev_sunday = today - timedelta(days=days_since_last_sunday + 7)
    prev_saturday = prev_sunday + timedelta(days=6)
    
    START_DATE_STR = prev_sunday.strftime('%Y-%m-%d')
    END_DATE_STR   = prev_saturday.strftime('%Y-%m-%d')
    
    """Entry‑point for Snowflake Python worksheet."""

    # ------------------------------------------------------------------
    # STEP 23 & 25 – Load the two input datasets from SQL views / tables
    # ------------------------------------------------------------------
    session.sql(f"""
    create or replace table MKT_DB.OMNICHANNEL.CANCELLATION_EXEMPT_QUERY as
    
    Select
    Date(rdl.OMS_Order_Release_Status_TS) Status_Date
    ,Case
        When ord1.OMS_Order_No is not null
        Then ord.OMS_Order_No
        Else Null
    End PO_Order_No
    ,Case
        When ord1.OMS_Order_No is null
        Then ord.OMS_Order_No
        Else ord1.OMS_Order_No
    End Order_No
    ,CASE
        WHEN nt.OMS_Note_RESN_Code in ('Customer Requests Cancellation witin SLA') THEN 'Y'
        WHEN nt.OMS_Note_RESN_Code in ('Customer Requests Cancellation within SLA') THEN 'Y'
        WHEN nt.OMS_Note_RESN_Code in ('TSC Requests Cancellation Exempt') THEN 'Y'
        WHEN nt.OMS_Note_RESN_Code in ('TSC Requests Cancellation  Exempt') THEN 'Y'
        WHEN nt.OMS_Note_RESN_Code in ('Vendor Can''t Ship to Address') THEN 'Y'
        When nt.OMS_Note_RESN_Code in ('PO Not Received') Then 'Y'
        When nt.OMS_Note_RESN_Code in ('PO has not been received') Then 'Y'
        When nt.OMS_Note_RESN_Code in ('Weather Delay') Then 'Y'
        When nt.OMS_Note_RESN_Code in ('Item is cancelled - Cannot ship to State selected') Then 'Y'
        When nt.OMS_Note_Text like ('%Vendor cannot ship to address - customer input needed%') Then 'Y'
        When nt.OMS_Note_Text like ('%Carrier pick up requested, but not picked up yet%') Then 'Y'
        When nt.OMS_Note_Text like ('%Closed for Federal Holiday%') Then 'Y'
        When nt.OMS_Note_Text like ('%Vendor closed for Inventory Audit%') Then 'Y'
        When nt.OMS_Note_Text like ('%PO received with 0$ cost%') Then 'Y'
        Else 'N'
    END DSV_Exception
    //,nt.OMS_Note_RESN_Code
    //,nt.OMS_Note_Text
    From EDW.VW_OMS_ORDER ord
    Inner Join MKT_DB.EDW.VW_TIME_DIMENSION td On
        Date(ord.OMS_ORDER_TS) = td.Cal_Date
    Inner Join EDW.VW_OMS_ORDER_LINE ol On
        ord.OMS_Order_Key = ol.OMS_Order_Key 
    Left Outer Join MKT_DB.EDW.VW_OMS_ORDER ord1 On
            ord1.OMS_ORDER_KEY = ol.OMS_Chain_Order_Key
    Inner Join EDW.VW_OMS_ORDER_LINE ol1 On
        ol.OMS_Chain_Order_Line_Key = ol1.OMS_Order_Line_Key 
    INNER JOIN EDW.VW_OMS_SHIP_NODE sn ON
        ol.OMS_SHIP_NODE_KEY = sn.OMS_SHIP_NODE_KEY
    Left Outer Join EDW.VW_OMS_ORDER_AUDIT oa ON
        ord.OMS_ORDER_KEY = oa.OMS_ORDER_KEY
        And
        oa.OMS_ORDER_AUDIT_RESN_CODE is not null
    Left Outer Join EDW.VW_OMS_NOTE nt On
        ord.OMS_Order_Key = nt.OMS_Table_Key
    Inner Join 
    (
        Select
            a1.RCO
            ,a1.OMS_Order_Release_Status
            ,a1.OMS_Status_Desc
            ,a1.OMS_Order_No
            ,a1.OMS_Item_Id
            ,a1.OMS_Order_Line_Key
            ,a1.OMS_Order_Release_Status_TS
            ,a1.OMS_Order_Release_Status_Key
            ,a1.OMS_ORDER_LINE_SCHEDULE_KEY
            ,a1.OMS_Order_Release_Key
            ,a1.OMS_Order_Key
            ,a1.OMS_Order_Release_Status_Total_Qty
            ,a1.OMS_Doc_Type
        From
        (
            Select
                Dense_rank () Over (Partition By ol.OMS_Order_Line_Key Order by ors.OMS_Order_Release_Status desc ) RCO
                ,ors.OMS_Order_Release_Status
                ,sts.OMS_Status_Desc
                ,ord.OMS_Order_No
                ,ol.OMS_Item_Id
                ,ol.OMS_Order_Line_Key
                ,ors.OMS_Order_Release_Status_TS
                ,ors.OMS_ORDER_RELEASE_STATUS_KEY
                ,ors.OMS_ORDER_RELEASE_KEY
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
                ,ors.OMS_ORDER_KEY
                ,ors.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
                ,ord.OMS_Doc_Type
            From MKT_DB.EDW.VW_OMS_ORDER_RELEASE_STATUS ors
            Inner Join MKT_DB.EDW.VW_OMS_Order_Line ol On
                ol.OMS_Order_Line_Key = ors.OMS_Order_Line_Key
            Inner Join MKT_DB.EDW.VW_OMS_Order ord On
                ors.OMS_ORDER_KEY = ord.OMS_ORDER_KEY
            Inner Join
            (
                Select
                    orm.OMS_ORDER_KEY
                    ,orm.OMS_Order_Line_Key
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
                    ,MAX(orm.OMS_Order_Release_Status_TS) Release_Status_Date
                From MKT_DB.EDW.VW_OMS_ORDER_RELEASE_STATUS orm
                Inner Join MKT_DB.EDW.VW_OMS_Order_Line ol On
                    ol.OMS_Order_Line_Key = orm.OMS_Order_Line_Key
                Inner Join MKT_DB.EDW.VW_OMS_Order ord On
                    orm.OMS_ORDER_KEY = ord.OMS_ORDER_KEY
                Inner Join MKT_DB.EDW.VW_TIME_DIMENSION tm On
                    tm.Cal_Date = Date(ol.OMS_Order_TS)
                Where
            //        tm.Year_Week = '202109'
        //            ord.OMS_Order_No in ('1081009984','1081056266','1081056266','1081056266','1081056266','1081056266','1081073693','1081073693','1080914091','1081380815','1081618330')
    //                orm.OMS_Order_Line_Key = '202109151848027785509002'
    //                And
                    ord.OMS_Doc_Type = '0005'
                Group By
                    orm.OMS_ORDER_KEY
                    ,orm.OMS_Order_Line_Key
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
            ) orm On 
                orm.OMS_Order_Line_Key = ol.OMS_Order_Line_Key
                And 
                orm.Release_Status_Date = ors.OMS_Order_Release_Status_TS
            Inner Join MKT_DB.EDW.VW_OMS_STATUS sts On
                sts.OMS_Status = ors.OMS_Order_Release_Status
                And
                sts.OMS_Process_Type_Key = 'PO_FULFILLMENT'
            Where
                ord.OMS_Doc_Type = '0005'
        //    Order By
        //        ord.OMS_Order_No
        //        ,ors.OMS_Order_Release_Status desc
        ) a1
        Where RCO = 1
    ) rdl On
        ol.OMS_Order_Line_Key = rdl.OMS_Order_Line_Key    
    Inner Join 
    ( 
        SELECT DISTINCT
            ORS.OMS_ORDER_KEY
            ,ORS.OMS_ORDER_LINE_KEY
            ,ORS.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
            ,ORS.OMS_ORDER_RELEASE_STATUS
            ,MIN(OMS_ORDER_RELEASE_STATUS_TS) AS STATUS_DATE
        FROM EDW.VW_OMS_ORDER_RELEASE_STATUS ORS
        WHERE ORS.OMS_ORDER_RELEASE_STATUS = '1100'
        GROUP BY 
            ORS.OMS_ORDER_KEY
            ,ORS.OMS_ORDER_LINE_KEY
            ,ORS.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
            ,ORS.OMS_ORDER_RELEASE_STATUS
    )  rs_11 On
        ol.OMS_Order_Line_Key = rs_11.OMS_Order_Line_Key
    Left Outer Join
    (
        Select
           ocl.OMS_ORDER_LINE_KEY
           ,ocl.OMS_ORDER_CANCEL_RESN_CODE
           ,ocl.OMS_ORDER_CANCEL_RESN_NOTE
           ,mocl.OMS_ORDER_CANCEL_QTY
        From 
        (
            Select
                OMS_ORDER_LINE_KEY
                ,Max(OMS_Create_TS) Max_OMS_Create_TS
                ,Sum(OMS_ORDER_CANCEL_QTY) OMS_ORDER_CANCEL_QTY
            From EDW.VW_OMS_ORDER_CANCEL_LINE ocl
            Group By
                OMS_ORDER_LINE_KEY
        ) mocl
        Inner Join EDW.VW_OMS_ORDER_CANCEL_LINE ocl On
            mocl.OMS_ORDER_LINE_KEY = ocl.OMS_ORDER_LINE_KEY
            And
            mocl.Max_OMS_Create_TS = ocl.OMS_Create_TS
    ) ocl On
        ocl.OMS_ORDER_LINE_KEY = ol1.OMS_ORDER_LINE_KEY
    
    WHERE
      (
       ord.OMS_DOC_TYPE
      =  '0005'
       AND
        rdl.OMS_Order_Release_Status_TS::DATE >= '{START_DATE_STR}'
        AND 
        rdl.OMS_Order_Release_Status_TS::DATE <= '{END_DATE_STR}'
      )
    /* User = @variable('BOUSER'); Report = @variable('DOCNAME'); Universe = @VARIABLE('UNVNAME') */
    """).collect()

    df_exempt = session.table('MKT_DB.OMNICHANNEL.CANCELLATION_EXEMPT_QUERY')
    

    session.sql(f"""
    create or replace table MKT_DB.OMNICHANNEL.CHARGEBACK_CANCELS_QUERY as

    Select
    Order_Date
    ,Status_Date
    ,PO_Order_No
    ,Order_No
    ,OMS_ITEM_ID
    ,OMS_ORDER_LINE_UNIT_PRICE
    ,Status_Qty
    ,Demand_Qty
    ,Demand_Sales
    ,OMS_Order_Release_Status
    ,OMS_Status_Desc
    ,OMS_ORDER_AUDIT_RESN_CODE
    ,OMS_ORDER_AUDIT_RESN_TEXT
    ,OMS_ORDER_CANCEL_RESN_CODE
    ,OMS_ORDER_CANCEL_RESN_NOTE
    ,OMS_SHIP_NODE_KEY
    ,OMS_SHIP_NODE_DESC
    ,c4.PO_LINE_UNIT_PRICE * Status_Qty Item_Cost
    From
    (
    Select
    Date(ord.OMS_ORDER_TS) Order_Date
    ,Date(rdl.OMS_Order_Release_Status_TS) Status_Date
    ,Case
        When ord1.OMS_Order_No is not null
        Then ord.OMS_Order_No
        Else Null
    End PO_Order_No
    ,Case
        When ord1.OMS_Order_No is null
        Then ord.OMS_Order_No
        Else ord1.OMS_Order_No
    End Order_No
    ,ol.OMS_ITEM_ID
    ,ol.OMS_ORDER_LINE_UNIT_PRICE
    ,rdl.OMS_Order_Release_Status_Total_Qty Status_Qty
    ,CASE
        WHEN rs_11.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY is not null
        THEN rs_11.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
        ELSE ol.OMS_ORDER_LINE_ORIG_ORDER_QTY
    END Demand_Qty
    ,CASE
        WHEN rs_11.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY is not null
        THEN ol.OMS_ORDER_LINE_UNIT_PRICE * rs_11.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
        ELSE ol.OMS_ORDER_LINE_ORIG_ORDER_QTY * ol.OMS_ORDER_LINE_UNIT_PRICE
    END Demand_Sales
    ,rdl.OMS_Order_Release_Status
    ,rdl.OMS_Status_Desc
    ,oa.OMS_ORDER_AUDIT_RESN_CODE
    ,oa.OMS_ORDER_AUDIT_RESN_TEXT
    ,ocl.OMS_ORDER_CANCEL_RESN_CODE
    ,ocl.OMS_ORDER_CANCEL_RESN_NOTE
    ,sn.OMS_SHIP_NODE_KEY
    ,sn.OMS_SHIP_NODE_DESC
    From EDW.VW_OMS_ORDER ord
    Inner Join MKT_DB.EDW.VW_TIME_DIMENSION td On
        Date(ord.OMS_ORDER_TS) = td.Cal_Date
    Inner Join EDW.VW_OMS_ORDER_LINE ol On
        ord.OMS_Order_Key = ol.OMS_Order_Key 
    Left Outer Join MKT_DB.EDW.VW_OMS_ORDER ord1 On
            ord1.OMS_ORDER_KEY = ol.OMS_Chain_Order_Key
    Inner Join EDW.VW_OMS_ORDER_LINE ol1 On
        ol.OMS_Chain_Order_Line_Key = ol1.OMS_Order_Line_Key 
    INNER JOIN EDW.VW_OMS_SHIP_NODE sn ON
        ol.OMS_SHIP_NODE_KEY = sn.OMS_SHIP_NODE_KEY
    Left Outer Join EDW.VW_OMS_ORDER_AUDIT oa ON
        ord.OMS_ORDER_KEY = oa.OMS_ORDER_KEY
        And
        oa.OMS_ORDER_AUDIT_RESN_CODE is not null
    Inner Join 
    (
        Select
            a1.RCO
            ,a1.OMS_Order_Release_Status
            ,a1.OMS_Status_Desc
            ,a1.OMS_Order_No
            ,a1.OMS_Item_Id
            ,a1.OMS_Order_Line_Key
            ,a1.OMS_Order_Release_Status_TS
            ,a1.OMS_Order_Release_Status_Key
            ,a1.OMS_ORDER_LINE_SCHEDULE_KEY
            ,a1.OMS_Order_Release_Key
            ,a1.OMS_Order_Key
            ,a1.OMS_Order_Release_Status_Total_Qty
            ,a1.OMS_Doc_Type
        From
        (
            Select
                Dense_rank () Over (Partition By ol.OMS_Order_Line_Key Order by ors.OMS_Order_Release_Status desc ) RCO
                ,ors.OMS_Order_Release_Status
                ,sts.OMS_Status_Desc
                ,ord.OMS_Order_No
                ,ol.OMS_Item_Id
                ,ol.OMS_Order_Line_Key
                ,ors.OMS_Order_Release_Status_TS
                ,ors.OMS_ORDER_RELEASE_STATUS_KEY
                ,ors.OMS_ORDER_RELEASE_KEY
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
                ,ors.OMS_ORDER_KEY
                ,ors.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
                ,ord.OMS_Doc_Type
            From MKT_DB.EDW.VW_OMS_ORDER_RELEASE_STATUS ors
            Inner Join MKT_DB.EDW.VW_OMS_Order_Line ol On
                ol.OMS_Order_Line_Key = ors.OMS_Order_Line_Key
            Inner Join MKT_DB.EDW.VW_OMS_Order ord On
                ors.OMS_ORDER_KEY = ord.OMS_ORDER_KEY
            Inner Join
            (
                Select
                    orm.OMS_ORDER_KEY
                    ,orm.OMS_Order_Line_Key
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
                    ,MAX(orm.OMS_Order_Release_Status_TS) Release_Status_Date
                From MKT_DB.EDW.VW_OMS_ORDER_RELEASE_STATUS orm
                Inner Join MKT_DB.EDW.VW_OMS_Order_Line ol On
                    ol.OMS_Order_Line_Key = orm.OMS_Order_Line_Key
                Inner Join MKT_DB.EDW.VW_OMS_Order ord On
                    orm.OMS_ORDER_KEY = ord.OMS_ORDER_KEY
                Inner Join MKT_DB.EDW.VW_TIME_DIMENSION tm On
                    tm.Cal_Date = Date(ol.OMS_Order_TS)
                Where
                    ord.OMS_Doc_Type = '0005'
                Group By
                    orm.OMS_ORDER_KEY
                    ,orm.OMS_Order_Line_Key
                    ,orm.OMS_ORDER_LINE_SCHEDULE_KEY
            ) orm On 
                orm.OMS_Order_Line_Key = ol.OMS_Order_Line_Key
                And 
                orm.Release_Status_Date = ors.OMS_Order_Release_Status_TS
            Inner Join MKT_DB.EDW.VW_OMS_STATUS sts On
                sts.OMS_Status = ors.OMS_Order_Release_Status
                And
                sts.OMS_Process_Type_Key = 'PO_FULFILLMENT'
            Where
                ord.OMS_Doc_Type = '0005'
        //    Order By
        //        ord.OMS_Order_No
        //        ,ors.OMS_Order_Release_Status desc
        ) a1
        Where RCO = 1
    ) rdl On
        ol.OMS_Order_Line_Key = rdl.OMS_Order_Line_Key
    Inner Join 
    ( 
        SELECT DISTINCT
            ORS.OMS_ORDER_KEY
            ,ORS.OMS_ORDER_LINE_KEY
            ,ORS.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
            ,ORS.OMS_ORDER_RELEASE_STATUS
            ,MIN(OMS_ORDER_RELEASE_STATUS_TS) AS STATUS_DATE
        FROM EDW.VW_OMS_ORDER_RELEASE_STATUS ORS
        WHERE ORS.OMS_ORDER_RELEASE_STATUS = '1100'
        GROUP BY 
            ORS.OMS_ORDER_KEY
            ,ORS.OMS_ORDER_LINE_KEY
            ,ORS.OMS_ORDER_RELEASE_STATUS_TOTAL_QTY
            ,ORS.OMS_ORDER_RELEASE_STATUS
    )  rs_11 On
        ol.OMS_Order_Line_Key = rs_11.OMS_Order_Line_Key
    Left Outer Join
    (
        Select
           ocl.OMS_ORDER_LINE_KEY
           ,ocl.OMS_ORDER_CANCEL_RESN_CODE
           ,ocl.OMS_ORDER_CANCEL_RESN_NOTE
           ,mocl.OMS_ORDER_CANCEL_QTY
        From 
        (
            Select
                OMS_ORDER_LINE_KEY
                ,Max(OMS_Create_TS) Max_OMS_Create_TS
                ,Sum(OMS_ORDER_CANCEL_QTY) OMS_ORDER_CANCEL_QTY
            From EDW.VW_OMS_ORDER_CANCEL_LINE ocl
            Group By
                OMS_ORDER_LINE_KEY
        ) mocl
        Inner Join EDW.VW_OMS_ORDER_CANCEL_LINE ocl On
            mocl.OMS_ORDER_LINE_KEY = ocl.OMS_ORDER_LINE_KEY
            And
            mocl.Max_OMS_Create_TS = ocl.OMS_Create_TS
    ) ocl On
        ocl.OMS_ORDER_LINE_KEY = ol1.OMS_ORDER_LINE_KEY
    WHERE
      (
       ord.OMS_DOC_TYPE
      =  '0005'
       AND
      rdl.OMS_ORDER_RELEASE_STATUS  =  '9000'
      And
      ol.OMS_ITEM_CAT_CODE not in ('983','981','782')  
        AND
       TO_DATE(TO_CHAR(rdl.OMS_Order_Release_Status_TS, 'YYYY-MM-DD'), 'YYYY-MM-DD') >= '{START_DATE_STR}'
        AND
        TO_DATE(TO_CHAR(rdl.OMS_Order_Release_Status_TS, 'YYYY-MM-DD'), 'YYYY-MM-DD') <= '{END_DATE_STR}'
      )
    ) a1
    Left Outer Join
    (
        SELECT
            EDW.VW_PURCHASE_ORDER_LINE.PO_NO,
            EDW.VW_ARTICLE.ARTICLE_NO,
            EDW.VW_PURCHASE_ORDER_LINE.PO_LINE_UNIT_PRICE
        FROM EDW.VW_TIME_DIMENSION
        INNER JOIN EDW.VW_PURCHASE_ORDER_LINE ON 
            EDW.VW_TIME_DIMENSION.CAL_DATE=EDW.VW_PURCHASE_ORDER_LINE.DELIV_DATE
        INNER JOIN EDW.VW_STORE ON 
            EDW.VW_STORE.STORE_NO=EDW.VW_PURCHASE_ORDER_LINE.STORE_NO
        INNER JOIN EDW.VW_ARTICLE ON 
            EDW.VW_ARTICLE.ARTICLE_NO=EDW.VW_PURCHASE_ORDER_LINE.ARTICLE_NO
        INNER JOIN EDW.VW_VENDOR ON 
            EDW.VW_VENDOR.VENDOR_ID=EDW.VW_PURCHASE_ORDER_LINE.VENDOR_ID
        WHERE
            EDW.VW_STORE.DISTB_CHAN_CODE = 30
        GROUP BY
            EDW.VW_PURCHASE_ORDER_LINE.PO_NO, 
            EDW.VW_ARTICLE.ARTICLE_NO, 
            EDW.VW_PURCHASE_ORDER_LINE.PO_LINE_UNIT_PRICE
    ) c4 On
        PO_Order_No = c4.PO_NO
        And
        OMS_ITEM_ID = c4.ARTICLE_NO
    Group By
    Order_Date
    ,Status_Date
    ,PO_Order_No
    ,Order_No
    ,OMS_ITEM_ID
    ,OMS_ORDER_LINE_UNIT_PRICE
    ,Status_Qty
    ,Demand_Qty
    ,Demand_Sales
    ,OMS_Order_Release_Status
    ,OMS_Status_Desc
    ,OMS_ORDER_AUDIT_RESN_CODE
    ,OMS_ORDER_AUDIT_RESN_TEXT
    ,OMS_ORDER_CANCEL_RESN_CODE
    ,OMS_ORDER_CANCEL_RESN_NOTE
    ,OMS_SHIP_NODE_KEY
    ,OMS_SHIP_NODE_DESC
    ,c4.PO_LINE_UNIT_PRICE
    """).collect()
    
    df_cancels = session.table('MKT_DB.OMNICHANNEL.CHARGEBACK_CANCELS_QUERY')

    # --------------------------------------------------------------
    # STEP 24 – Filter Exemptions flag = 'Y'
    # --------------------------------------------------------------
    df_exempt_f = (
        df_exempt
        .filter(col("DSV_EXCEPTION") == lit("Y"))
        .select(col("PO_ORDER_NO"))
    )

    # --------------------------------------------------------------
    # STEP 26 – Exclude REMORSE cancellations
    # --------------------------------------------------------------
    df_cancels_f = df_cancels.filter(col("OMS_ORDER_CANCEL_RESN_CODE") != lit("REMORSE"))

    # --------------------------------------------------------------
    # STEP 27 – Remove POs that are in the exemption list (anti‑join)
    # --------------------------------------------------------------
    df_joined = df_cancels_f.join(
        df_exempt_f, df_cancels_f["PO_ORDER_NO"] == df_exempt_f["PO_ORDER_NO"], how="leftanti"
    )


    # --------------------------------------------------------------
    # STEP 28 – Scenario exclusions (output non‑matching rows in KNIME)
    # --------------------------------------------------------------
    df_filtered = (
        df_joined
        .filter(~col("OMS_ORDER_AUDIT_RESN_CODE").isin(A_EXCLUDE_REASONS) | col("OMS_ORDER_AUDIT_RESN_CODE").is_null())
        .filter(~col("OMS_ORDER_CANCEL_RESN_NOTE").isin(NOTE_EXCLUDE_REASONS) | col("OMS_ORDER_CANCEL_RESN_NOTE").is_null())
    )

    #df_filtered = df_filtered.filter(col("ITEM_COST").is_not_null())

    # --------------------------------------------------------------
    # STEP 29 – Trim to the working‑set columns
    # --------------------------------------------------------------
    base_df = df_filtered.select(
        col("ORDER_DATE"),                         # Order Date
        col("STATUS_DATE").alias("CANCELLATION_DATE"),
        col("PO_ORDER_NO").alias("PO_NUMBER"),
        col("ITEM_COST").alias("LINE_TOTAL"),
    )

    # --------------------------------------------------------------
    # STEP 30 – Add metadata & chargeback calc
    # --------------------------------------------------------------
    df_expr = (
        base_df.fillna({"LINE_TOTAL": 0})  # fill LINE_TOTAL nulls first
        .with_column("RULE_NUMBER",     lit("DMC710"))
        .with_column("SITE",            lit(5509))
        .with_column("AUDIT_TIMESTAMP", regexp_replace(substr(current_timestamp().cast("string"), lit(0), lit(10)), lit("-"), lit("")))
        .with_column("VIOLATE_FLAG",    lit("Y"))
        .with_column("ANCHOR_CONSTANT", lit("x"))
        .with_column("CHARGEBACK_AMOUNT", abs_(col("LINE_TOTAL")) * lit(0.03))
    )


    # --------------------------------------------------------------
    # STEP 31 – Build AUDIT_COMMENTS concat field (if parts exist)
    # --------------------------------------------------------------
    df_expr = df_expr.with_column(
        "AUDIT_COMMENTS",
        concat(
            lit("Order Date: "), col("ORDER_DATE"), lit(";"),
            lit("Cancellation Date: "), col("CANCELLATION_DATE"), lit(";")
        )
    )

    # --------------------------------------------------------------
    # STEP 32 – Drop no‑longer‑needed columns
    # --------------------------------------------------------------
    drop_cols = [c for c in ["ORDER_DATE", "CANCELLATION_DATE", "LINE_TOTAL"] if c in df_expr.columns]
    df_expr = df_expr.drop(*drop_cols)

    # --------------------------------------------------------------
    # STEP 33 – Aggregate chargeback per PO & metadata combo
    # --------------------------------------------------------------
    grp_cols = [
        "PO_NUMBER",
        "RULE_NUMBER",
        "AUDIT_TIMESTAMP",
        "SITE",
        "VIOLATE_FLAG",
        "ANCHOR_CONSTANT",
        "AUDIT_COMMENTS",
    ]


    output_df = (
        df_expr.group_by(grp_cols)
        .agg(coalesce(sum_(col("CHARGEBACK_AMOUNT")), lit(0)).alias("CHARGEBACK_AMOUNT"))
    )

    # --------------------------------------------------------------
    # STEP 34 – Replace missing chargeback with 0
    # --------------------------------------------------------------
    output_df = output_df.fillna({"CHARGEBACK_AMOUNT": 0}).replace({float("nan"): 0})

    # --------------------------------------------------------------
    # STEP 35 – Deduplicate
    # --------------------------------------------------------------
    output_df = output_df.drop_duplicates()

    # --------------------------------------------------------------
    # STEP 36 – Final column order (KNIME Column Resorter)
    # --------------------------------------------------------------
    output_df = output_df.select(
        "SITE",
        "AUDIT_TIMESTAMP",
        "PO_NUMBER",
        "RULE_NUMBER",
        "VIOLATE_FLAG",
        "AUDIT_COMMENTS",
        "ANCHOR_CONSTANT",
        "CHARGEBACK_AMOUNT",
    )

    # Sample preview in worksheet
    output_df.show()

    output_df.write.mode("overwrite").save_as_table("MKT_DB.OMNICHANNEL.DMC710_OUT")
    
    return output_df
$$;


-- schedule task
CREATE OR REPLACE TASK run_dmc710_at_0800am
WAREHOUSE = OMS_WH
SCHEDULE = 'USING CRON 00 08 * * 1 America/Chicago'
AS
CALL run_dmc710_chargeback();


--start task
ALTER TASK run_dmc710_at_0800am RESUME;



--debugging

SHOW TASKS LIKE 'run_dmc710_at_0800am';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'RUN_DMC710_AT_0800AM',
    RESULT_LIMIT => 5
))
ORDER BY SCHEDULED_TIME DESC;


--checking output table
select * from MKT_DB.OMNICHANNEL.DMC710_OUT;






