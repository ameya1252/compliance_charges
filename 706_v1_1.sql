# ──────────────────────────────────────────────────────────────────────────────
# DMC-706 - OFFSHORE INPUT BUILDER  (Version 1)
#
# Purpose
# -------
# Creates a clean, pre-aggregated table (OMNICHANNEL.BOL_706_INPUT) that the
# offshore team will use to apply their violation flags.  No chargeback or flag
# logic is performed here.
#
# Output columns written to OMNICHANNEL.BOL_706_INPUT:
#   TRACKING_NUMBER | PO_NUMBER | SHIPPER_NAME | PICKUP_DATE
#   GROSS_CHARGE_AMT | FUEL_SURCHARGE_AMT | ACCESSORIAL_AMT | DISC_AMT
#   TOTAL_CHARGE_AMT
#
# Author : Ameya Deshmukh
# Created: 2025-07-28
# ──────────────────────────────────────────────────────────────────────────────

import snowflake.snowpark as snowpark

# ---------------------------------------------------------------------------
# Configuration – adjust the window as needed
# ---------------------------------------------------------------------------
DATE_FROM = '2025-03-05'
DATE_TO   = '2025-03-06'

# ---------------------------------------------------------------------------
# Main Snowpark entry-point
# ---------------------------------------------------------------------------
def main(session: snowpark.Session):

    # 1. Build the offshore input table (CREATE OR REPLACE)
    session.sql(f"""
        CREATE OR REPLACE TABLE OMNICHANNEL.BOL_706_OFFSHORE_INPUT AS
        SELECT
              y.PRO_NO                                      AS TRACKING_NUMBER
            , TRIM(UPPER(MAX(d.LTL_PO_NO)))                 AS PO_NUMBER
            , MAX(d.SHIPPER_NAME)                           AS SHIPPER_NAME
            , MAX(d.PICKUP_DATE)                            AS PICKUP_DATE
            , SUM(y.GROSS_CHARGE_AMT)                       AS GROSS_CHARGE_AMT
            , SUM(y.FUEL_SURCHARGE_AMT)                     AS FUEL_SURCHARGE_AMT
            , SUM(y.ACCESSORIAL_AMT)                        AS ACCESSORIAL_AMT
            , SUM(y.DISC_AMT)                               AS DISC_AMT
            , SUM(y.GROSS_CHARGE_AMT
                 + y.FUEL_SURCHARGE_AMT
                 + y.ACCESSORIAL_AMT)                       AS TOTAL_CHARGE_AMT
        FROM  EDW.VW_SHIPMENT_LTL_CONTAINER_YW y
        JOIN  EDW.VW_SHIPMENT_LTL_CONTAINER    d
              ON d.PRO_NO = y.PRO_NO
        WHERE d.PICKUP_DATE BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}'
        GROUP BY y.PRO_NO
    """).collect()

    # 2. Return a DataFrame reference (and optional preview)
    df = session.table("OMNICHANNEL.BOL_706_OFFSHORE_INPUT")
    df.show(15)       # ← comment out in production

    return df
