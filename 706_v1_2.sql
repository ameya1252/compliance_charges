# ──────────────────────────────────────────────────────────────────────────────
# DMC-706 COMPLIANCE CHARGEBACK – SNOWPARK IMPLEMENTATION  (SLIMMED VERSION)
#
# Purpose
# -------
# Offshore team now delivers a pre-aggregated table OFFSHORE.BOL_706_FLAGS that
# already contains violation flags.  This script merely:
#   • pulls that data for the required date range
#   • computes the chargeback on flagged rows
#   • standardises the output columns for Finance / downstream jobs
#
# Expected source columns in OFFSHORE.BOL_706_FLAGS  (one row per PRO / PO):
#   TRACKING_NUMBER | PO_NUMBER | TOTAL_CHARGE_AMT | VIOLATE_FLAG | PICKUP_DATE
#
# Output columns (identical to legacy feed):
#   SITE | AUDIT_TIMESTAMP | PO_NUMBER | RULE_NUMBER | VIOLATE_FLAG
#   AUDIT_COMMENTS | CHARGEBACK_AMOUNT | ANCHOR_CONST
#
# Author : Ameya Deshmukh
# Updated: 2025-07-28
# ──────────────────────────────────────────────────────────────────────────────

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import (
    col, lit, abs as snow_abs, current_date, to_char, concat
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATE_FROM     = '2025-03-05'
DATE_TO       = '2025-03-06'
DISCOUNT_PCT  = 0.65           # 65 % chargeback on non-discounted shipments
RULE_NUM      = 'DMC706'
SITE_ID       = 5509
ANCHOR_CONST  = 'x'

# ---------------------------------------------------------------------------
# Main Snowpark entry-point
# ---------------------------------------------------------------------------
def main(session: snowpark.Session):

    # 1. Bring in pre-flagged rows supplied by offshore team
    df = (
        session.table("OMNICHANNEL.BOL_706_OFFSHORE_OUTPUT")
               .filter((col("PICKUP_DATE") >= DATE_FROM) &
                       (col("PICKUP_DATE") <= DATE_TO))
    )

    # 2. Calculate chargeback only where violation already flagged
    df = df.with_column(
            "CHARGEBACK_AMOUNT",
            snow_abs(
                col("TOTAL_CHARGE_AMT") * DISCOUNT_PCT
            )
        )

    # 3. Keep just violating rows & produce final schema
    df_out = (
        df.filter(col("VIOLATE_FLAG") == 'Y')
          .select(
              lit(SITE_ID).alias("SITE"),
              to_char(current_date(), 'YYYYMMDD').alias("AUDIT_TIMESTAMP"),
              col("PO_NUMBER"),
              lit(RULE_NUM).alias("RULE_NUMBER"),
              col("VIOLATE_FLAG"),
              concat(
                  lit("PRO: "), col("TRACKING_NUMBER").cast("string"),
                  lit(" Vendor must include the words Tractor Supply Company in "
                      "front of or in place of their company name on the Ship "
                      "From section of the BOL; failure causes TSC's ship rate "
                      "discount with the carrier to be removed. Please ensure "
                      "that all BOLs contain this verbiage going forward.")
              ).alias("AUDIT_COMMENTS"),
              col("CHARGEBACK_AMOUNT"),
              lit(ANCHOR_CONST).alias("ANCHOR_CONST")
          )
    )

    # Quick sanity peek (optional – remove in prod)
    df_out.show(15)

    # 4. Persist final output for downstream use
    df_out.write.mode("overwrite").save_as_table("OMNICHANNEL.BOL_706_FINAL_OUTPUT")
    
    return df_out

