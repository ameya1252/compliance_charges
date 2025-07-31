/*======================================================================
  PROCEDURE  : build_combined_chargebacks
  PURPOSE    : Merge any set of chargeback‑rule output tables
               (DMC<rule>_OUT) the same way KNIME’s Concatenate node does:
                  • union columns
                  • reuse RowID
                  • append "_dup" on duplicates
  PARAMS     : charge_list_str – comma‑separated list, e.g. '702,..,710,..'
  RETURNS    : STRING – status summary
  AUTHOR     : Ameya Deshmukh • Jul 30 2025
======================================================================*/
CREATE OR REPLACE PROCEDURE build_combined_chargebacks(charge_list_str STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.window import Window

CATALOG = "MKT_DB"
SCHEMA  = "OMNICHANNEL"
OUT_TBL = f"{CATALOG}.{SCHEMA}.DMC_CHARGEBACK_COMBINED"

# ── helper to check rule table exists ────────────────────────────
def _rule_table(session, rule_num):
    tname = f"DMC{rule_num}_OUT"
    exists = session.sql(f"""
        SELECT 1 FROM {CATALOG}.INFORMATION_SCHEMA.TABLES
         WHERE TABLE_CATALOG = '{CATALOG}'
           AND TABLE_SCHEMA  = '{SCHEMA}'
           AND UPPER(TABLE_NAME) = UPPER('{tname}')
    """).count() > 0
    return (f"{CATALOG}.{SCHEMA}.{tname}", exists)

def main(session: snowpark.Session, charge_list_str: str):
    rules = [r.strip() for r in charge_list_str.split(",") if r.strip()]
    if not rules:
        raise ValueError("charge_list_str cannot be empty.")

    dfs, missing = [], []
    for r in rules:
        tbl, ok = _rule_table(session, r)
        if ok:
            dfs.append(session.table(tbl))
        else:
            missing.append(r)
    if not dfs:
        raise RuntimeError(f"No rule tables found ({', '.join(missing)})")

    # ── Build superset of columns & align each DF ────────────────
    all_cols = sorted({c for df in dfs for c in df.columns})

    def _align(df):
        for col_name in all_cols:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(None))
        return df.select(*all_cols)

    df = _align(dfs[0])
    for other in dfs[1:]:
        df = df.union(_align(other))

    # ── Choose / create RowID ─────────────────────────────────────
    if "ROW_ID" in df.columns:
        id_col = "ROW_ID"
    elif "PO_NUMBER" in df.columns:
        id_col = "PO_NUMBER"
    else:
        df = df.withColumn("ROW_ID",
                           F.monotonically_increasing_id().cast("string"))
        id_col = "ROW_ID"

    # ── Append “_dup” for duplicate RowIDs ───────────────────────
    w = Window.partitionBy(id_col).orderBy(F.lit(0))
    df = (df.withColumn("_r", F.row_number().over(w))
              .withColumn(id_col,
                          F.when(F.col("_r") == 1, F.col(id_col))
                           .otherwise(F.concat(F.col(id_col), F.lit("_dup"))))
              .drop("_r"))

    # ── Persist combined table ───────────────────────────────────
    df.write.mode("overwrite").saveAsTable(OUT_TBL)

    merged = [r for r in rules if r not in missing]
    return (f"Combined rules: {', '.join(merged)} -> {OUT_TBL} refreshed."
            + (f"  Skipped: {', '.join(missing)}" if missing else ""))
$$;



/*──────────────────────────────────────────────────────────────────
  TASK : combine_chargebacks_monday_0900
  Runs : build_combined_chargebacks('702,710')
  When : 09:00 every Monday (America/Chicago)
──────────────────────────────────────────────────────────────────*/
CREATE OR REPLACE TASK MKT_DB.OMNICHANNEL.combine_chargebacks_monday_0900
  WAREHOUSE = OMS_WH
  SCHEDULE  = 'USING CRON 00 09 * * 1 America/Chicago'
AS
  -- ► Update the list below whenever you add/remove rule tables
  CALL MKT_DB.OMNICHANNEL.build_combined_chargebacks('702,710');



-- Start the task immediately
ALTER TASK MKT_DB.OMNICHANNEL.combine_chargebacks_monday_0900 RESUME;



--debugging

SHOW TASKS LIKE 'combine_chargebacks_monday_0900';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'combine_chargebacks_monday_0900',
    RESULT_LIMIT => 5
))
ORDER BY SCHEDULED_TIME DESC;


--checking output table
select * from MKT_DB.OMNICHANNEL.DMC_CHARGEBACK_COMBINED;




