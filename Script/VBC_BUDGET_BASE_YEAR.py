with PYTHON_WORKSHEET as procedure (p_Year INT, p_Month INT, p_Run_Mode STRING, p_Prcs_Cutoff_Dt STRING) returns Table() language python runtime_version=3.11 packages=('snowflake-snowpark-python') handler='main' as '# Budget Base Year Row Driver

import calendar
import json
from datetime import datetime

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import (
    col,
    lit,
    current_timestamp,
    coalesce,
    round,
    count_distinct,
    row_number,
)
from snowflake.snowpark import Window


def _resolve_run_fields(session: snowpark.Session, p_Year: int, p_Month: int, p_Run_Mode: str, p_Prcs_Cutoff_Dt: str):
    run_dt = datetime.today().date()

    def _month_end(d: datetime.date):
        last_day = calendar.monthrange(d.year, d.month)[1]
        return datetime(d.year, d.month, last_day).date()

    if p_Run_Mode == "M":
        input_dt = datetime.fromisoformat(p_Prcs_Cutoff_Dt).date()
        cutoff_date = input_dt
        performance_year = input_dt.year if input_dt.month >= 5 else input_dt.year - 1
        base_year = None
        last_day_of_input_dt = _month_end(input_dt)
        aud_insrt_id = "VBC_BUDGET_BASE_YEAR"

    elif p_Run_Mode == "H":
        input_dt = datetime(p_Year, p_Month, 1).date()
        cutoff_date = input_dt
        performance_year = p_Year
        base_year = None
        last_day_of_input_dt = _month_end(input_dt)
        aud_insrt_id = "VBC_BUDGET_BASE_YEAR"

    elif p_Run_Mode == "B":
        input_dt = datetime(p_Year, p_Month, 1).date() if p_Month else datetime.fromisoformat(p_Prcs_Cutoff_Dt).date()
        cutoff_date = datetime.fromisoformat(p_Prcs_Cutoff_Dt).date() if p_Prcs_Cutoff_Dt else input_dt
        performance_year = p_Year
        base_year = p_Year
        last_day_of_input_dt = _month_end(cutoff_date)
        aud_insrt_id = "VBC_BUDGET_BASE_YEAR"

    else:
        raise ValueError(f"Unsupported p_Run_Mode: {p_Run_Mode}. Expected M, H, or B.")

    return {
        "run_dt": run_dt,
        "input_dt": input_dt,
        "cutoff_date": cutoff_date,
        "base_year": base_year,
        "performance_year": performance_year,
        "last_day_of_input_dt": last_day_of_input_dt,
        "aud_insrt_id": aud_insrt_id,
    }


def _insert_control_record(session: snowpark.Session, aud_insrt_id: str, input_dt, last_day_of_input_dt):
    session.sql(f"""
        INSERT INTO DATAHUB_ANALYTICS.ETL_OPERNS.ETL_PRCS_CNTL
        (AUD_INSRT_ID, AUD_INSRT_TMSTP, SUBJ_AREA, SRCE_SYS_CD, SRCE_SYS_CD_DESC, ETL_PRCS_NM, ETL_PRCS_FREQ, DATA_PULL_STRT_TMSTP, DATA_PULL_END_TMSTP, ETL_PRCS_STUS_INDC, ETL_PRCS_STRT_TMSTP)
        VALUES
        (''{aud_insrt_id}'', CURRENT_TIMESTAMP(), ''VBC'', ''ALL'', ''ALL'', ''ADF_BUDGET_BASE_YEAR'', ''MONTHLY'', ''{input_dt}'', ''{last_day_of_input_dt}'', ''P'', CURRENT_TIMESTAMP())
    """).collect()


def _complete_control_record(session: snowpark.Session, aud_insrt_id: str, metadata: str):
    prcs_cntl_key = (
        session.table("DATAHUB_ANALYTICS.ETL_OPERNS.ETL_PRCS_CNTL")
        .filter(
            (col("ETL_PRCS_NM") == lit("ADF_BUDGET_BASE_YEAR")) &
            (col("ETL_PRCS_STUS_INDC") == lit("P")) &
            (col("AUD_INSRT_ID") == lit(aud_insrt_id))
        )
        .select("ETL_PRCS_CNTL_SKEY")
        .order_by(col("ETL_PRCS_CNTL_SKEY").desc())
        .limit(1)
        .collect()[0]["ETL_PRCS_CNTL_SKEY"]
    )

    session.sql(f"""
        UPDATE DATAHUB_ANALYTICS.ETL_OPERNS.ETL_PRCS_CNTL
        SET ETL_PRCS_STUS_INDC = ''C'',
            AUD_UPDT_ID = ''{aud_insrt_id}'',
            AUD_UPDT_TMSTP = CURRENT_TIMESTAMP(),
            ETL_PRCS_END_TMSTP = CURRENT_TIMESTAMP(),
            PAYLOAD = ''{metadata}''
        WHERE ETL_PRCS_CNTL_SKEY = {prcs_cntl_key}
    """).collect()


def _build_mode_filter(p_Run_Mode: str, performance_year: int, base_year: int):
    if p_Run_Mode in ("M", "H"):
        return col("PERFORMANCE_YEAR") == lit(performance_year)
    if p_Run_Mode == "B":
        return col("BASE_YEAR") == lit(base_year)
    raise ValueError(f"Unsupported p_Run_Mode: {p_Run_Mode}. Expected M, H, or B.")


def _select_weighted_mode_rows(source, p_Run_Mode: str, performance_year: int, base_year: int):
    mode_filter = _build_mode_filter(p_Run_Mode, performance_year, base_year)
    if p_Run_Mode in ("M", "H"):
        source = source.filter(mode_filter)
    weight_window = Window.partition_by(
        "PROGRAM_IDENTIFIER",
        "PROGRAM_TYPE_CODE",
        "AGE_COHORT_NAME",
        "PRODUCT_NAME",
        "POPULATION_TYPE_NAME",
        "PERFORMANCE_YEAR",
        "MARKET_TREND_ADJUSTMENT_MULTIPLIER",
    )
    # Only B mode needs other performance years before selecting its base year.
    weighted_source = source.with_column(
        "BASE_YEAR_WEIGHT",
        (lit(1.0) / count_distinct(col("BASE_YEAR")).over(weight_window)).cast("NUMBER(3,2)"),
    )
    mode_source = weighted_source.filter(mode_filter) if p_Run_Mode == "B" else weighted_source
    selection_window = Window.partition_by(
        "PROGRAM_IDENTIFIER",
        "PROGRAM_TYPE_CODE",
        "AGE_COHORT_NAME",
        "PRODUCT_NAME",
        "POPULATION_TYPE_NAME",
        "PERFORMANCE_YEAR",
        "BASE_YEAR",
    ).order_by(
        col("CLAIM_CUTOFF_DATE").desc_nulls_last(),
        coalesce(col("MARKET_TREND_ADJUSTMENT_MULTIPLIER") == lit(0), lit(False)).desc(),
        col("MARKET_TREND_ADJUSTMENT_MULTIPLIER").asc_nulls_last(),
        col("VALUE_BASED_CARE_PROGRAM_BUDGET_SKEY").desc(),
    )
    return (
        mode_source.with_column("_SCENARIO_RANK", row_number().over(selection_window))
        .filter(col("_SCENARIO_RANK") == lit(1))
        .drop("_SCENARIO_RANK")
    )


def _carry_source_measures(source):
    return (
        source.with_column("TREND_PERCENT", col("TREND").cast("NUMBER(5,3)"))
        .with_column("AVERAGE_BURDEN_SCORE", round(col("AVERAGE_BURDEN_SCORE"), 2).cast("NUMBER(38,2)"))
        .with_column("BURDEN_SCORE_POINTS", col("BURDEN_SCORE_POINTS").cast("NUMBER(38,2)"))
    )


def main(session: snowpark.Session, p_Year: int, p_Month: int, p_Run_Mode: str, p_Prcs_Cutoff_Dt: str):
    session.use_database("DATAHUB_ANALYTICS")
    session.use_schema("ENTERPRISE_DATA_ASSETS")

    run_info = _resolve_run_fields(session, p_Year, p_Month, p_Run_Mode, p_Prcs_Cutoff_Dt)
    base_year = run_info["base_year"]
    performance_year = run_info["performance_year"]
    cutoff_date = run_info["cutoff_date"]
    aud_insrt_id = run_info["aud_insrt_id"]

    _insert_control_record(session, aud_insrt_id, run_info["input_dt"], run_info["last_day_of_input_dt"])

    base_table = "DATAHUB_ANALYTICS.ENTERPRISE_DATA_ASSETS.VBC_BUDGET_BASE_YEAR"
    source_table = "DATAHUB_ANALYTICS.ENTERPRISE_DATA_ASSETS.VBC_PROGRAM_BUDGET"

    combined_filter = (
        (col("CURRENT_ROW_INDICATOR") == lit("Y")) &
        (col("YEAR_TYPE_NAME") == lit("BASE")) &
        (col("POPULATION_TYPE_NAME") == lit("ATTRIBUTED_MEMBERS")) &
        (col("BASE_YEAR").is_not_null())
    )

    src = (
        session.table(source_table)
        .filter(combined_filter)
        .select(
            col("VALUE_BASED_CARE_PROGRAM_BUDGET_SKEY"),
            col("CLAIM_CUTOFF_DATE"),
            col("PROGRAM_TYPE_CODE"),
            col("POPULATION_TYPE_NAME"),
            col("PROGRAM_IDENTIFIER"),
            col("PROGRAM_NAME"),
            col("AGE_COHORT_NAME"),
            col("PRODUCT_NAME"),
            col("PERFORMANCE_YEAR"),
            col("YEAR_TYPE_NAME"),
            col("BASE_YEAR"),
            col("MARKET_TREND_ADJUSTMENT_MULTIPLIER"),
            col("TOTAL_DEBIT"),
            col("STOP_LOSS_EXCLUSION_DEBIT"),
            col("MEMBER_EXCLUSION_DEBIT"),
            col("CLAIM_EXCLUSION_DEBIT"),
            col("TOTAL_EXCLUSION_DEBIT"),
            col("TOTAL_NET_DEBIT").alias("NET_DEBIT"),
            col("MEMBER_MONTH_COUNT"),
            col("PMPM_NET_DEBIT"),
            col("TREND"),
            col("TOTAL_BURDEN_SCORE").alias("BURDEN_SCORE_POINTS"),
            col("PMPM_BURDEN_SCORE").alias("AVERAGE_BURDEN_SCORE"),
            col("TRENDED_PMPM_NET_DEBIT"),
        )
    )

    src = _select_weighted_mode_rows(src, p_Run_Mode, performance_year, base_year)
    source_row_count = src.count()
    if source_row_count == 0:
        metadata = json.dumps({
            "p_Year": p_Year,
            "p_Month": p_Month,
            "p_Run_Mode": p_Run_Mode,
            "p_Prcs_Cutoff_Dt": p_Prcs_Cutoff_Dt,
            "performance_year": performance_year,
            "base_year": base_year,
            "cutoff_date": str(cutoff_date),
            "source_row_count": 0,
            "action": "no_rows_for_mode_no_delete_insert",
        })
        _complete_control_record(session, aud_insrt_id, metadata)
        return session.create_dataframe([(
            f"VBC_BUDGET_BASE_YEAR row-driver found no rows for mode {p_Run_Mode} / year {performance_year}."
        )], ["message"])

    src = src.with_column("BASE_YEAR_WEIGHT", col("BASE_YEAR_WEIGHT").cast("NUMBER(3,2)"))
    src = _carry_source_measures(src)

    sl_df = session.table("DATAHUB_ANALYTICS.DP_VALUE_BASED_CARE.WORK_BUDGET_MEMBER_STOP_LOSS").alias("sl")
    me_df = session.table("DATAHUB_ANALYTICS.DP_VALUE_BASED_CARE.WORK_BUDGET_MEMBER_EXCLUSION").alias("me")
    ma_df = session.table("DATAHUB_ANALYTICS.ENTERPRISE_DATA_ASSETS.MEMBER_ATTRIBUTION").alias("ma")
    wbm_df = session.table("DATAHUB_ANALYTICS.DP_VALUE_BASED_CARE.WORK_BUDGET_MEMBER").alias("wbm")
    base_group_df = src.select(
        "PROGRAM_IDENTIFIER",
        "PROGRAM_TYPE_CODE",
        "AGE_COHORT_NAME",
        "PRODUCT_NAME",
        "BASE_YEAR",
        "CLAIM_CUTOFF_DATE",
    ).distinct().alias("bg")

    excluded_member_months = (
        sl_df
        .join(
            me_df,
            (
                (sl_df["MEMBER_LIFE_IDENTIFIER"] == me_df["MEMBER_LIFE_IDENTIFIER"]) &
                (sl_df["PROGRAM_IDENTIFIER"] == me_df["PROGRAM_IDENTIFIER"]) &
                (sl_df["AGE_COHORT_NAME"] == me_df["AGE_COHORT_NAME"]) &
                (sl_df["BASE_YEAR"] == me_df["BASE_YEAR"]) &
                (sl_df["CLAIM_CUTOFF_DATE"] == me_df["CLAIM_CUTOFF_DATE"])
            ),
            how="inner",
        )
        .join(
            ma_df,
            (
                (sl_df["MEMBER_LIFE_IDENTIFIER"] == ma_df["MEMBER_LIFE_IDENTIFIER"]) &
                (sl_df["PROGRAM_IDENTIFIER"] == ma_df["PROGRAM_IDENTIFIER"]) &
                (ma_df["RECORD_CURRENT_ROW_INDICATOR"] == lit("Y")) &
                (ma_df["ATTRIBUTION_APPLIED_MONTH"] == sl_df["CLAIM_CUTOFF_DATE"])
            ),
            how="inner",
        )
        .join(
            wbm_df,
            (
                (sl_df["MEMBER_LIFE_IDENTIFIER"] == wbm_df["MEMBER_LIFE_IDENTIFIER"]) &
                (sl_df["PRODUCT_NAME"] == wbm_df["PRODUCT_NAME"]) &
                (sl_df["CLAIM_CUTOFF_DATE"] == wbm_df["CLAIM_CUTOFF_DATE"])
            ),
            how="inner",
        )
        .join(
            base_group_df,
            (
                (sl_df["PROGRAM_IDENTIFIER"] == base_group_df["PROGRAM_IDENTIFIER"]) &
                (ma_df["PROGRAM_TYPE_CODE"] == base_group_df["PROGRAM_TYPE_CODE"]) &
                (sl_df["AGE_COHORT_NAME"] == base_group_df["AGE_COHORT_NAME"]) &
                (sl_df["PRODUCT_NAME"] == base_group_df["PRODUCT_NAME"]) &
                (sl_df["BASE_YEAR"] == base_group_df["BASE_YEAR"]) &
                (sl_df["CLAIM_CUTOFF_DATE"] == base_group_df["CLAIM_CUTOFF_DATE"])
            ),
            how="inner",
        )
        .filter(
            (me_df["YEAR_TYPE_NAME"] == lit("BASE")) &
            (me_df["ATTRIBUTED_MEMBER_INDICATOR"] == lit("Y")) &
            (me_df["RECORD_CURRENT_ROW_INDICATOR"] == lit("Y")) &
            (sl_df["BASE_YEAR"].is_not_null())
        )
        .group_by(
            base_group_df["PROGRAM_IDENTIFIER"],
            base_group_df["PROGRAM_TYPE_CODE"],
            base_group_df["AGE_COHORT_NAME"],
            base_group_df["PRODUCT_NAME"],
            base_group_df["BASE_YEAR"],
            base_group_df["CLAIM_CUTOFF_DATE"],
        )
        .agg(count_distinct(sl_df["MEMBER_LIFE_IDENTIFIER"]).alias("EXCLUDED_MEMBER_MONTH_COUNT"))
        .select(
            base_group_df["PROGRAM_IDENTIFIER"].alias("EXCL_PROGRAM_IDENTIFIER"),
            base_group_df["PROGRAM_TYPE_CODE"].alias("EXCL_PROGRAM_TYPE_CODE"),
            base_group_df["AGE_COHORT_NAME"].alias("EXCL_AGE_COHORT_NAME"),
            base_group_df["PRODUCT_NAME"].alias("EXCL_PRODUCT_NAME"),
            base_group_df["BASE_YEAR"].alias("EXCL_BASE_YEAR"),
            base_group_df["CLAIM_CUTOFF_DATE"].alias("EXCL_CLAIM_CUTOFF_DATE"),
            col("EXCLUDED_MEMBER_MONTH_COUNT"),
        )
    )

    src = src.join(
        excluded_member_months,
        (
            (col("PROGRAM_IDENTIFIER") == col("EXCL_PROGRAM_IDENTIFIER")) &
            (col("PROGRAM_TYPE_CODE") == col("EXCL_PROGRAM_TYPE_CODE")) &
            (col("AGE_COHORT_NAME") == col("EXCL_AGE_COHORT_NAME")) &
            (col("PRODUCT_NAME") == col("EXCL_PRODUCT_NAME")) &
            (col("BASE_YEAR") == col("EXCL_BASE_YEAR")) &
            (col("CLAIM_CUTOFF_DATE") == col("EXCL_CLAIM_CUTOFF_DATE"))
        ),
        how="left",
    ).drop(
        "EXCL_PROGRAM_IDENTIFIER",
        "EXCL_PROGRAM_TYPE_CODE",
        "EXCL_AGE_COHORT_NAME",
        "EXCL_PRODUCT_NAME",
        "EXCL_BASE_YEAR",
        "EXCL_CLAIM_CUTOFF_DATE",
    )

    src = src.with_column("EXCLUDED_MEMBER_MONTH_COUNT", coalesce(col("EXCLUDED_MEMBER_MONTH_COUNT"), lit(0)).cast("NUMBER(38,0)"))

    src = src.with_column("AUDIT_INSERT_IDENTIFIER", lit(aud_insrt_id))
    src = src.with_column("AUDIT_INSERT_TIMESTAMP", current_timestamp())
    src = src.with_column("AUDIT_UPDATE_IDENTIFIER", lit(None).cast("VARCHAR"))
    src = src.with_column("AUDIT_UPDATE_TIMESTAMP", lit(None).cast("TIMESTAMP_TZ"))
    src = src.with_column("AUDIT_SOURCE_ACTION_CODE", lit("I"))

    src.create_or_replace_temp_view("TMP_VBC_BUDGET_BASE_YEAR")

    delete_key = "BASE_YEAR" if p_Run_Mode == "B" else "PERFORMANCE_YEAR"
    delete_sql = f"""
        DELETE FROM {base_table} AS tgt
        WHERE EXISTS (
            SELECT 1
            FROM TMP_VBC_BUDGET_BASE_YEAR src
            WHERE tgt.{delete_key} = src.{delete_key}
        )
    """

    target_row_count = session.table(base_table).count()
    if source_row_count > 0 and target_row_count > 0:
        session.sql(delete_sql).collect()

    session.sql(f"""
        INSERT INTO {base_table} (
            AUDIT_INSERT_IDENTIFIER,
            AUDIT_INSERT_TIMESTAMP,
            AUDIT_UPDATE_IDENTIFIER,
            AUDIT_UPDATE_TIMESTAMP,
            AUDIT_SOURCE_ACTION_CODE,
            PROGRAM_IDENTIFIER,
            PROGRAM_NAME,
            AGE_COHORT_NAME,
            PRODUCT_NAME,
            PERFORMANCE_YEAR,
            BASE_YEAR,
            BASE_YEAR_WEIGHT,
            TOTAL_DEBIT,
            STOP_LOSS_EXCLUSION_DEBIT,
            MEMBER_EXCLUSION_DEBIT,
            CLAIM_EXCLUSION_DEBIT,
            EXCLUDED_MEMBER_MONTH_COUNT,
            TOTAL_EXCLUSION_DEBIT,
            NET_DEBIT,
            MEMBER_MONTH_COUNT,
            PMPM_NET_DEBIT,
            TREND_PERCENT,
            BURDEN_SCORE_POINTS,
            AVERAGE_BURDEN_SCORE,
            TRENDED_PMPM_NET_DEBIT
        )
        SELECT
            AUDIT_INSERT_IDENTIFIER,
            AUDIT_INSERT_TIMESTAMP,
            AUDIT_UPDATE_IDENTIFIER,
            AUDIT_UPDATE_TIMESTAMP,
            AUDIT_SOURCE_ACTION_CODE,
            PROGRAM_IDENTIFIER,
            PROGRAM_NAME,
            AGE_COHORT_NAME,
            PRODUCT_NAME,
            PERFORMANCE_YEAR,
            BASE_YEAR,
            BASE_YEAR_WEIGHT,
            TOTAL_DEBIT,
            STOP_LOSS_EXCLUSION_DEBIT,
            MEMBER_EXCLUSION_DEBIT,
            CLAIM_EXCLUSION_DEBIT,
            EXCLUDED_MEMBER_MONTH_COUNT,
            TOTAL_EXCLUSION_DEBIT,
            NET_DEBIT,
            MEMBER_MONTH_COUNT,
            PMPM_NET_DEBIT,
            TREND_PERCENT,
            BURDEN_SCORE_POINTS,
            AVERAGE_BURDEN_SCORE,
            TRENDED_PMPM_NET_DEBIT
        FROM TMP_VBC_BUDGET_BASE_YEAR
    """).collect()

    inserted_rows = src.count()
    metadata = json.dumps({
        "p_Year": p_Year,
        "p_Month": p_Month,
        "p_Run_Mode": p_Run_Mode,
        "p_Prcs_Cutoff_Dt": p_Prcs_Cutoff_Dt,
        "base_year": base_year,
        "performance_year": performance_year,
        "cutoff_date": str(cutoff_date),
        "source_row_count": source_row_count,
        "rows_inserted": inserted_rows,
        "row_publish_mode": "row_by_row_source_publish",
        "action": "deleted_and_reinserted_rows_for_mode_key",
    })

    _complete_control_record(session, aud_insrt_id, metadata)

    return session.create_dataframe([(
        f"VBC_BUDGET_BASE_YEAR row-driver refreshed for mode {p_Run_Mode}. Rows inserted: {inserted_rows}."
    )], ["message"])
' call PYTHON_WORKSHEET(@{pipeline().parameters.p_Year}, @{pipeline().parameters.p_Month}, @{pipeline().parameters.p_Run_Mode}, '@{pipeline().parameters.p_Prcs_Cutoff_Dt}');
