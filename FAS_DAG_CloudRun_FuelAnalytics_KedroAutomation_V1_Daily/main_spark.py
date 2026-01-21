"""
Main entrypoint for FAS Daily Fuel Analytics (Spark-native)
"""

from pyspark.sql import SparkSession

from config.config import load_paths, load_parameters, get_path
from src.fuel.utils.io import read_dataset, write_dataset

# =========================
# L02
# =========================
from src.fuel.pipeline.l02_intermediate.typecasting_pipeline import (
    run_typecasting_pipeline,
)

# =========================
# L03
# =========================
from src.fuel.pipeline.l03_primary.population_pipeline import run_population_pipeline
from src.fuel.pipeline.l03_primary.refuel_pipeline import run_refuel_pipeline
from src.fuel.pipeline.l03_primary.fleet_pipeline import run_fleet_pipeline
from src.fuel.pipeline.l03_primary.vhms_payloads_pipeline import (
    run_vhms_payloads_pipeline,
)

# =========================
# L04
# =========================
from src.fuel.pipeline.l04_feature.merge_refuel_payloads_pipeline import run_merge_refuel_payload_pipeline
from src.fuel.pipeline.l04_feature.fleet_cycles_pipeline import run_fleet_cycles_pipeline
from src.fuel.pipeline.l04_feature.metrics_calc_pipeline import run_metrics_calc_pipeline
from src.fuel.pipeline.l04_feature.outlie_standar_pipeline import run_outlier_standar_pipeline

#===============================================================#
#Prepare Spark and Connections
#--------------------------------------------------------------#

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("fas-daily-fuel-analytics")
        .getOrCreate()
    )

    # =========================================================
    # Load config
    # =========================================================
    paths = load_paths()
    params = load_parameters()

    l02_params = params["l02"]["fas_daily"]["typecasting_params"]
    l03_params = params["l03"]["fas_daily"]
    l04_params = params["l04"]["fas_daily"]

    # =========================================================
    # L01 – RAW (READ)
    # =========================================================
    raw_fleet = read_dataset(
        spark, get_path(paths, "l01", "fas_daily", "raw_fleet"), "csv"
    )
    raw_populasi = read_dataset(
        spark, get_path(paths, "l01", "fas_daily", "raw_populasi"), "csv"
    )
    raw_refuel = read_dataset(
        spark, get_path(paths, "l01", "fas_daily", "raw_refuel"), "csv"
    )
    raw_vhms = read_dataset(
        spark, get_path(paths, "l01", "fas_daily", "raw_vhms_payloads"), "csv"
    )
    raw_customer_shift = read_dataset(
        spark, get_path(paths, "l01", "fas_daily", "raw_customer_shift"), "csv"
    )

    # =========================================================
    # L02 – INTERMEDIATE (PROCESS → WRITE)
    # =========================================================
    int_fleet = run_typecasting_pipeline(
        raw_fleet, l02_params["raw_fleet"]
    )
    write_dataset(
        int_fleet,
        get_path(paths, "l02", "fas_daily", "int_fleet"),
    )

    int_populasi = run_typecasting_pipeline(
        raw_populasi, l02_params["raw_populasi"]
    )
    write_dataset(
        int_populasi,
        get_path(paths, "l02", "fas_daily", "int_populasi"),
    )

    int_refuel = run_typecasting_pipeline(
        raw_refuel, l02_params["raw_refuel"]
    )
    write_dataset(
        int_refuel,
        get_path(paths, "l02", "fas_daily", "int_refuel"),
    )

    int_vhms = run_typecasting_pipeline(
        raw_vhms, l02_params["raw_vhms_payloads"]
    )
    write_dataset(
        int_vhms,
        get_path(paths, "l02", "fas_daily", "int_vhms_payloads"),
    )

    # =========================================================
    # L03 – PRIMARY (READ → PROCESS → WRITE)
    # =========================================================
    prm_pop = run_population_pipeline(
        int_populasi,
        l03_params["create_populasi_pipeline"],
    )

    write_dataset(
        prm_pop["prm_populasi_dumptruck"],
        get_path(paths, "l03", "fas_daily", "prm_populasi_dumptruck"),
    )
    write_dataset(
        prm_pop["prm_populasi_excavator"],
        get_path(paths, "l03", "fas_daily", "prm_populasi_excavator"),
    )

    prm_refuel = run_refuel_pipeline(
        int_refuel,
        prm_pop["prm_populasi_dumptruck"],
        l03_params["create_refuel_pipeline"],
    )
    write_dataset(
        prm_refuel,
        get_path(paths, "l03", "fas_daily", "prm_refuel"),
    )

    prm_fleet = run_fleet_pipeline(
        int_fleet,
        raw_customer_shift,
        prm_pop["prm_populasi_dumptruck"],
        prm_pop["prm_populasi_excavator"],
        l03_params["create_fleet_pipeline"],
    )
    write_dataset(
        prm_fleet,
        get_path(paths, "l03", "fas_daily", "prm_fleet"),
    )

    prm_payloads = run_vhms_payloads_pipeline(
        int_vhms,
        prm_pop["prm_populasi_dumptruck"],
        raw_customer_shift,
        l03_params["create_payloads_pipeline"],
    )
    write_dataset(
        prm_payloads,
        get_path(paths, "l03", "fas_daily", "prm_vhms_payloads"),
    )

    # =========================================================
    # L04 – FEATURE (READ → PROCESS → WRITE)
    # =========================================================
    feat_merge = run_merge_refuel_payload_pipeline(
        prm_refuel,
        prm_payloads,
        l04_params["create_merged_refuel_payload_pipeline"],
    )
    write_dataset(
        feat_merge,
        get_path(paths, "l04", "fas_daily", "feat_merge_refuel_payload"),
    )

    feat_fleet_cycle, feat_payload_shift = run_fleet_cycles_pipeline(
        feat_merge,
        prm_payloads,
        prm_fleet,
        l04_params["create_fleet_cycles_pipeline"],
    )

    write_dataset(
        feat_fleet_cycle,
        get_path(paths, "l04", "fas_daily", "feat_fleet_cycle"),
    )
    write_dataset(
        feat_payload_shift,
        get_path(paths, "l04", "fas_daily", "feat_payload_shift"),
    )

    metrics = run_metrics_calc_pipeline(
        feat_fleet_cycle,
        feat_payload_shift,
        l04_params["create_metrics_calc_pipeline"],
    )

    reports = run_outlier_standar_pipeline(
        metrics["feat_rr_metrics"],
        metrics["feat_shift_metrics"],
        l04_params["create_outlier_standar_pipeline"],
    )

    # =========================================================
    # L08 – REPORTING (WRITE)
    # =========================================================
    write_dataset(
        reports["rpt_fuel_ratio"],
        get_path(paths, "l08", "fas_daily", "rpt_fuel_ratio_intv"),
    )
    write_dataset(
        reports["rpt_payload"],
        get_path(paths, "l08", "fas_daily", "rpt_payload_intv"),
    )
    write_dataset(
        reports["rpt_refuel_qty"],
        get_path(paths, "l08", "fas_daily", "rpt_refuel_qty_intv"),
    )

    spark.stop()


if __name__ == "__main__":
    main()
