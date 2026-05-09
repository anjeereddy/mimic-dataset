from pyspark.sql import functions as F
from pyspark.sql.window import Window
from mimic_dataset.utils.globals import GlobalVariables as G
from mimic_dataset.utils.file import load_config

def execute_gold():
    config, _ = load_config()
    schema_name = config['schema_name']

    spark = G.spark

    # ICU visits per patient
    icu_visits_per_patient_df = (
        spark.table(f"{schema_name}.silver.fact_icustays")
            .groupBy("subject_id")
            .count()
            .withColumnRenamed("count", "icu_visits")
    )
    icu_visits_per_patient_df.write.mode("overwrite").saveAsTable(f"{schema_name}.gold.icu_visits_per_patient")

    # Readmissions by insurance
    readmissions_by_insurance_df = (
        spark.table(f"{schema_name}.silver.fact_admissions_enriched")
            .groupBy("insurance", "admission_type")
            .count()
            .withColumnRenamed("count", "total_admissions")
    )
    readmissions_by_insurance_df.write.mode("overwrite").saveAsTable(f"{schema_name}.gold.readmissions_by_insurance")

    # ICU visit distribution


    icu_visits_dist_df = (
        icu_visits_per_patient_df
            .groupBy("icu_visits")
            .agg(F.count("*").alias("patient_count"))
            .withColumn(
            "percentage",
            F.format_number(
                F.col("patient_count") * 100.0 / F.sum("patient_count").over(Window.partitionBy()),
                2
            ).cast("decimal(27,2)")
        )
    )
    icu_visits_dist_df.write.mode("overwrite").saveAsTable(f"{schema_name}.gold.icu_visit_distribution")

    # ICU admissions trend
    icu_admissions_trend_df = (
        spark.table(f"{schema_name}.silver.fact_icustays")
            .withColumn("admission_year", F.year("intime"))
            .groupBy("admission_year")
            .count()
            .withColumnRenamed("count", "total_admissions")
            .orderBy("admission_year")
    )
    icu_admissions_trend_df.write.mode("overwrite").saveAsTable(f"{schema_name}.gold.icu_admissions_trend")
