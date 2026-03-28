from mimic_dataset.utils.spark_session import get_spark

def ingest():

    schema_name = 'mimic_catalog'
    raw_data_location = 'abfss://df-mimic-data@azdatafactorydevadls.dfs.core.windows.net/raw'
    spark = _spark

    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.bronze
            """.format(schema_name = schema_name))
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.silver
            """.format(schema_name = schema_name))
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.gold
            """.format(schema_name = schema_name))

    # Drop table if exists
    spark.sql("""
              DROP TABLE IF EXISTS {schema_name}.bronze.raw_data
              """.format(schema_name = schema_name))

    # Create table using Delta format and external location
    spark.sql("""
    CREATE TABLE {schema_name}.bronze.raw_data
    USING DELTA
    LOCATION 'abfss://df-mimic-data@azdatafactorydevadls.dfs.core.windows.net/bronze_data'
    """.format(schema_name = schema_name))

    files = [
        "ADMISSIONS.csv", "ICUSTAYS.csv", "PATIENTS.csv", "LABEVENTS.csv",
        "CALLOUT.csv", "CAREGIVERS.csv", "CHARTEVENTS.csv", "CPTEVENTS.csv",
        "DATETIMEEVENTS.csv", "DIAGNOSES_ICD.csv", "DRGCODES.csv",
        "D_CPT.csv", "D_ICD_DIAGNOSES.csv", "D_ICD_PROCEDURES.csv",
        "D_ITEMS.csv", "D_LABITEMS.csv", "INPUTEVENTS_CV.csv",
        "INPUTEVENTS_MV.csv", "MICROBIOLOGYEVENTS.csv", "NOTEEVENTS.csv",
        "OUTPUTEVENTS.csv", "PRESCRIPTIONS.csv", "PROCEDUREEVENTS_MV.csv",
        "PROCEDURES_ICD.csv", "SERVICES.csv", "TRANSFERS.csv"
    ]

    for f in files:
        table_name = f.replace(".csv", "").lower() + "_raw"

        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{raw_data_location}/{f}"))

        (df.write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(f"{schema_name}.bronze.{table_name}"))

        print(f"✅ Ingested: {f} -> bd_mimic.bronze.{table_name}")
