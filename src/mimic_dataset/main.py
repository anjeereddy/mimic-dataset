import sys
import json

from mimic_dataset.bronze.ingest_data_to_bronze import ingest
from mimic_dataset.utils.globals import GlobalVariables as G
from mimic_dataset.silver.transform_and_load_to_silver import execute_silver as silver
from mimic_dataset.gold.load_to_gold import execute_gold as gold
from mimic_dataset.data_quality.quality_check import execute_quality_check as quality_check
from mimic_dataset.gen_ai.rag_pipeline import execute_rag_pipeline as rag_pipeline

def main(args=None):

    G.setup_spark()

    args_json = sys.argv[1] if not args else args
    print(args_json)

    parameters = json.loads(args_json)
    print(parameters)

    step = parameters["STEP"]
    print(step)
    if step == "BRONZE_LOAD":
        ingest()
    elif step == "SILVER_TRANSFORM_LOAD":
        silver()
    elif step == "QUALITY_CHECK":
        quality_check()
    elif step == "GOLD_TRANSFORM_LOAD":
        gold()
    elif step == "RAG_PIPELINE":
        rag_pipeline()
    else:
        print(f"Invalid STEP parameter: {step}")
        sys.exit(1)
    print("✅ Job Completed Successfully")


if __name__ == "__main__":
    main()
