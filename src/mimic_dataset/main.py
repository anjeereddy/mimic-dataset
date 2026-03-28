import sys
import json

from mimic_dataset.bronze.ingest_data_to_bronze import ingest
from mimic_dataset.utils.globals import GlobalVariables as G
from mimic_dataset.silver.transform_and_load_to_silver import execute_silver as silver

def main(args=None):

    G.setup_spark()

    print("Hello")
    args_json = sys.argv[1] if not args else args
    print(args_json)

    parameters = json.loads(args_json)
    print(parameters)

    step = parameters["STEP"]
    print(step)
    if step == "BRONZE_LOAD":
        ingest()
    elif step == "SILVER_TRASNFORM_LOAD":
        silver()
    print("✅ Job Completed Successfully")


