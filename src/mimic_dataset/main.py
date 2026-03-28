import sys
import json

from mimic_dataset.bronze.ingest_data_to_bronze import ingest
from mimic_dataset.utils.globals import GlobalVariables as G

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

    print("✅ Job Completed Successfully")


