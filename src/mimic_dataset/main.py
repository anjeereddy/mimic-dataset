"""
MIMIC Dataset ETL Pipeline Orchestrator

Main entry point for executing MIMIC dataset ETL pipeline steps:
- BRONZE_LOAD: Ingest raw MIMIC data into Bronze layer
- SILVER_TRANSFORM_LOAD: Transform and load data to Silver layer
- QUALITY_CHECK: Perform data quality validation
- GOLD_TRANSFORM_LOAD: Aggregate and load data to Gold layer
- RAG_PIPELINE: Initialize RAG pipeline for Gen-AI capabilities

Usage:
    python -m mimic_dataset.main '{"STEP": "BRONZE_LOAD"}'
    python -m mimic_dataset.main '{"STEP": "SILVER_TRANSFORM_LOAD"}'
    python -m mimic_dataset.main '{"STEP": "QUALITY_CHECK"}'
    python -m mimic_dataset.main '{"STEP": "GOLD_TRANSFORM_LOAD"}'
    python -m mimic_dataset.main '{"STEP": "RAG_PIPELINE"}'
"""

import sys
import json
import logging
from typing import Dict, Any

from mimic_dataset.bronze.ingest_data_to_bronze import ingest
from mimic_dataset.utils.globals import GlobalVariables as G
from mimic_dataset.utils.file import load_config
from mimic_dataset.silver.transform_and_load_to_silver import execute_silver as silver
from mimic_dataset.gold.load_to_gold import execute_gold as gold
from mimic_dataset.data_quality.quality_check import execute_quality_check as quality_check
from mimic_dataset.gen_ai.rag_pipeline import execute_rag_pipeline as rag_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define valid pipeline steps
VALID_STEPS = {
    "BRONZE_LOAD": ingest,
    "SILVER_TRANSFORM_LOAD": silver,
    "QUALITY_CHECK": quality_check,
    "GOLD_TRANSFORM_LOAD": gold,
    "RAG_PIPELINE": rag_pipeline
}


def validate_parameters(parameters: Dict[str, Any]) -> str:
    """
    Validate input parameters.

    Args:
        parameters (Dict[str, Any]): Input parameters dictionary

    Returns:
        str: Validated STEP parameter

    Raises:
        KeyError: If STEP key is missing
        ValueError: If STEP value is invalid
    """
    if "STEP" not in parameters:
        raise KeyError("Missing required parameter: 'STEP'")

    step = parameters["STEP"]

    if step not in VALID_STEPS:
        raise ValueError(
            f"Invalid STEP: '{step}'. "
            f"Valid steps are: {', '.join(VALID_STEPS.keys())}"
        )

    return step


def main(args: str = None) -> None:
    """
    Orchestrate MIMIC ETL pipeline execution.

    Args:
        args (str, optional): JSON string with execution parameters.
                             If not provided, reads from command-line argument.

    Raises:
        SystemExit: On validation errors or execution failures
    """
    try:
        logger.info("=" * 80)
        logger.info("🚀 MIMIC DATASET ETL PIPELINE ORCHESTRATOR")
        logger.info("=" * 80)

        # Initialize Spark session
        logger.info("📌 Initializing Spark session...")
        G.setup_spark()
        logger.info("✓ Spark session initialized")

        # Load and display configuration
        logger.info("📌 Loading configuration...")
        config, config_json = load_config()
        logger.info(f"✓ Schema: {config.get('schema_name')}")
        logger.info(f"✓ Data location: {config.get('raw_data_location')}")

        # Parse input parameters
        logger.info("📌 Parsing execution parameters...")
        args_json = sys.argv[1] if args is None else args
        logger.debug(f"Raw arguments: {args_json}")

        parameters = json.loads(args_json)
        logger.info(f"✓ Parameters received: {parameters}")

        # Validate and extract step
        step = validate_parameters(parameters)
        logger.info(f"📌 Executing step: {step}")

        # Execute the requested step
        logger.info("📌 Starting pipeline step execution...")
        VALID_STEPS[step]()

        logger.info("=" * 80)
        logger.info("✅ Job Completed Successfully")
        logger.info("=" * 80)

    except KeyError as e:
        logger.error(f"❌ Parameter validation error: {e}")
        logger.error("Expected format: '{\"STEP\": \"BRONZE_LOAD|SILVER_TRANSFORM_LOAD|QUALITY_CHECK|GOLD_TRANSFORM_LOAD|RAG_PIPELINE\"}'")
        sys.exit(1)

    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON parsing error: {e}")
        logger.error("Please provide valid JSON string as argument")
        sys.exit(1)

    except ValueError as e:
        logger.error(f"❌ Invalid parameter value: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
