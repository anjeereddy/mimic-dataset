import yaml
import json
from importlib import resources


def load_config(env):
    # Path inside package
    config_path = f"resources/configs/{env}/config.yaml"

    # Read file using importlib (works inside .whl)
    with resources.files("mimic_dataset").joinpath(config_path).open("r") as f:
        data = yaml.safe_load(f)

    # Convert to JSON string (optional)
    json_data = json.dumps(data, indent=4)

    return data, json_data


if __name__ == "__main__":
    config_dict, config_json = load_config()

    print("DICT:", config_dict)
    print("JSON:", config_json)