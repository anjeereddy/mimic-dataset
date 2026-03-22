import sys
import json


def main(args=None):
    args_json = sys.argv[1] if not args else args
    print(args_json)

    parameters = json.loads(args_json)
    print(parameters)