import os
import json
import uuid
import logging
import yaml

def setup_logging(log_dir="logs", log_file="add_keys.log"):
    """Sets up logging configuration."""
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )

def load_config(config_path="config/settings.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Config file not found at {config_path}")
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file: {e}")
    return None

def add_unique_keys(input_file: str, output_file: str):
    """Read test cases, add unique keys, and save to a new file."""
    try:
        # Load JSON file
        with open(input_file, "r") as f:
            test_cases = json.load(f)
        
        # Process each field and test case
        for field_name, cases in test_cases.items():
            for case in cases:
                case["key"] = str(uuid.uuid4())  # Assign a unique UUID
        
        # Save updated test cases with a backup mechanism
        if os.path.exists(output_file):
            backup_file = f"{output_file}.{uuid.uuid4().hex}.bak"
            os.rename(output_file, backup_file)
            logging.info(f"Backup created: {backup_file}")
        
        with open(output_file, "w") as f:
            json.dump(test_cases, f, indent=2)
        
        logging.info(f"Successfully saved updated test cases to {output_file}")
    except Exception as e:
        logging.error(f"Error processing test cases: {str(e)}")
        raise

def main():
    setup_logging()
    config = load_config()
    if not config:
        exit()
    
    input_file = config.get("generated_test_cases_file", "data/generated_test_cases.json")
    output_file = config.get("test_case_keys_file", "data/test_case_with_keys.json")
    
    add_unique_keys(input_file, output_file)

if __name__ == "__main__":
    main()
