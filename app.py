import os
import yaml
from src import parse_excel, enrich_rules , generate_test_cases , add_keys

def load_config(config_path="config/settings.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        return None


def main():
    """Main function to orchestrate the test automation process."""
    config = load_config()
    if config is None:
        exit()

    # # 1. Parse Excel and Extract Rules
    rules = parse_excel.parse_excel(config)
    if rules:
        parse_excel.save_rules(rules, config.get("processed_rules_file"))
    else:
        print("Error: Failed to parse Excel and extract rules.")
        return

    # 2. Enrich Rules with Constraints
    # enrich_rules.enrich_rules(config)

    # # 3. Generate Test Cases
    generate_test_cases.main()

    # 4. Add Unique Keys
    add_keys.add_unique_keys(config["generated_test_cases_file"], config["test_case_keys_file"])

if __name__ == "__main__":
    main()
