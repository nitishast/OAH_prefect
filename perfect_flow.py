import prefect
from prefect import flow, task
from prefect.context import get_run_context

from prefect.artifacts import create_link_artifact, create_table_artifact
from src import parse_excel, enrich_rules, generate_test_cases, add_keys  # Adjust import paths
import logging
import yaml
import os
import json
import pandas as pd

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]  # Log to console
)

def load_config(config_path="config/settings.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Config file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file: {e}")
        return None

def validate_data(rules):
    """
    Performs basic validation on the extracted rules.
    This example checks for missing fields, but you can add more sophisticated checks.
    """
    if not rules:
        raise ValueError("No rules to validate.")

    for parent_field, details in rules.items():
        if "fields" not in details:
            raise ValueError(f"Missing 'fields' in {parent_field}")
        for field_name, field_details in details["fields"].items():
            if not all(key in field_details for key in ["data_type", "mandatory_field", "from_source", "primary_key", "required_for_deployment", "deployment_validation", "business_rules"]):
                raise ValueError(f"Missing required keys in field {field_name} of {parent_field}")

@task(name="Parse Excel and Extract Rules", retries=3, retry_delay_seconds=60)
def parse_excel_task(config):
    """Parses the Excel file and extracts rules."""
    try:
        rules = parse_excel.parse_excel(config)
        if rules:
            return rules
        else:
            logging.error("Failed to parse Excel and extract rules.")
            raise ValueError("Failed to parse Excel and extract rules.")
    except Exception as e:
        logging.error(f"Error parsing Excel: {e}")
        raise  # Re-raise to trigger retries

@task(name="Validate Parsed Rules", retries=1)
def validate_parsed_rules_task(rules):
    """Validates the parsed rules."""
    try:
        validate_data(rules)
        return True
    except ValueError as e:
        logging.error(f"Validation failed: {e}")
        raise

@task(name="Save Parsed Rules", retries=1)  # Simple save, no retries needed
def save_parsed_rules(rules, config):
    """Saves the parsed rules to a file and creates a Prefect artifact."""
    try:
        filepath = config.get("processed_rules_file")
        parse_excel.save_rules(rules, filepath)

        # Create a link artifact to the saved file: this is one of the easy implementation of artifacts, just link to the local files.
        run_context = get_run_context() #added run context to get a more precise tracking
        
        if run_context:
            create_link_artifact(
                key="parsed_rules_file",
                name="Parsed Rules File",
                description="Link to the saved JSON file containing the parsed rules.",
                target=os.path.abspath(filepath),  # Use absolute path
            )
        else:
            logging.warn("Skip creating the link as not running in prefect context")

        return True
    except Exception as e:
        logging.error(f"Error saving parsed rules: {e}")
        return False


@task(name="Enrich Rules with Constraints", retries=3, retry_delay_seconds=60)
def enrich_rules_task(config, rules):
    """Enriches rules with constraints using LLM."""
    try:
        config["processed_rules_file"] = "data/temp_rules.json"
        output_file = config.get("constrains_processed_rules_file")
        with open(output_file, "w") as f:
            json.dump(rules, f, indent=4) # saving this as json file
        from src import enrich_rules # Importing here to make it accessible and update
        enrich_rules.enrich_rules(config)

        run_context = get_run_context() #added run context to get a more precise tracking
        
        if run_context:
            # Create a link artifact to the saved file: this is one of the easy implementation of artifacts, just link to the local files.
            create_link_artifact(
                key="enriched_rules_file",
                name="Enriched Rules File",
                description="Link to the saved JSON file containing the enriched rules.",
                target=os.path.abspath(config["constrains_processed_rules_file"]),  # Use absolute path
            )
        else:
            logging.warn("Skip creating the link as not running in prefect context")
        return True  # Indicate success
    except Exception as e:
        logging.error(f"Error enriching rules: {e}")
        raise

@task(name="Generate Test Cases", retries=3, retry_delay_seconds=60)
def generate_test_cases_task(config):
    """Generates test cases and creates an artifact."""
    try:
        from src import llm # Import llm inside to make it accessible
        llm_client = llm.initialize_llm(config) #load LLM config here

        from src import generate_test_cases
        generate_test_cases.main(config, llm_client=llm_client) #call main with llm_client
        run_context = get_run_context() #added run context to get a more precise tracking
        
        if run_context:
            # Create a link artifact to the saved file: this is one of the easy implementation of artifacts, just link to the local files.
            create_link_artifact(
                key="generated_test_cases_file",
                name="Generated Test Cases File",
                description="Link to the saved JSON file containing the generated test cases.",
                target=os.path.abspath(config["generated_test_cases_file"]),  # Use absolute path
            )
        else:
            logging.warn("Skip creating the link as not running in prefect context")
        return True
    except Exception as e:
        logging.error(f"Error generating test cases: {e}")
        raise

@task(name="Add Unique Keys", retries=3, retry_delay_seconds=60)
def add_keys_task(config):
    """Adds unique keys to the test cases and creates a link artifact."""
    try:
        from src import add_keys # Import add keys inside to make it accessible
        add_keys.main(config) #call main with the config file now 

        run_context = get_run_context() #added run context to get a more precise tracking
        if run_context:
            # Create a link artifact to the saved file: this is one of the easy implementation of artifacts, just link to the local files.
            create_link_artifact(
                key="test_case_keys_file",
                name="Test Case Keys File",
                description="Link to the saved JSON file containing the test cases with unique keys.",
                target=os.path.abspath(config["test_case_keys_file"]),  # Use absolute path
            )
        else:
            logging.warn("Skip creating the link as not running in prefect context")

        return True
    except Exception as e:
        logging.error(f"Error adding unique keys: {e}")
        raise

def load_rules(config):
    rules_file = config.get("processed_rules_file")
    try:
        with open(rules_file, "r") as f:
            rules_ = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load rules: {str(e)}")
        raise
    return rules_

@flow(name="Test Automation Workflow")
def test_automation_flow(config_path="config/settings.yaml", enrich=True):
    """Orchestrates the test automation process."""
    config = load_config(config_path)
    if not config:
        return

    # First delete generated_test_cases_file and test_case_keys_file if exists, by checking if the file exists and delete. 
    if os.path.exists(config["generated_test_cases_file"]):
        os.remove(config["generated_test_cases_file"])

    if os.path.exists(config["test_case_keys_file"]):
        os.remove(config["test_case_keys_file"])

    rules = parse_excel_task(config)
    if not rules:
        return
    
    validate_result = validate_parsed_rules_task(rules)

    # Now save rules
    save_task = save_parsed_rules(rules,config)

    #Load Rules for `enrich_rules_task`
    rules_new = load_rules(config)
    #load the rules

    enrichment_result = True # added this line for the test to skip
    if enrich: # added to not execute if it is not specified
        #Load Rules for `enrich_rules_task`
        # rules_new = load_rules(config) This load rule is not required as already present, this also created the issue as rule is loaded but not passed

        enrichment_result = enrich_rules_task.submit(config,rules_new) # changing it form .call() to .submit()
        

    if not enrichment_result:
        print("Skipping enrichments")

    generation_result = generate_test_cases_task(config)
    if not generation_result:
        return

    add_keys_task(config)

if __name__ == "__main__":
    test_automation_flow(enrich=False)