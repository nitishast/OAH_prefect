import json
import os
import google.generativeai as genai  # If using Gemini
import pandas as pd
import yaml
import logging # Import logging

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

def enrich_constraints(field_name, data_type, business_rules, llm_client, llm_model, max_output_tokens=200):
    """Enriches the rules with more details constraints."""
    prompt = f"""
    Based on the field '{field_name}' of type '{data_type}', and the business rules '{business_rules}', extract the constraints of the following rules, for each column. 
    List the constraints only (separated by commas), and use a comma to separate if more than one value is provided. Do not include a header or any information about the column name.
    For example, writing constraints for First Name:
    Constraints: "Mandatory, No Special Characters, Only Alphabets"
    """
    try:
        if isinstance(llm_client, genai.GenerativeModel):
            response = llm_client.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_output_tokens
                )
            )
            extracted_constraints = response.text

        else:
            raise ValueError("Unsupported LLM client type.")

        return extracted_constraints

    except Exception as e:
        logging.error(f"Error generating test cases: {e}")
        return None

def clean_and_split_constraints(constraints_string):
    """Clean and split the comma-separated constraints."""
    if not constraints_string:
        return []

    # Remove leading/trailing whitespace and backticks, then split by comma
    cleaned_constraints = constraints_string.strip().replace('```', '').strip()
    constraints_list = [c.strip() for c in cleaned_constraints.split(',')]

    # Remove empty constraints (if any)
    constraints_list = [c for c in constraints_list if c]

    return constraints_list

def enrich_rules(config):
    """Enriches the rules with constraints."""
    # Load LLM
    if config.get("gemini_api_key"):
        genai.configure(api_key=config["gemini_api_key"])
        llm_model = config.get("gemini_model", "gemini-1.5-flash")  # Or use a different model name
        llm_client = genai.GenerativeModel(llm_model)

    else:
        print("Error: No LLM API key found in config.yaml. Please configure either Gemini or OpenAI.")
        return

    # Load processed rules
    rules_file = config.get("processed_rules_file")
    if not rules_file:
        print("Error: processed_rules_file not found in config.")
        return

    try:
        with open(rules_file, "r") as f:
            rules = json.load(f)
    except FileNotFoundError:
        print(f"Error: Rules file not found at {rules_file}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {rules_file}: {e}")
        return

    enriched_rules = {}
    for parent_field, details in rules.items():
        enriched_rules[parent_field] = {"fields": {}}  # Removed description
        for field_name, field_details in details["fields"].items():
            data_type = field_details["data_type"]
            business_rules = field_details["business_rules"]  # Added to get business rules

            extracted_constraints = enrich_constraints(field_name, data_type, business_rules, llm_client, llm_model)
            constraints_list = clean_and_split_constraints(extracted_constraints)

            enriched_rules[parent_field]["fields"][field_name] = {
                "data_type": data_type,
                "mandatory_field": field_details["mandatory_field"],
                "from_source": field_details["from_source"],  # Added to include from_source
                "primary_key": field_details["primary_key"],  # Added to include primary_key
                "required_for_deployment": field_details["required_for_deployment"],  # Added to include required_for_deployment
                "deployment_validation": field_details["deployment_validation"],  # Added to include deployment_validation
                "business_rules": business_rules,  # Added to include business_rules
                "constraints": constraints_list  # add constraints as a List
            }

    # Save the enreiched constrains
    output_file = config.get("constrains_processed_rules_file")
    try:
        with open(output_file, "w") as f:
            json.dump(enriched_rules, f, indent=4)
        print(f"✅ Rules extracted and saved to {output_file}")
    except IOError as e:
        print(f"Error saving rules to {output_file}: {e}")

# def main():
#     config = load_config()
#     if config is None:
#         exit()

#     enrich_rules(config)

# if __name__ == "__main__":
#     config = load_config()
#     if config is None:
#         exit()

#     enrich_rules(config)