import pandas as pd
import json
import yaml

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


def preprocess_excel(file_path, sheet_name):
    """Preprocess the Excel file."""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Rename columns to remove leading/trailing spaces
        df.columns = df.columns.str.strip()

        # Detect column indices dynamically
        rx_bc_col = None
        attribute_details_col = None
        data_type_col = None
        business_rules_col = None
        mandatory_field_col = None
        from_source_col = None
        primary_key_col = None
        required_for_deployment_col = None
        deployment_validation_col = None

        for i, col in enumerate(df.columns):
            col_lower = col.lower()
            if "schema name" in col_lower:
                rx_bc_col = i
            elif "attributes details" in col_lower:
                attribute_details_col = i
            elif "data type" in col_lower:
                data_type_col = i
            elif "business rules" in col_lower:
                business_rules_col = i
            elif "mandatory field" in col_lower:
                mandatory_field_col = i
            elif "required from source to have data populated" in col_lower:
                from_source_col = i
            elif "primary key" in col_lower:
                primary_key_col = i
            elif "required for deployment validation" in col_lower:
                required_for_deployment_col = i
            elif "deployment validation" in col_lower:
                deployment_validation_col = i

        if rx_bc_col is None or attribute_details_col is None or data_type_col is None or business_rules_col is None or mandatory_field_col is None or from_source_col is None or primary_key_col is None or required_for_deployment_col is None or deployment_validation_col is None:
            raise ValueError("Could not automatically detect required columns.  "
                             "Please ensure all required columns exist in the Excel sheet.")

        # Step 1: Fill down the "Schema Name" category
        df.iloc[:, rx_bc_col] = df.iloc[:, rx_bc_col].ffill()

        return df

    except Exception as e:
        print(f"Error preprocessing Excel file: {e}")
        return None

def extract_rules_from_dataframe(df):
    """Extract rules from the cleaned dataframe."""
    extracted_rules = {}
    rx_bc_col = None
    attribute_details_col = None
    data_type_col = None
    business_rules_col = None
    mandatory_field_col = None
    from_source_col = None
    primary_key_col = None
    required_for_deployment_col = None
    deployment_validation_col = None

    for i, col in enumerate(df.columns):
        col_lower = col.lower()
        if "schema name" in col_lower:
            rx_bc_col = i
        elif "attributes details" in col_lower:
            attribute_details_col = i
        elif "data type" in col_lower:
            data_type_col = i
        elif "business rules" in col_lower:
            business_rules_col = i
        elif "mandatory field" in col_lower:
            mandatory_field_col = i
        elif "required from source to have data populated" in col_lower:
            from_source_col = i
        elif "primary key" in col_lower:
            primary_key_col = i
        elif "required for deployment validation" in col_lower:
            required_for_deployment_col = i
        elif "deployment validation" in col_lower:
            deployment_validation_col = i
    try:
        extracted_rules = {}
        for _, row in df.iterrows():
            parent_field = str(row.iloc[rx_bc_col]).strip()
            field_name = str(row.iloc[attribute_details_col]).strip()
            data_type = str(row.iloc[data_type_col]).strip() if pd.notna(row.iloc[data_type_col]) else "String"
            business_rules = str(row.iloc[business_rules_col]).strip() if pd.notna(row.iloc[business_rules_col]) else ""
            mandatory_field = str(row.iloc[mandatory_field_col]).strip().lower() == "yes"
            from_source = str(row.iloc[from_source_col]).strip().lower() == "yes"
            primary_key = str(row.iloc[primary_key_col]).strip().lower() == "yes"
            required_for_deployment = str(row.iloc[required_for_deployment_col]).strip().lower() == "yes"
            deployment_validation = str(row.iloc[deployment_validation_col]).strip().lower() == "yes"

            if parent_field not in extracted_rules:
                extracted_rules[parent_field] = {"fields": {}}

            extracted_rules[parent_field]["fields"][field_name] = {
                "data_type": data_type,
                "mandatory_field": mandatory_field,
                "from_source": from_source,
                "primary_key": primary_key,
                "required_for_deployment": required_for_deployment,
                "deployment_validation": deployment_validation,
                "business_rules": business_rules
            }
        return extracted_rules
    except Exception as e:
        print(f"Error extracting rules: {e}")
        return {}

def parse_excel(config):
    """Parses the Excel file and extracts the rules."""
    excel_file = config.get("excel_file")
    excel_sheet_name = config.get("excel_sheet_name")

    if not excel_file or not excel_sheet_name:
        print("Error: excel_file or excel_sheet_name not found in config.")
        return None

    df = preprocess_excel(excel_file, excel_sheet_name)

    if df is not None:
        rules = extract_rules_from_dataframe(df)
        return rules
    else:
        return None

def save_rules(rules, output_file):
    """Saves the extracted rules to a JSON file."""
    try:
        with open(output_file, "w") as f:
            json.dump(rules, f, indent=4)
        print(f"✅ Rules extracted and saved to {output_file}")
    except IOError as e:
        print(f"Error saving rules to {output_file}: {e}")

if __name__ == "__main__":
    config = load_config()
    if config is None:
        exit()

    rules = parse_excel(config)
    if rules:
        output_file = config.get("processed_rules_file")
        if output_file:
            save_rules(rules, output_file)
        else:
            print("Error: processed_rules_file not found in config.")