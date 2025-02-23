# src/llm.py
import google.generativeai as genai
import logging
import openai
from azure.identity import DefaultAzureCredential


def initialize_llm(config):
    """Initializes the LLM client based on the configuration."""
    api_use = config.get("api_use", "Gemini")  # Default to Gemini if not specified

    try:
        if api_use.lower() == "gemini":
            return _initialize_gemini(config)
        elif api_use.lower() == "openai":
            return _initialize_openai(config)
        else:
            raise ValueError(f"Unsupported API specified: {api_use}.  Must be 'Gemini' or 'OpenAI'.")
    except Exception as e:
        logging.error(f"Failed to initialize LLM: {str(e)}")
        raise


def _initialize_gemini(config):
    """Initializes the Gemini client."""
    try:
        if not config.get("gemini_api_key"):
            raise ValueError("Gemini API key not found in config")

        genai.configure(api_key=config["gemini_api_key"])
        model_name = config.get("gemini_model", "gemini-1.5-flash")
        return genai.GenerativeModel(model_name)
    except Exception as e:
        logging.error(f"Failed to initialize Gemini: {str(e)}")
        raise


def _initialize_openai(config):
    """Initializes the OpenAI client."""
    try:
        # Get Azure credentials
        default_credential = DefaultAzureCredential()
        access_token = default_credential.get_token("https://cognitiveservices.azure.com/.default")

        if not access_token:
            raise ValueError("Failed to obtain Azure access token")

        # Initialize OpenAI client with Azure configuration
        return openai.AzureOpenAI(
            api_version=config.get("openai_api_version", "2024-06-01"),
            azure_endpoint=config.get("azure_openai_endpoint",
                                      "https://prod-1.services.unitedaistudio.uhg.com/aoai-shared-openai-prod-1"),
            api_key=access_token.token,
            azure_deployment=config.get("deployment_name", "gpt-4o_2024-05-13"),
            default_headers={
                "projectId": config.get("project_id", "0bef8880-4e98-413c-bc0b-41c280fd1b2a")
            }
        )
    except Exception as e:
        logging.error(f"Failed to initialize OpenAI client: {str(e)}")
        raise


def generate_test_cases_with_llm(llm_client, prompt, max_output_tokens=1000):
    """Generates test cases using the appropriate LLM client."""
    try:
        if isinstance(llm_client, genai.GenerativeModel):
            response = llm_client.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    max_output_tokens=max_output_tokens
                )
            )
            if hasattr(response, 'text'):
                return response.text
            else:
                logging.error(f"Error: LLM Response missing 'text' attribute.")
                return None
        elif isinstance(openai.AzureOpenAI):
            response = llm_client.chat.completions.create(
                model=llm_client.azure_deployment,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_output_tokens,  # Adjust as needed
            )
            return response.choices[0].message.content
        else:
            raise ValueError("Unsupported LLM client type.")

    except Exception as e:
        logging.error(f"Exception in generate_test_cases_with_llm: {e}")
        return None