import google.generativeai as genai
import logging

def initialize_llm(config):
    """Initializes the LLM client with error handling."""
    try:
        if not config.get("gemini_api_key"):
            raise ValueError("Gemini API key not found in config")
        
        genai.configure(api_key=config["gemini_api_key"])
        model_name = config.get("gemini_model", "gemini-1.5-flash")
        return genai.GenerativeModel(model_name)
    except Exception as e:
        logging.error(f"Failed to initialize LLM: {str(e)}")
        raise

def generate_test_cases_with_llm(llm_client, prompt, max_output_tokens=1000):
    """Generates test cases using an LLM and a given prompt."""
    try:
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

    except Exception as e:
        logging.error(f"Exception in generate_test_cases_with_llm: {e}")
        return None