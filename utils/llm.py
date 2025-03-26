import logging
from typing import Dict, List, Any, Optional
from langchain_anthropic import ChatAnthropic
from config.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_llm(model_name: Optional[str] = None) -> ChatAnthropic:
    """
    Get a LangChain ChatAnthropic instance.
    
    Args:
        model_name: The name of the model to use, defaults to the one in config
        
    Returns:
        A ChatAnthropic instance
    """
    model = model_name or Config.DEFAULT_MODEL
    return ChatAnthropic(model=model, api_key=Config.ANTHROPIC_API_KEY)

def create_prompt_template(template: str) -> str:
    """
    Create a prompt template with proper formatting.
    
    Args:
        template: The template string
        
    Returns:
        A formatted prompt template
    """
    # Add any common formatting or structure needed for prompts
    return template
