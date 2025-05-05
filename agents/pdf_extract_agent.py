from llama_cloud_services import LlamaExtract
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class PDFExtractAgent:
    """
    Agent for extracting data from PDF files using LlamaExtract.
    """
    
    def __init__(self, verbose=False):
        """
        Initialize the PDF Extract Agent.
        
        Args:
            verbose (bool): Whether to print verbose output
        """
        self.verbose = verbose
        
        # Get API key from environment variables
        self.api_key = os.getenv('LLAMA_CLOUD_API_KEY')
        if not self.api_key:
            raise ValueError("LLAMA_CLOUD_API_KEY environment variable not found")
        
        # Initialize client with API key
        self.extractor = LlamaExtract(api_key=self.api_key)
        
        if self.verbose:
            logger.info("PDFExtractAgent initialized")
    
    def run(self, state):
        """
        Run the PDF extraction process.
        
        Args:
            state (dict): The state containing:
                - schema_path (str): Path to the schema JSON file
                - pdf_path (str): Path to the PDF file to extract data from
        
        Returns:
            dict: The extraction results
        """
        try:
            schema_path = state.get('schema_path')
            pdf_path = state.get('pdf_path')
            
            if not schema_path or not os.path.exists(schema_path):
                return {"error": f"Schema file not found: {schema_path}"}
            
            if not pdf_path or not os.path.exists(pdf_path):
                return {"error": f"PDF file not found: {pdf_path}"}
            
            # Load the schema
            with open(schema_path, 'r') as file:
                schema_data = json.load(file)
            
            if self.verbose:
                logger.info(f"Loaded schema from {schema_path}")
                logger.info(f"Extracting data from {pdf_path}")
            
            # Create agent with schema
            agent = self.extractor.get_agent(
                name=f"Excel Extractor"
            )

            agent.data_schema = schema_data
            agent.save()
            
            # Extract data from PDF
            result = agent.extract(pdf_path)
            
            if self.verbose:
                logger.info("Extraction completed")
            
            # Save the result to a file for debugging
            output_path = os.path.join(os.path.dirname(schema_path), "llama_agent_output.json")
            with open(output_path, 'w') as file:
                json.dump(result.data, file, indent=2)
            
            if self.verbose:
                logger.info(f"Saved extraction results to {output_path}")
            
            # Return the extracted data
            return {
                "success": True,
                "data": result.data,
                "output_path": output_path
            }
            
        except Exception as e:
            logger.error(f"Error in PDFExtractAgent: {str(e)}")
            return {"error": str(e)}
