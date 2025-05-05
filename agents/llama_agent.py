from llama_cloud_services import LlamaExtract
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Get API key from environment variables
api_key = os.getenv('LLAMA_CLOUD_API_KEY')

# Initialize client with API key
extractor = LlamaExtract(api_key=api_key)

# Open and read the JSON file
with open('schema.json', 'r') as file:
    data = json.load(file)

agent = extractor.create_agent(name="template-schema", data_schema=data)

result = agent.extract("/home/raphael/excel-header-matcher/Untitled spreadsheet - Dublin Manifest.pdf")
print(result.data)