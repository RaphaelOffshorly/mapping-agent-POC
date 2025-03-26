# Excel Header Matcher

A Flask web application that uses AI to match headers from Excel files to target columns. This application has been refactored to follow the agentic framework of Langgraph, using multiple agents to make decisions on how to improve the matching process.

[![GitHub](https://img.shields.io/badge/GitHub-Private_Repository-blue)](https://github.com/RaphaelOffshorly/mapping-agent-POC)

## Project Structure

The project follows a modular structure with clear separation of concerns:

```
excel-header-matcher/
├── agents/                 # Agent implementations
│   ├── base_agent.py       # Base agent class
│   ├── header_extractor_agent.py
│   ├── column_description_agent.py
│   ├── header_matching_agent.py
│   ├── sample_data_agent.py
│   └── suggestion_agent.py
├── config/                 # Configuration
│   └── config.py           # Application configuration
├── tools/                  # Tool implementations
│   ├── base_tool.py        # Base tool class
│   ├── header_extraction_tool.py
│   ├── column_description_tool.py
│   ├── header_matching_tool.py
│   ├── sample_data_tool.py
│   ├── header_suggestion_tool.py
│   └── data_suggestion_tool.py
├── utils/                  # Utility functions
│   ├── common.py           # Common utility functions
│   ├── excel.py            # Excel-related utility functions
│   └── llm.py              # LLM-related utility functions
├── static/                 # Static files (CSS, JS)
│   ├── css/
│   │   └── styles.css
│   └── js/
│       ├── main.js
│       └── results.js
├── templates/              # HTML templates
│   ├── index.html
│   └── results.html
├── app.py                  # Original Flask application
├── app_new.py              # Refactored Flask application
├── workflow.py             # Langgraph workflow
├── requirements.txt        # Project dependencies
└── README.md               # Project documentation
```

## Agentic Framework

The application uses the Langgraph framework to implement a multi-agent system for Excel header matching. The workflow consists of the following agents:

1. **HeaderExtractorAgent**: Extracts potential headers from Excel files
2. **ColumnDescriptionAgent**: Describes target columns using LLM
3. **HeaderMatchingAgent**: Matches headers to target columns using LLM
4. **SampleDataAgent**: Extracts sample data for matched headers
5. **SuggestionAgent**: Suggests headers and data for target columns

Each agent has access to specific tools that help it accomplish its task. The agents work together in a sequential workflow to process Excel files and match headers to target columns.

## Tools

The application provides the following tools to the agents:

1. **HeaderExtractionTool**: Extracts potential headers from Excel files
2. **ColumnDescriptionTool**: Describes target columns using LLM
3. **HeaderMatchingTool**: Matches headers to target columns using LLM
4. **SampleDataTool**: Extracts sample data for matched headers
5. **HeaderSuggestionTool**: Suggests headers for target columns using LLM
6. **DataSuggestionTool**: Suggests sample data for target columns using LLM

## Workflow

The workflow is defined in `workflow.py` and follows these steps:

1. Extract potential headers from the Excel file
2. Describe target columns using LLM
3. Match headers to target columns using LLM
4. Extract sample data for matched headers
5. Suggest headers and data for target columns

## Setup and Installation

### Prerequisites

- Python 3.9+ installed
- Git installed

### Step 1: Clone the Repository

```bash
git clone https://github.com/RaphaelOffshorly/mapping-agent-POC.git
cd mapping-agent-POC
```

### Step 2: Create and Activate a Virtual Environment

#### On Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

#### On macOS/Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Set Up Environment Variables

Create a `.env` file in the root directory with the following content:

```
# Required API Keys
ANTHROPIC_API_KEY=your_anthropic_api_key

# Flask Configuration
FLASK_SECRET_KEY=your_secret_key_here
FLASK_ENV=development
FLASK_DEBUG=1

# Optional Configuration
# MAX_CONTENT_LENGTH=16777216  # 16MB max upload size
# ALLOWED_EXTENSIONS=xlsx,xls,csv
```

Notes:
- You need an Anthropic API key to use Claude for AI matching
- Generate a random string for FLASK_SECRET_KEY (e.g., using `openssl rand -hex 24`)
- The `.env` file is excluded from Git in `.gitignore` for security

### Step 5: Run the Application

```bash
python app_new.py
```

The application will be available at `http://localhost:5000`

## Features

- Upload Excel files and specify target columns
- Extract potential headers from Excel files
- Match headers to target columns using AI
- Extract sample data for matched headers
- Suggest headers and data for target columns
- Export results to CSV

## Technologies Used

- Flask: Web framework
- Pandas: Excel file processing
- Anthropic Claude: LLM for matching and suggestions
- Langgraph: Agentic framework
- Bootstrap: Frontend styling
