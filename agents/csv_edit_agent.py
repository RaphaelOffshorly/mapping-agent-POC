import logging
import os
import pandas as pd
from typing import Dict, Any, TypedDict, Annotated, List
from langchain_core.messages import HumanMessage, AIMessage
from langchain_anthropic import ChatAnthropic
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

from tools.csv_edit_tools_file import csv_pandas_edit, csv_pandas_eval

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class EditAgentState(TypedDict):
    messages: Annotated[list, add_messages]
    csv_file_path: str
    unedited_csv_path: str = ""  # Path to unedited CSV for verification

def create_csv_edit_agent(verbose=False):
    """
    Create a CSV Edit Agent using LangGraph.
    This agent uses a single tool to run pandas commands for CRUD and Excel-like operations.
    """
    tools = [csv_pandas_edit, csv_pandas_eval]
    llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)

    system_message = """You are a CSV editing agent.
The CSV file you must edit is located at: {csv_file_path}. Never ask the user for the file path. Always use this path in your tool calls.
Every time you edit you must call the csv_pandas_edit tool with both csv_file_path and pandas_edit_command. 
You also have access to the csv_pandas_eval tool which lets you verify your edits by providing a verification function that will be executed on the CSV without modifying it (for checking results only).
Do not respond with explanations or questions; always respond with a tool call.

IMPORTANT CHANGE: Instead of providing direct pandas commands, you must now define a function called 'edit_dataframe' that:
1. Takes a DataFrame as its only parameter
2. Performs the requested operations on the DataFrame
3. Returns the modified DataFrame

Your job is to:
1. Parse the user's request and generate a python function called 'edit_dataframe' that will perform the requested operation.
2. The function will receive the DataFrame loaded from the CSV file as its only parameter.
3. Use the csv_pandas_edit tool to execute your function definition and save the result back to the CSV.
4. You can perform any CRUD operation (Create Read Update Delete) and any Excel-like formula or transformation supported by pandas.
5. Make sure your function RETURNS the modified DataFrame at the end (don't just modify it in place).
6. Only use the csv_pandas_edit tool for editing. Do not use any other tools.
7. You can include as many operations as needed inside your function.
8. Do not perform any operation outside the explicit user request.

IMPORTANT: You must follow these rules:
- If the user requests to "fill" or "update" a column with a specific number of values (e.g. "Fill the Last Name column with Last Name 1-10"), your function should:
  1. Check if the DataFrame has enough rows
  2. If not, add more rows to the DataFrame to accommodate all the values
  3. Then fill the specified column with the new values
  4. And finally return the modified DataFrame
  5. Example: If the user says "Fill the Last Name column with Last Name 1-10" and the DataFrame has only 5 rows, your function should look like:
     ```
     def edit_dataframe(df):
         # First ensure we have enough rows
         if len(df) < 10:
             # Create a dictionary for empty rows
             empty_data = {{}}
             for col in df.columns:
                 empty_data[col] = [''] * (10 - len(df))
             # Create empty rows DataFrame and concatenate
             empty_rows = pd.DataFrame(empty_data)
             df = pd.concat([df, empty_rows], ignore_index=True)
         
         # Now fill the Last Name column
         df['Last Name'] = ['Last Name ' + str(i) for i in range(1, 11)]
         
         # Return the modified DataFrame
         return df
     ```

- If the user requests to "add" or "append" values to a single column (e.g. "Add Last Name 6-10 to the Last Name column") your function should append new rows to the DataFrame filling only the specified column with the new values and leaving all other columns blank.
- Example: If the user says "Add Last Name 6-10 to the Last Name column" and the DataFrame has columns ['First Name' 'Last Name' 'Contact'] your function should look like:
    ```
    def edit_dataframe(df):
        new_rows = pd.DataFrame({{'Last Name': ['Last Name 6', 'Last Name 7', 'Last Name 8', 'Last Name 9', 'Last Name 10'], 
                                'First Name': ['', '', '', '', ''], 
                                'Contact': ['', '', '', '', '']}})
        df = pd.concat([df, new_rows], ignore_index=True)
        return df
    ```
- Always preserve the column order and leave unspecified columns as empty strings.
- Make sure to return the modified dataframe in all cases.

When you respond always provide the complete function definition as a string argument to the csv_pandas_edit tool.
"""

    graph_builder = StateGraph(EditAgentState)

    def assistant(state: EditAgentState):
        messages = state["messages"]
        csv_file_path = state["csv_file_path"]

        if verbose:
            logger.info(f"[CSVEditAgent] Received state: {state}")
            logger.info(f"[CSVEditAgent] Messages: {[m.content if hasattr(m, 'content') else str(m) for m in messages]}")
            logger.info(f"[CSVEditAgent] CSV file path: {csv_file_path}")

        # Always prepend system/context messages to ensure the agent knows the CSV file path
        # Format the system message with the actual csv_file_path
        formatted_system_message = system_message.format(csv_file_path=csv_file_path)
        
        # Get the user request from the first message if it exists
        user_request = ""
        if len(state["messages"]) > 0 and hasattr(state["messages"][0], "content"):
            user_request = state["messages"][0].content
            
        context_message = HumanMessage(
            content=f"User request: {user_request}\nCSV file path: {csv_file_path}"
        )
        
        # Load the CSV file to get its structure first
        try:
            import pandas as pd
            if os.path.exists(csv_file_path):
                df = pd.read_csv(csv_file_path)
                df_info = {
                    "columns": list(df.columns),
                    "shape": df.shape,
                    "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                    "sample": df.head(3).to_dict('records') if not df.empty else {}
                }
                df_info_str = f"CSV Structure:\nColumns: {df_info['columns']}\nShape: {df_info['shape']}\nSample data: {df_info['sample']}"
                logger.info(f"Loaded CSV structure: {df_info_str[:200]}...")
            else:
                df_info_str = "CSV file not found."
                logger.warning(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            df_info_str = f"Error loading CSV: {str(e)}"
            logger.error(f"Error loading CSV structure: {e}")
        
        # Add DataFrame structure information to the context message
        enhanced_context_message = HumanMessage(
            content=f"User request: {user_request}\nCSV file path: {csv_file_path}\n\n{df_info_str}"
        )
        
        # Prepend system and context messages to ensure the agent has the CSV file path and structure
        messages = [HumanMessage(content=formatted_system_message, name="system"), enhanced_context_message] + messages

        # Add an updated instruction to ensure the agent knows to perform explicit verification
        enhanced_messages = messages + [
            HumanMessage(
                content="Remember to use the appropriate tool:\n"
                "1. Use csv_pandas_edit with both csv_file_path and pandas_edit_command to make changes to the CSV.\n"
                "2. IMPORTANT: After your edit is successful, you MUST use csv_pandas_eval to explicitly verify your changes.\n"
                "3. For example, after filling 'Last Name' column, verify with an explicit tool call to csv_pandas_eval with a verification function like:\n"
                "   ```\n"
                "   def verify_dataframe(df):\n"
                "       # Verify the Last Name column was properly filled\n"
                "       last_names = df['Last Name'].head(10).tolist()\n"
                "       return last_names\n"
                "   ```\n"
                "Note: Always make explicit tool calls for verification - do not rely on automatic verification."
            )
        ]

        llm_with_tools = llm.bind_tools(tools)
        response = llm_with_tools.invoke(enhanced_messages)
        if verbose:
            logger.info(f"[CSVEditAgent] LLM response: {getattr(response, 'content', str(response))}")
            if hasattr(response, "tool_calls"):
                logger.info(f"[CSVEditAgent] Tool calls: {getattr(response, 'tool_calls', None)}")
        return {"messages": [response]}

    def tool_caller(state: EditAgentState):
        last_message = state["messages"][-1]
        csv_file_path = state["csv_file_path"]
        
        if not hasattr(last_message, "tool_calls") or not last_message.tool_calls:
            if verbose:
                logger.info("[CSVEditAgent] No tool calls in last message.")
            return {}
            
        tool_results = []
        successful_edit = False
        edit_command = None
        
        # We no longer need to save a copy of the original CSV here - that's done by the supervisor
        
        # Execute the requested tool calls
        for tool_call in last_message.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            
            if verbose:
                logger.info(f"[CSVEditAgent] Tool call: {tool_name} with args: {tool_args}")
                
            if tool_name == "csv_pandas_edit":
                if "csv_file_path" not in tool_args:
                    tool_args["csv_file_path"] = csv_file_path
                    
                if "pandas_edit_command" in tool_args:
                    edit_command = tool_args["pandas_edit_command"]
                    
                logger.info(f"Executing tool {tool_name}")
                result = csv_pandas_edit.invoke(tool_args)
                
                if verbose:
                    logger.info(f"[CSVEditAgent] Tool result: {result}")
                    
                from langchain_core.messages import ToolMessage
                tool_message = ToolMessage(
                    content=str(result),
                    tool_call_id=tool_call["id"],
                    name=tool_name
                )
                tool_results.append(tool_message)
                
                # Check if the edit was successful
                if "Edit successful" in str(result):
                    successful_edit = True
            
            elif tool_name == "csv_pandas_eval":
                if "csv_file_path" not in tool_args:
                    tool_args["csv_file_path"] = csv_file_path
                    
                logger.info(f"Executing tool {tool_name}")
                result = csv_pandas_eval.invoke(tool_args)
                
                if verbose:
                    logger.info(f"[CSVEditAgent] Eval result: {result}")
                    
                from langchain_core.messages import ToolMessage
                tool_message = ToolMessage(
                    content=str(result),
                    tool_call_id=tool_call["id"],
                    name=tool_name
                )
                tool_results.append(tool_message)
        
        # Instead of automatic verification, we'll let the agent make explicit tool calls
        # This is important for Claude API compatibility where each tool result must have a corresponding tool call
        if successful_edit and verbose:
            logger.info("[CSVEditAgent] Edit successful. Agent should explicitly verify using csv_pandas_eval tool.")
        
        return {"messages": tool_results}

    def should_use_tools(state: EditAgentState):
        last_message = state["messages"][-1]
        csv_file_path = state["csv_file_path"]
        
        # Check for edit verification completion
        verification_complete = False
        successful_edit = False
        verification_message = None
        
        # First check if we have a verification summary or successful edit message
        for msg in reversed(state["messages"]):
            if hasattr(msg, "name"):
                # Look for verification summary message
                if msg.name == "edit_verification" and hasattr(msg, "content"):
                    verification_complete = True
                    verification_message = msg
                    break
                    
                # Or check for successful edit
                if msg.name == "csv_pandas_edit" and hasattr(msg, "content") and "Edit successful" in msg.content:
                    successful_edit = True
        
        # If we have verification completion, we're done with the current edit
        if verification_complete:
            if verbose:
                logger.info("[CSVEditAgent] Edit verified and completed successfully. Ending process.")
            return END
            
        # Check for evaluation results - if we have both an edit and evaluation, we can finish
        eval_found = False
        for msg in reversed(state["messages"]):
            if hasattr(msg, "name") and msg.name == "csv_pandas_eval":
                eval_found = True
                break
                
        if successful_edit and eval_found:
            if verbose:
                logger.info("[CSVEditAgent] Edit was successful and evaluation completed. Ending process.")
            return END
        
        # Check for consecutive identical edit commands (loops)
        tool_call_commands = []
        for msg in reversed(state["messages"]):
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                if len(msg.tool_calls) > 0 and "pandas_edit_command" in msg.tool_calls[0]["args"]:
                    command = msg.tool_calls[0]["args"]["pandas_edit_command"]
                    tool_call_commands.append(command)
                    # Only need to check the most recent commands
                    if len(tool_call_commands) >= 2:
                        break
                        
        # If we have consecutive identical commands, we're in a loop
        if len(tool_call_commands) >= 2 and tool_call_commands[0] == tool_call_commands[1]:
            if verbose:
                logger.info("[CSVEditAgent] Detected repetitive identical edit commands. Ending loop.")
            return END
        
        # Safety check: limit the number of tool messages to prevent endless loops
        tool_message_count = 0
        for msg in state["messages"]:
            if hasattr(msg, "name") and msg.name in ["csv_pandas_edit", "csv_pandas_eval"]:
                tool_message_count += 1
        
        # If we've had too many tool calls, end the process
        max_tool_calls = 5
        if tool_message_count >= max_tool_calls:
            if verbose:
                logger.info(f"[CSVEditAgent] Reached maximum number of tool calls ({max_tool_calls}). Ending process.")
            return END
        
        # If the last message has tool calls, continue to the tool caller
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            return "tool_caller"
        else:
            return END

    graph_builder.add_node("assistant", assistant)
    graph_builder.add_node("tool_caller", tool_caller)
    graph_builder.add_conditional_edges("assistant", should_use_tools)
    graph_builder.add_edge("tool_caller", "assistant")
    graph_builder.add_edge(START, "assistant")

    return graph_builder.compile()

class CSVEditAgent:
    """
    Agent for editing CSV data based on user instructions.
    Uses a LangGraph agent with a single pandas-editing tool.
    """
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.graph = create_csv_edit_agent(verbose=verbose)
        if self.verbose:
            logger.info("CSV Edit Agent initialized")

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if self.verbose:
                logger.info(f"[CSVEditAgent] Running CSV Edit Agent with state keys: {list(state.keys())}")
                logger.info(f"[CSVEditAgent] State: {state}")
            messages = state.get("messages", [])
            csv_file_path = state.get("csv_file_path", "")
            
            # Get the unedited CSV path if it's already in the state
            unedited_csv_path = state.get("unedited_csv_path", "")
            
            graph_input = {
                "messages": messages,
                "csv_file_path": csv_file_path,
                "unedited_csv_path": unedited_csv_path
            }
            graph_output = self.graph.invoke(graph_input)
            all_messages = messages + graph_output["messages"]
            
            # Include the unedited_csv_path in the result
            result = {
                "messages": all_messages,
                "csv_file_path": csv_file_path,
                "unedited_csv_path": graph_output.get("unedited_csv_path", unedited_csv_path)
            }
            if self.verbose:
                logger.info(f"[CSVEditAgent] Final result: {result}")
            return result
        except Exception as e:
            logger.error(f"[CSVEditAgent] Error running CSV Edit Agent: {e}", exc_info=True)
            return {
                "error": str(e),
                "messages": state.get("messages", []) + [AIMessage(content=f"Error: {str(e)}")],
                "csv_file_path": state.get("csv_file_path")
            }
