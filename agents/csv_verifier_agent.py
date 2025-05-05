import logging
from typing import Dict, Any, TypedDict, Annotated, List, Literal
from langchain_core.messages import HumanMessage, AIMessage
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field
from langchain_core.output_parsers import StrOutputParser
from langchain_core.output_parsers.json import JsonOutputParser
from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages

from tools.csv_edit_tools_file import csv_pandas_eval

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class VerifierOutput(BaseModel):
    verdict: Literal["PASS", "FAILED", "IN_PROGRESS"] = Field(description="The verdict of the verification")
    reason: str = Field(description="Detailed explanation with evidence for each operation")

class VerifierState(TypedDict):
    messages: Annotated[list, add_messages]
    csv_file_path: str
    user_request: str
    step_count: int

def create_csv_verifier_agent(verbose=False):
    """
    Create a CSV Verifier Agent using LangGraph.
    This agent uses only csv_pandas_eval to check specific rows/columns based on the user request.
    """
    tools = [csv_pandas_eval]
    llm = ChatOpenAI(model="gpt-4.1", temperature=0)

    system_message = """# Role and Objective
You are a CSV file verification system that evaluates whether requested changes have been properly implemented.

# Instructions
- Verify that requested CSV edits were successfully implemented 
- Do not chat, ask questions, or provide conversational responses
- Focus solely on verification of requested changes
- Your only purpose is to thoroughly validate each requested operation
- Return a structured verdict as your final output

# Verification Process
1. Understand exactly what changes were requested
2. Examine the current state of the data (post-edits)
3. For each specific operation in the user's request:
   a. Identify the exact column(s) or row(s) that should have been modified
   b. Use csv_pandas_eval to check the current state
   c. Verify if the current state matches the requested change
   d. Document clear evidence of success or failure

# Example Operations and Verification Approach
- Row removal → Check if specified rows no longer exist
- Column addition → Verify column exists with correct values
- Value updates → Compare values against requested changes
- Filtering → Confirm only matching rows remain
- Data cleaning → Verify format/standardization occurred

# Output Format
Your final response MUST be a JSON object:
```json
{
  "verdict": "VERDICT_VALUE",
  "reason": "Detailed explanation with evidence for each operation"
}
```

Where VERDICT_VALUE must be one of:
- "PASS" - ALL requested operations were implemented correctly
- "FAILED" - One or more operations were NOT implemented correctly
- "IN_PROGRESS" - Still performing verification checks

# Available Tool
csv_pandas_eval: Execute Python code to verify CSV data. Create a function 'verify_dataframe(df)' that returns verification results.

# Critical Requirements
- Make each check specific and targeted to verify a particular operation
- Gather complete evidence before making your determination
- Return IN_PROGRESS verdict if you still need to perform additional checks 
- Do not suggest new edits or alternative approaches
"""

    graph_builder = StateGraph(VerifierState)
    # Step limit to prevent infinite loops (increased because we're not forcing decisions)
    MAX_STEPS = 20

    def verifier_assistant(state: VerifierState):
        messages = state["messages"]
        csv_file_path = state["csv_file_path"]
        user_request = state["user_request"]
        step_count = state.get("step_count", 0) + 1

        if verbose:
            logger.info(f"[Verifier] Step {step_count} - User request: {user_request}")
            logger.info(f"[Verifier] CSV file path: {csv_file_path}")
            logger.info(f"[Verifier] Messages so far: {[m.content if hasattr(m, 'content') else str(m) for m in messages]}")

        # If this is the first step, prepend system/context messages
        if not messages:
            context_message = HumanMessage(
                content=f"User request: {user_request}\nCSV file path: {csv_file_path}"
            )
            messages = [HumanMessage(content=system_message, name="system"), context_message]
            
            # Immediately add the forced JSON verdict reminder
            force_json_message = HumanMessage(
                content="""YOU MUST END YOUR FINAL RESPONSE WITH A JSON VERDICT, like this:
```json
{
  "verdict": "PASS",  // or "FAILED"
  "reason": "Detailed explanation of your findings"
}
```
This JSON format is MANDATORY - I will not accept any other format for your final verdict."""
            )
            messages.append(force_json_message)

        # Add the user request to context if this is a later step
        if step_count == 2:
            reminder_message = """When verifying, follow this exact process:
1. Carefully examine whether ALL requested operations were successfully performed
2. For each operation, collect evidence to confirm it was done correctly
3. Only mark as PASS if ALL requested operations were successfully implemented
4. Your FINAL response MUST be in this JSON format:

```json
{
  "verdict": "PASS",  // Use "PASS" if ALL operations were successful, "FAILED" otherwise
  "reason": "Detailed explanation with evidence for each operation that you verified"
}
```

Make sure you complete ALL verifications before providing your final JSON verdict."""
            
            messages.append(HumanMessage(content=reminder_message))

        # Set up the JSON output parser for structured output
        parser = JsonOutputParser(pydantic_object=VerifierOutput)
        
        # Get the schema for verification output
        format_instructions = parser.get_format_instructions()
        
        if step_count > 2 and any(msg.name == "csv_pandas_eval" for msg in messages if hasattr(msg, "name")):
            # Once we have enough verification data, remind about structured output format
            format_message = HumanMessage(
                content=f"Based on your verification, please provide a final verdict in the proper format: {format_instructions}"
            )
            messages.append(format_message)
            
        llm_with_tools = llm.bind_tools(tools)
        response = llm_with_tools.invoke(messages)
        
        if verbose:
            logger.info(f"[Verifier] LLM response: {getattr(response, 'content', str(response))}")
            if hasattr(response, "tool_calls"):
                logger.info(f"[Verifier] Tool calls: {getattr(response, 'tool_calls', None)}")
                
        return {"messages": [response], "step_count": step_count}

    def tool_caller(state: VerifierState):
        last_message = state["messages"][-1]
        if not hasattr(last_message, "tool_calls") or not last_message.tool_calls:
            if verbose:
                logger.info("[Verifier] No tool calls in last message.")
            return {}
        tool_results = []
        step_count = state.get("step_count", 0) + 1
        for tool_call in last_message.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            if verbose:
                logger.info(f"[Verifier] Tool call: {tool_name} with args: {tool_args}")
            try:
                if tool_name == "csv_pandas_eval":
                    if "csv_file_path" not in tool_args:
                        tool_args["csv_file_path"] = state["csv_file_path"]
                    logger.info(f"Verifier executing tool {tool_name} with args {tool_args}")
                    
                    # Call the appropriate tool
                    result = csv_pandas_eval.invoke(tool_args)
                        
                    if verbose:
                        logger.info(f"[Verifier] Tool result: {result}")
                else:
                    result = f"Unknown tool: {tool_name}"
                    if verbose:
                        logger.warning(f"[Verifier] Unknown tool: {tool_name}")
                from langchain_core.messages import ToolMessage
                tool_message = ToolMessage(
                    content=str(result),
                    tool_call_id=tool_call["id"],
                    name=tool_name
                )
                tool_results.append(tool_message)
            except Exception as e:
                if verbose:
                    logger.error(f"[Verifier] Error executing tool {tool_name}: {e}")
                from langchain_core.messages import ToolMessage
                tool_message = ToolMessage(
                    content=f"Error: {str(e)}",
                    tool_call_id=tool_call["id"],
                    name=tool_name
                )
                tool_results.append(tool_message)
        # Append tool results to the message list for the next LLM step, and update step_count in state
        return {"messages": state["messages"] + tool_results, "step_count": step_count}

    graph_builder.add_node("assistant", verifier_assistant)
    graph_builder.add_node("tool_caller", tool_caller)

    def should_continue(state: VerifierState):
        """
        Determines if the agent should continue verification or end the process.
        This function checks:
        1. If the last message contains tool calls (continue to execute them)
        2. If the last message is a structured verdict that's not "IN_PROGRESS"
        3. If the maximum number of steps has been reached (safety limit)
        """
        import json
        last_message = state["messages"][-1]
        step_count = state.get("step_count", 0)
        
        # 1. If there are tool calls, continue to the tool caller
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            if verbose:
                logger.info("[Verifier] Continuing to execute tool calls")
            return "tool_caller"
        
        # 2. Check if the last message contains a verdict
        if hasattr(last_message, "content") and isinstance(last_message.content, str):
            content = last_message.content.strip()
            parser = JsonOutputParser(pydantic_object=VerifierOutput)
            
            try:
                # Try to extract and parse the JSON content
                if content.startswith('```json') and '```' in content[7:]:
                    # Extract JSON from code block
                    json_content = content[7:].split('```')[0].strip()
                elif '{' in content and '}' in content:
                    # Try to extract JSON object
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    json_content = content[json_start:json_end]
                else:
                    json_content = content
                
                # Try to parse as VerifierOutput
                try:
                    verdict_data = parser.parse(json_content)
                    # Use TypedDict for schema validation
                    if verdict_data["verdict"] != "IN_PROGRESS":
                        if verbose:
                            logger.info(f"[Verifier] Found verdict: {verdict_data['verdict']} - ending verification")
                        return END
                except Exception as parse_err:
                    # Fall back to regular JSON parsing if parser fails
                    verdict_data = json.loads(json_content)
                    if 'verdict' in verdict_data and verdict_data['verdict'] != "IN_PROGRESS":
                        if verbose:
                            logger.info(f"[Verifier] Found verdict: {verdict_data['verdict']} - ending verification")
                        return END
            except (json.JSONDecodeError, ValueError) as e:
                if verbose:
                    logger.info(f"[Verifier] Message doesn't contain valid JSON verdict: {str(e)}")
                # Not a JSON response, continue checking
                pass
                
        # 3. Safety check for max steps
        if step_count >= MAX_STEPS:
            if verbose:
                logger.info(f"[Verifier] Reached maximum steps ({MAX_STEPS}) - ending verification")
            return END
            
        # If none of the above, continue with the assistant
        if verbose:
            logger.info("[Verifier] No verdict or tool calls detected, continuing with assistant")
        return "assistant"

    # Add conditional edges from both assistant and tool_caller to ensure proper flow
    graph_builder.add_conditional_edges("assistant", should_continue)
    
    # Add a special check after tool execution to see if we should continue or end
    def after_tool_execution(state: VerifierState):
        """
        Determines if we should continue after tool execution.
        This allows the agent to end without going back to the assistant if max steps reached.
        """
        step_count = state.get("step_count", 0)
        
        # Check for sufficient verification data
        sufficient_data = False
        info_collected = False
        csv_structure_checked = False
        specific_checks_done = False
        
        # Count how many times the pandas_eval tool was used
        pandas_eval_count = 0
        for msg in state["messages"]:
            if hasattr(msg, "name") and msg.name == "csv_pandas_eval":
                pandas_eval_count += 1
        
        # If we've used pandas_eval at least 2 times, we probably have enough info
        # (One for structure inspection, one for specific checks)
        if pandas_eval_count >= 2:
            sufficient_data = True
            
        # Safety check for max steps
        if step_count >= MAX_STEPS:
            if verbose:
                logger.info(f"[Verifier] Reached maximum steps ({MAX_STEPS}) after tool execution - ending verification")
            
            # Create a properly formatted IN_PROGRESS message using the parser
            parser = JsonOutputParser(pydantic_object=VerifierOutput)
            in_progress_data = {
                "verdict": "IN_PROGRESS",
                "reason": "The verification process has reached the maximum number of steps without conclusively verifying all requested operations. The operations that were verified appear to be correctly implemented, but a complete verification was not finished."
            }
            
            formatted_message = parser.parse(in_progress_data)
            default_message = HumanMessage(content=formatted_message)
            state["messages"].append(default_message)
            return END
            
        # If we have sufficient data and are at least at step 3, prompt for structured verdict
        if sufficient_data and step_count >= 3:
            # Set up the JSON output parser for structured output
            parser = JsonOutputParser(pydantic_object=VerifierOutput)
            format_instructions = parser.get_format_instructions()
            
            nudge_message = HumanMessage(
                content=f"""Now that you have verified the CSV data, you MUST provide your final verdict in the proper format:

{format_instructions}

1. Review each of the requested operations from the user's original request
2. Confirm whether each specific operation was correctly implemented based on your verification
3. Set verdict to "PASS" only if ALL requested operations were properly implemented
4. Set verdict to "FAILED" if any operation was not implemented correctly
5. Set verdict to "IN_PROGRESS" only if you need to perform more verification steps
6. Provide a thorough explanation in the reason field with specific evidence"""
            )
            state["messages"].append(nudge_message)
            
        # Otherwise continue with the assistant
        return "assistant"
        
    graph_builder.add_conditional_edges("tool_caller", after_tool_execution)
    graph_builder.add_edge(START, "assistant")

    return graph_builder.compile()

class CSVVerifierAgent:
    """
    Agent for verifying CSV edits based on user instructions.
    Uses a LangGraph agent.
    """
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.graph = create_csv_verifier_agent(verbose=verbose)
        if self.verbose:
            logger.info("CSV Verifier Agent initialized")

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if self.verbose:
                logger.info(f"Running CSV Verifier Agent with state keys: {list(state.keys())}")
            messages = state.get("messages", [])
            csv_file_path = state.get("csv_file_path", "")
            user_request = state.get("user_request", "")
            step_count = state.get("step_count", 0)
            
            # Setup parser for structured output
            parser = JsonOutputParser(pydantic_object=VerifierOutput)
            format_instructions = parser.get_format_instructions()
            
            # Add instructional message to guide the agent
            if messages and len(messages) > 0:
                # Add a hint to examine the CSV structure first
                hint_message = HumanMessage(
                    content=f"You may want to first examine the CSV structure using csv_pandas_eval with a verification function like this:\n\ndef verify_dataframe(df):\n    # Check basic structure\n    structure_info = {{\n        'columns': list(df.columns),\n        'rows': len(df),\n        'sample_data': df.head(5).to_dict('records')\n    }}\n    return structure_info\n\nWhen you've completed verification, provide your verdict using this format:\n{format_instructions}"
                )
                # Insert after the first message
                messages = [messages[0], hint_message] + messages[1:] if len(messages) > 1 else messages + [hint_message]
                logger.info("Added hint for examining CSV structure with output format instructions")
            
            graph_input = {
                "messages": messages,
                "csv_file_path": csv_file_path,
                "user_request": user_request,
                "step_count": step_count
            }
            graph_output = self.graph.invoke(graph_input)
            all_messages = messages + graph_output["messages"]
            
            # Extract the verdict from the final message if possible
            final_verdict = None
            if all_messages and hasattr(all_messages[-1], "content"):
                try:
                    content = all_messages[-1].content
                    import json
                    # Try to extract JSON from various formats
                    if content.startswith('```json') and '```' in content[7:]:
                        json_content = content[7:].split('```')[0].strip()
                        final_verdict = json.loads(json_content)
                    elif '{' in content and '}' in content:
                        json_start = content.find('{')
                        json_end = content.rfind('}') + 1
                        json_content = content[json_start:json_end]
                        final_verdict = json.loads(json_content)
                except Exception as e:
                    if self.verbose:
                        logger.warning(f"Could not extract verdict from final message: {e}")
            
            result = {
                "messages": all_messages,
                "csv_file_path": csv_file_path,
                "user_request": user_request,
                "step_count": graph_output.get("step_count", step_count),
                "final_verdict": final_verdict
            }
            return result
        except Exception as e:
            logger.error(f"Error running CSV Verifier Agent: {e}", exc_info=True)
            return {
                "error": str(e),
                "messages": state.get("messages", []) + [AIMessage(content=f"Error: {str(e)}")],
                "csv_file_path": state.get("csv_file_path")
            }
