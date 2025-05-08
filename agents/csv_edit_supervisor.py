import logging
from typing import Dict, Any, Literal, TypedDict, Annotated, List, Union
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from agents.csv_verifier_agent import CSVVerifierAgent, VerifierOutput
from langchain_anthropic import ChatAnthropic
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command
from langgraph.graph.message import add_messages

from agents.csv_edit_agent import create_csv_edit_agent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Define the state for the supervisor graph
class SupervisorState(TypedDict):
    messages: Annotated[list, add_messages]
    csv_file_path: str
    source_data: Dict[str, Any]
    next: str
    original_request: str
    rewritten_request: str
    verification_failure: str

def create_csv_edit_supervisor_agent(verbose=False):
    """
    Create a CSV Edit Supervisor Agent using LangGraph.
    The supervisor delegates to the csv_edit worker agent.
    """
    # Define worker names
    members = ["csv_edit", "csv_verifier"]
    options = members + ["FINISH"]

    # Supervisor system prompt
    system_prompt = (
        "You are a supervisor tasked with managing a conversation for CSV editing. "
        f"Your available workers: {members}. "
        "Given the user request and the current state, respond with the worker to act next. "
        "Each worker will perform a task and respond with their results and status. "
        "Always follow this workflow:\n"
        "1. First send the request to csv_edit to perform the edits\n"
        "2. Then send to csv_verifier to check if the edits were performed correctly\n"
        "3. If verification FAILS, send to csv_edit again with the reason for failure\n"
        "4. Continue this cycle until verification PASSES, then respond with FINISH."
    )

    # LLM for supervisor
    llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)

    # TypedDict for router output
    class Router(TypedDict):
        next: Literal["csv_edit", "csv_verifier", "FINISH"]

    # Supervisor node
    def supervisor_node(state: SupervisorState) -> Command[Literal["csv_edit", "csv_verifier", "__end__"]]:
        if verbose:
            logger.info(f"[Supervisor] Entering supervisor_node with state: {state}")
        
        # Check for verification result messages from verifier agent
        for msg in reversed(state["messages"]):
            if hasattr(msg, 'name') and msg.name == "csv_verifier" and hasattr(msg, 'content'):
                content = msg.content
                
                # First try to extract JSON from the message if present
                try:
                    import json
                    json_content = None
                    
                    # Try to extract JSON from the message content
                    if '```json' in content and '```' in content:
                        json_start = content.find('```json') + 7
                        json_end = content.find('```', json_start)
                        if json_end > json_start:
                            json_content = content[json_start:json_end].strip()
                    elif '{' in content and '}' in content:
                        json_start = content.find('{')
                        json_end = content.rfind('}') + 1
                        if json_end > json_start:
                            json_content = content[json_start:json_end]
                    
                    if json_content:
                        verdict_data = json.loads(json_content)
                        if 'verdict' in verdict_data:
                            if verdict_data['verdict'] == "PASS":
                                if verbose:
                                    logger.info("[Supervisor] Found verification PASS verdict in JSON, ending workflow")
                                return Command(goto=END, update={"next": END})
                            elif verdict_data['verdict'] == "FAILED" or verdict_data['verdict'] == "FAIL":
                                if verbose:
                                    logger.info("[Supervisor] Found verification FAIL verdict in JSON, sending to csv_edit")
                                state["verification_failure"] = verdict_data.get('reason', content)
                                return Command(goto="csv_edit", update={"next": "csv_edit"})
                except (json.JSONDecodeError, ValueError, KeyError, IndexError):
                    # Not JSON or couldn't parse, fall back to string-based checks
                    pass
                
                # Fall back to simple string-based checks (for backward compatibility)
                if "FAIL" in content.upper() or "fail" in content.lower():
                    if verbose:
                        logger.info("[Supervisor] Found verification FAIL message, sending to csv_edit")
                    # Store the failure reason
                    state["verification_failure"] = content
                    return Command(goto="csv_edit", update={"next": "csv_edit"})
                elif "PASS" in content.upper() or "pass" in content.lower():
                    if verbose:
                        logger.info("[Supervisor] Found verification PASS message, ending workflow")
                    return Command(goto=END, update={"next": END})
        
        # If this is a brand new request, route to csv_edit
        if len(state["messages"]) == 1:
            if verbose:
                logger.info("[Supervisor] New request, routing to csv_edit")
            
            # Extract the original user request
            original_request = ""
            for msg in state["messages"]:
                if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                    if hasattr(msg, 'content'):
                        original_request = msg.content
                        break
            
            # Save original request in state
            state["original_request"] = original_request
            
            return Command(goto="csv_edit", update={"next": "csv_edit"})
            
        # Filter out empty messages to avoid API errors
        filtered_messages = []
        for msg in state["messages"]:
            # Skip messages with empty content
            if hasattr(msg, 'content') and (msg.content == [] or msg.content == ""):
                if verbose:
                    logger.info(f"[Supervisor] Skipping empty message: {msg}")
                continue
            filtered_messages.append(msg)
        
        messages = [
            {"role": "system", "content": system_prompt}
        ] + filtered_messages
        
        response = llm.with_structured_output(Router).invoke(messages)
        goto = response["next"]
        if verbose:
            logger.info(f"[Supervisor] Supervisor LLM chose next: {goto}")
        if goto == "FINISH":
            goto = END
        return Command(goto=goto, update={"next": goto})

    # Worker node: wraps the csv_edit agent
    csv_edit_graph = create_csv_edit_agent(verbose=verbose)
    def csv_edit_node(state: SupervisorState) -> Command[Literal["csv_verifier"]]:
        if verbose:
            logger.info(f"[Supervisor] Entering csv_edit_node with state: {state}")
            
        # Check if we already have a successful edit based on messages
        # If the most recent message is from csv_edit and indicates completion, skip to verification
        for msg in reversed(state["messages"]):
            if (hasattr(msg, 'name') and msg.name == "csv_edit" and 
                hasattr(msg, 'content') and "CSV edit complete" in msg.content):
                if verbose:
                    logger.info("[Supervisor] Found previous successful edit message, skipping to verification")
                return Command(
                    update=state,
                    goto="csv_verifier"
                )
        
        # Check if we have a recent tool message showing successful edit
        tool_message_found = False
        for msg in reversed(state["messages"]):
            if isinstance(msg, ToolMessage) and msg.name == "csv_pandas_edit":
                tool_message_found = True
                # Check if there's also an edit summary message
                for check_msg in reversed(state["messages"]):
                    if (hasattr(check_msg, 'name') and check_msg.name == "csv_edit" and 
                        "CSV edit complete" in check_msg.content):
                        if verbose:
                            logger.info("[Supervisor] Found ToolMessage with successful edit and completion message, skipping to verification")
                        return Command(
                            update=state,
                            goto="csv_verifier"
                        )
                break
                
        agent_input = {
            "messages": state["messages"],
            "csv_file_path": state["csv_file_path"],
            "source_data": state.get("source_data", {})
        }
        
        try:
            # Add a message limit to prevent the csv_edit_agent from going into an infinite loop
            # Only keep the last 6 messages to ensure the conversation doesn't get too long
            if len(agent_input["messages"]) > 6:
                logger.warning(f"[Supervisor] Truncating conversation - too many messages: {len(agent_input['messages'])}")
                # Always keep the first message (user request)
                important_msgs = [agent_input["messages"][0]]
                
                # Add the most recent edit result if it exists
                for msg in reversed(agent_input["messages"]):
                    if isinstance(msg, ToolMessage) and msg.name == "csv_pandas_edit":
                        important_msgs.append(msg)
                        break
                
                agent_input["messages"] = important_msgs[:6]
                
            # Run the edit agent
            result = csv_edit_graph.invoke(agent_input)
        except Exception as e:
            logger.error(f"[Supervisor] Error in csv_edit_node: {e}", exc_info=True)
            error_message = HumanMessage(
                content=f"Error executing CSV edit: {str(e)}",
                name="csv_edit"
            )
            return Command(
                update={
                    "messages": state["messages"] + [error_message],
                    "csv_file_path": state["csv_file_path"],
                    "source_data": state.get("source_data", {})
                },
                goto="csv_verifier"
            )
            
        if verbose:
            logger.info(f"[Supervisor] csv_edit_node result: {result}")
            
        # Extract only the relevant messages from the result
        # We only want the final tool message that shows the completed edit
        relevant_messages = []
        tool_message_found = False
        
        for msg in reversed(result.get("messages", [])):
            if isinstance(msg, ToolMessage) and msg.name == "csv_pandas_edit" and not tool_message_found:
                relevant_messages.append(msg)
                tool_message_found = True
                break
        
        # If we didn't find any relevant messages, just take the last message
        if not relevant_messages and result.get("messages"):
            relevant_messages = [result["messages"][-1]]
        
        # Add a summary message with a unique completion indicator
        hash_val = hash(str(relevant_messages))
        hash_str = str(abs(hash_val))[:8] if relevant_messages else 'unknown'
        edit_completion_marker = f"CSV edit complete #{hash_str}. Moving to verification."
        
        edit_summary = HumanMessage(
            content=edit_completion_marker,
            name="csv_edit"
        )
        
        updated_messages = state["messages"] + relevant_messages + [edit_summary]
        
        return Command(
            update={
                "messages": updated_messages,
                "csv_file_path": state["csv_file_path"],
                "source_data": state.get("source_data", {})
            },
            goto="csv_verifier"
        )

    # Verifier node: uses CSVVerifierAgent to dynamically check the CSV based on user request
    def csv_verifier_node(state: SupervisorState) -> Command[Literal["supervisor", "__end__"]]:
        try:
            if verbose:
                logger.info(f"[Supervisor] Entering csv_verifier_node with state: {state}")

            # Use the original user request from state if available, otherwise extract from messages
            user_request = state.get("original_request", "")
            if not user_request:
                # Try to extract from messages
                user_message = next((m for m in state["messages"] if isinstance(m, HumanMessage) and not hasattr(m, 'name')), None)
                user_request = user_message.content if user_message else ""

            verifier_agent = CSVVerifierAgent(verbose=verbose)
            verifier_state = {
                "messages": [],
                "csv_file_path": state["csv_file_path"],
                "user_request": user_request
            }
            result = verifier_agent.run(verifier_state)
            if verbose:
                logger.info(f"[Supervisor] csv_verifier_node result: {result}")
            
            # Get the final verdict directly from the verifier agent result
            decision = None
            reason = "Verification complete."
            verdict_data = result.get("final_verdict", None)
            
            if verdict_data and isinstance(verdict_data, dict) and "verdict" in verdict_data:
                # Use the structured JSON verdict
                if verdict_data["verdict"] == "PASS":
                    decision = "pass"
                    reason = verdict_data.get("reason", "The edit matches the core intent of the user request.")
                elif verdict_data["verdict"] == "FAILED" or verdict_data["verdict"] == "FAIL":
                    decision = "fail"
                    reason = verdict_data.get("reason", "The edit does not match the user request.")
                # If IN_PROGRESS, we'll fall back to looking at the last message
            
            # If no structured verdict was found, look at the last message as fallback
            if not decision and result.get("messages"):
                last_msg = result["messages"][-1]
                if hasattr(last_msg, 'content') and isinstance(last_msg.content, str):
                    content = last_msg.content
                    
                    # Try to extract JSON from the content
                    try:
                        import json
                        json_content = None
                        
                        # Handle code blocks with ```json``` format
                        if '```json' in content and '```' in content:
                            json_start = content.find('```json') + 7
                            json_end = content.find('```', json_start)
                            if json_end > json_start:
                                json_content = content[json_start:json_end].strip()
                        # Handle JSON directly in the content
                        elif '{' in content and '}' in content:
                            json_start = content.find('{')
                            json_end = content.rfind('}') + 1
                            if json_end > json_start:
                                json_content = content[json_start:json_end]
                                
                        if json_content:
                            parsed_verdict = json.loads(json_content)
                            if 'verdict' in parsed_verdict:
                                if parsed_verdict['verdict'] == "PASS":
                                    decision = "pass"
                                    reason = parsed_verdict.get("reason", "The edit matches the core intent of the user request.")
                                elif parsed_verdict['verdict'] == "FAILED" or parsed_verdict['verdict'] == "FAIL":
                                    decision = "fail"
                                    reason = parsed_verdict.get("reason", "The edit does not match the user request.")
                    except (json.JSONDecodeError, ValueError, KeyError, IndexError):
                        # Not JSON or couldn't parse, don't change the decision
                        pass
            
            # If we still don't have a decision, default to pass after sufficient verification
            if not decision:
                # Default to pass if we've gone through multiple verification steps
                if result.get("step_count", 0) >= 3:
                    decision = "pass"
                    reason = "The edit appears to match the user request after multiple verification steps."
                    logger.info("[Supervisor] No clear verdict found after multiple steps, defaulting to PASS")
                else:
                    # Otherwise default to fail to be conservative
                    decision = "fail"
                    reason = "Unable to verify that the edit matches the user request."
                    logger.info("[Supervisor] No clear verdict found with insufficient steps, defaulting to FAIL")

            # Format the verification message based on the decision
            if decision == "pass":
                verification_message = HumanMessage(
                    content=f"CSV verification PASSED: {reason}",
                    name="csv_verifier"
                )
                updated_messages = state["messages"] + [verification_message]
                if verbose:
                    logger.info("[Supervisor] Verification PASSED, ending workflow.")
                return Command(
                    update={
                        "messages": updated_messages,
                        "csv_file_path": state["csv_file_path"],
                        "source_data": state.get("source_data", {})
                    },
                    goto=END
                )
            else:
                verification_message = HumanMessage(
                    content=f"CSV verification FAILED: {reason}",
                    name="csv_verifier"
                )
                updated_messages = state["messages"] + [verification_message]
                if verbose:
                    logger.info("[Supervisor] Verification FAILED, returning to supervisor.")
                return Command(
                    update={
                        "messages": updated_messages,
                        "csv_file_path": state["csv_file_path"],
                        "source_data": state.get("source_data", {})
                    },
                    goto="supervisor"
                )
        except Exception as e:
            if verbose:
                logger.error(f"[Supervisor] Exception in csv_verifier_node: {e}")
            error_message = HumanMessage(
                content=f"CSV verification failed: {str(e)}",
                name="csv_verifier"
            )
            updated_messages = state["messages"] + [error_message]
            return Command(
                update={
                    "messages": updated_messages,
                    "csv_file_path": state["csv_file_path"],
                    "source_data": state.get("source_data", {})
                },
                goto="supervisor"
            )
    
    # Build the LangGraph
    builder = StateGraph(SupervisorState)
    builder.add_edge(START, "supervisor")
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("csv_edit", csv_edit_node)
    builder.add_node("csv_verifier", csv_verifier_node)
    builder.add_edge("csv_edit", "csv_verifier")
    builder.add_edge("csv_verifier", "supervisor")
    builder.add_edge("supervisor", END)  # fallback, in case supervisor returns END

    # Conditional edge from supervisor to worker or END
    def route_decision(state: SupervisorState):
        nxt = state.get("next", None)
        if nxt == "csv_edit":
            return "csv_edit"
        elif nxt == "csv_verifier":
            return "csv_verifier"
        else:
            return END

    builder.add_conditional_edges("supervisor", route_decision)

    return builder.compile()

class CSVEditSupervisorAgent:
    """
    Supervisor agent for CSV editing, orchestrating the csv_edit worker agent.
    """
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.graph = create_csv_edit_supervisor_agent(verbose=verbose)
        if self.verbose:
            logger.info("CSV Edit Supervisor Agent initialized")

    def _summarize_actions(self, messages: List[Any], user_request: str) -> str:
        """
        Generate a human-friendly summary of the actions taken by the agent.
        
        Args:
            messages: The list of messages from the agent execution
            user_request: The original user request
            
        Returns:
            A human-friendly summary of the actions taken
        """
        try:
            # Extract tool calls and results
            tool_calls = []
            for msg in messages:
                if isinstance(msg, AIMessage) and hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tool_call in msg.tool_calls:
                        if tool_call["name"] == "csv_pandas_edit" and "args" in tool_call:
                            args = tool_call["args"]
                            if "pandas_edit_command" in args:
                                tool_calls.append(args["pandas_edit_command"])
                
                # Also check for tool messages which contain results
                elif isinstance(msg, ToolMessage) and msg.name == "csv_pandas_edit":
                    tool_calls.append(f"Result: {msg.content}")
            
            # If no tool calls were found, return a generic message
            if not tool_calls:
                return "I processed your request, but no specific CSV editing actions were taken."
            
            # Use the LLM to generate a summary
            llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)
            commands = "\n".join(tool_calls)
            prompt = f"""
            The user requested: "{user_request}"
            
            The following pandas commands were executed on the CSV:
            {commands}
            
            Please provide a clear, concise, human-friendly summary of what was done to the CSV file.
            Focus on explaining the changes in plain language that a non-technical person would understand.
            Be specific about what data was modified, added, or removed.
            """
            
            summary = llm.invoke(prompt).content
            return summary
        except Exception as e:
            logger.error(f"[Supervisor] Error generating summary: {e}", exc_info=True)
            return f"I processed your request to {user_request}, but couldn't generate a detailed summary of the actions taken."

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the CSV Edit Supervisor Agent.
        Args:
            state: The initial state containing messages, csv_file_path, and source_data
        Returns:
            The final state after running the supervisor agent
        """
        try:
            if self.verbose:
                logger.info(f"[Supervisor] Running CSV Edit Supervisor Agent with state keys: {list(state.keys())}")
                logger.info(f"[Supervisor] Initial state: {state}")
                
            # Extract and log the user request if available
            user_request = ""
            if state.get("messages") and len(state["messages"]) > 0:
                first_msg = state["messages"][0]
                if hasattr(first_msg, "content"):
                    user_request = first_msg.content
                    logger.info(f"[Supervisor] User request: '{user_request}'")
                
            # Ensure required keys
            messages = state.get("messages", [])
            csv_file_path = state.get("csv_file_path", "")
            source_data = state.get("source_data", {})
            
            # Set a maximum number of steps to prevent infinite loops
            max_steps = 10
            step_count = 0
            
            supervisor_state = {
                "messages": messages,
                "csv_file_path": csv_file_path,
                "source_data": source_data,
                "next": "csv_edit",  # Always start with csv_edit
                "original_request": user_request,  # Store the original request
                "rewritten_request": "",
                "verification_failure": ""
            }
            
            # Add timeout protection - max 10 steps
            result = None
            while step_count < max_steps:
                step_count += 1
                if self.verbose:
                    logger.info(f"[Supervisor] Starting step {step_count} of {max_steps}")
                
                # Execute one step
                result = self.graph.invoke(supervisor_state)
                supervisor_state = result
                
                # Check for completion using JSON verdict if available
                completion_detected = False
                for msg in reversed(result.get("messages", [])):
                    if hasattr(msg, 'name') and msg.name == "csv_verifier" and hasattr(msg, 'content'):
                        content = msg.content
                        
                        # First try to extract JSON from the message if present
                        try:
                            import json
                            json_content = None
                            
                            # Try to extract JSON from the message content
                            if '```json' in content and '```' in content:
                                json_start = content.find('```json') + 7
                                json_end = content.find('```', json_start)
                                if json_end > json_start:
                                    json_content = content[json_start:json_end].strip()
                            elif '{' in content and '}' in content:
                                json_start = content.find('{')
                                json_end = content.rfind('}') + 1
                                if json_end > json_start:
                                    json_content = content[json_start:json_end]
                            
                            if json_content:
                                verdict_data = json.loads(json_content)
                                if 'verdict' in verdict_data and verdict_data['verdict'] == "PASS":
                                    if self.verbose:
                                        logger.info(f"[Supervisor] Workflow completed successfully after {step_count} steps (JSON verdict)")
                                    completion_detected = True
                                    break
                        except (json.JSONDecodeError, ValueError, KeyError, IndexError):
                            # Not JSON or couldn't parse, fall back to string-based checks
                            pass
                        
                        # Fall back to string check (for backward compatibility)
                        if "PASS" in content.upper():
                            if self.verbose:
                                logger.info(f"[Supervisor] Workflow completed successfully after {step_count} steps (string check)")
                            completion_detected = True
                            break
                
                # Early exit if successful (prioritize "next" state field then check completion detection)
                if supervisor_state.get("next") == END or completion_detected:
                    if self.verbose:
                        logger.info("[Supervisor] Early exit on successful completion")
                    break
            
            if step_count >= max_steps:
                logger.warning(f"[Supervisor] Reached maximum steps ({max_steps}), ending execution")
                # Add a message about reaching max steps
                warning_message = HumanMessage(
                    content=f"The workflow reached the maximum number of steps ({max_steps}). The process was stopped automatically.",
                    name="supervisor"
                )
                if result and "messages" in result:
                    result["messages"].append(warning_message)
            
            # Generate a human-friendly summary of the actions taken
            if result:
                action_summary = self._summarize_actions(result.get("messages", []), user_request)
                
                # Add the summary as a message from the csv_edit agent
                summary_message = HumanMessage(
                    content=action_summary,
                    name="csv_edit"
                )
                result["messages"].append(summary_message)
                
                if self.verbose:
                    logger.info(f"[Supervisor] Final result: {result}")
                return result
            else:
                return {"error": "Failed to execute workflow", "messages": messages}
                
        except Exception as e:
            logger.error(f"[Supervisor] Error running CSV Edit Supervisor Agent: {e}", exc_info=True)
            return {
                "error": str(e),
                "messages": state.get("messages", []),
                "csv_file_path": state.get("csv_file_path", "")
            }
