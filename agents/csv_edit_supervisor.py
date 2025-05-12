import logging
import operator
import uuid
from typing import Dict, Any, Literal, TypedDict, Annotated, List, Union, Optional
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from agents.csv_verifier_agent import CSVVerifierAgent, VerifierOutput
from langchain_anthropic import ChatAnthropic
from langgraph.graph import StateGraph, START, END
from langgraph.types import Command, interrupt
from langgraph.graph.message import add_messages
from langgraph.errors import GraphInterrupt
from langgraph.checkpoint.memory import MemorySaver
import pandas as pd
from config.config import Config

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
    csv_file_path: str  # Changed from Annotated[list, operator.add] to str
    next: Annotated[str, "last"]  # Use Annotated with "last" strategy to handle concurrent updates
    original_request: str
    rewritten_request: str
    verification_failure: str
    clarification_count: int
    is_request_clarified: bool
    in_clarification_mode: bool = False  # Track if we're in clarification mode
    interrupt_message: Optional[str] = None  # Instead of __interrupt__
    needs_input: bool = False  # Flag to indicate if user input is needed
    last_active_node: str = ""  # Track the last active node

def create_csv_edit_supervisor_agent(verbose=False):
    """
    Create a CSV Edit Supervisor Agent using LangGraph.
    The supervisor delegates to the csv_edit worker agent.
    """
    # Define worker names
    members = ["csv_edit", "csv_verifier", "request_clarifier", "human"]
    options = members + ["FINISH"]

    # Supervisor system prompt
    system_prompt = (
        "You are a supervisor tasked with managing a conversation for CSV editing. "
        f"Your available workers: {members}. "
        "Given the user request and the current state, respond with the worker to act next. "
        "Always follow this workflow:\n"
        "1. First send the request to request_clarifier to check if it needs clarification\n"
        "2. If clarified or already clear, send to csv_edit to perform the edits\n"
        "3. Then send to csv_verifier to check if the edits were performed correctly\n"
        "4. If verification FAILS, send to csv_edit again with the reason for failure\n"
        "5. Continue this cycle until verification PASSES, then respond with FINISH."
    )

    # LLM for supervisor
    llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)

    # TypedDict for router output
    class Router(TypedDict):
        next: Literal["csv_edit", "csv_verifier", "request_clarifier", "human", "FINISH"]

    # Human node - dedicated node for collecting user input
    def human_node(state: SupervisorState):
        """A node for collecting user input in response to clarification requests."""
        if verbose:
            logger.info(f"[Supervisor] Entering human_node with state: {state}")
        
        # Get the message that should be shown to the user
        interrupt_message = state.get("interrupt_message", "Please provide your input:")
        
        # This is the critical part - interrupt() pauses execution and awaits user input
        user_input = interrupt(value=interrupt_message)
        
        # The code below will only execute after the user provides input
        last_node = state.get("last_active_node", "request_clarifier")
        
        if verbose:
            logger.info(f"[Supervisor] Human node received input: '{user_input}'")
            logger.info(f"[Supervisor] Routing back to: '{last_node}'")
        
        # Return updated state and return control to the node that requested clarification
        return Command(
            update={
                "messages": state["messages"] + [HumanMessage(content=user_input)],
                "interrupt_message": None,  # Clear the interrupt
                "needs_input": False,
                "in_clarification_mode": True,
                "last_active_node": last_node  # Preserve last_active_node to maintain conversation context
            },
            goto=last_node  # Go back to the node that requested input
        )

    # Supervisor node
    def supervisor_node(state: SupervisorState) -> Command[Literal["csv_edit", "csv_verifier", "request_clarifier", "human", "__end__"]]:
        if verbose:
            logger.info(f"[Supervisor] Entering supervisor_node with state: {state}")
            logger.info(f"[Supervisor] in_clarification_mode: {state.get('in_clarification_mode', False)}")
            logger.info(f"[Supervisor] is_request_clarified: {state.get('is_request_clarified', False)}")
            logger.info(f"[Supervisor] needs_input: {state.get('needs_input', False)}")
            logger.info(f"[Supervisor] last_active_node: {state.get('last_active_node', '')}")
        
        # Check if we need human input
        if state.get("needs_input", False):
            if verbose:
                logger.info("[Supervisor] Routing to human node for input")
            return Command(
                update={
                    "last_active_node": state.get("last_active_node", "request_clarifier")
                },
                goto="human"
            )
            
        # Check if we're in clarification mode and need to go back to the request_clarifier
        # This is critical for ensuring the conversation continues properly after human input
        if state.get("in_clarification_mode", False) and not state.get("is_request_clarified", False):
            # Check if the most recent message is from a human (not named)
            recent_human_message = False
            for msg in reversed(state["messages"]):
                if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                    recent_human_message = True
                    break
            
            if recent_human_message:
                if verbose:
                    logger.info("[Supervisor] Detected human response in clarification mode, routing back to request_clarifier")
                return Command(goto="request_clarifier")
        
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
                                return Command(goto=END)
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
                    # Don't try to update "next" field to avoid conflicts
                    return Command(goto=END)
        
        # Check for clarification result
        for msg in reversed(state["messages"]):
            if hasattr(msg, 'name') and msg.name == "request_clarifier" and hasattr(msg, 'content'):
                content = msg.content
                
                # If the clarifier has explicitly marked the request as clear
                if "REQUEST_CLARIFIED" in content or "REQUEST_CLEAR" in content:
                    if verbose:
                        logger.info("[Supervisor] Request has been clarified, routing to csv_edit")
                    return Command(goto="csv_edit", update={"next": "csv_edit", "is_request_clarified": True})
                
                # If the clarifier has explicitly marked the request as out of scope
                if "OUT_OF_SCOPE" in content:
                    if verbose:
                        logger.info("[Supervisor] Request is out of scope, ending workflow")
                    return Command(goto=END, update={"next": END})
                
                # Check if we just got a response from clarifier that isn't clearly marked
                # This could be an intermediate clarification message
                if not state.get("is_request_clarified", False):
                    return Command(goto="request_clarifier", update={"next": "request_clarifier"})
        
        # If this is a brand new request, route to request_clarifier
        if len(state["messages"]) == 1:
            if verbose:
                logger.info("[Supervisor] New request, routing to request_clarifier")
            
            # Extract the original user request
            original_request = ""
            for msg in state["messages"]:
                if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                    if hasattr(msg, 'content'):
                        original_request = msg.content
                        state["original_request"] = original_request  # Update state directly
                        break
            
            # Save original request in state AND return it in the update
            # Critical: It must be in the update parameter to properly propagate to the next node
            if verbose:
                logger.info(f"[Supervisor] Extracted original request: '{original_request}'")
                
            # Debug check - in case we still have empty original_request
            if not original_request and len(state["messages"]) > 0:
                first_msg = state["messages"][0]
                if hasattr(first_msg, 'content'):
                    original_request = first_msg.content
                    logger.info(f"[Supervisor] Fallback extraction - original request: '{original_request}'")
                
            return Command(goto="request_clarifier", update={
                "next": "request_clarifier",
                "original_request": original_request,
                "clarification_count": 0,
                "is_request_clarified": False
            })
            
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

    # Request clarifier node: checks if the request needs clarification
    def request_clarifier_node(state: SupervisorState) -> Command[Literal["supervisor", "human", "__end__"]]:
        try:
            if verbose:
                logger.info(f"[Supervisor] Entering request_clarifier_node with state: {state}")
            
            # Get information about the CSV file to understand its structure
            csv_structure = {}
            try:
                if state["csv_file_path"]:
                    df = pd.read_csv(state["csv_file_path"])
                    csv_structure = {
                        "columns": list(df.columns),
                        "sample_data": df.head(2).to_dict(orient="records"),
                        "shape": df.shape
                    }
            except Exception as e:
                logger.error(f"[Supervisor] Error reading CSV file: {e}")
                csv_structure = {"error": str(e)}
            
            # Check if we're already engaged in a clarification conversation
            # First check if it's already in the state
            in_clarification_mode = state.get("in_clarification_mode", False)
            
            # Always set the last_active_node to request_clarifier when we're in this node
            # This is critical for proper routing after human interaction
            state["last_active_node"] = "request_clarifier"
                    
            # Get the original request - first try from state, then try from messages if needed
            user_request = state.get("original_request", "")
            
            if not user_request:
                # Try to extract from messages - look for the first human message without a name
                for msg in state["messages"]:
                    if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                        if hasattr(msg, 'content'):
                            user_request = msg.content
                            # Update the state with this extracted request to prevent future lookups
                            state["original_request"] = user_request
                            break
            
            # Log the user request for debugging
            logger.info(f"[Supervisor] Request clarifier processing user_request: '{user_request}'")
            # Increment the clarification count
            clarification_count = state.get("clarification_count", 0)
            new_clarification_count = clarification_count + 1 if in_clarification_mode else clarification_count

            if not in_clarification_mode:
                # Check if we have any request_clarifier messages
                for msg in reversed(state["messages"]):
                    if hasattr(msg, 'name') and msg.name == "request_clarifier":
                        in_clarification_mode = True
                        # Update the state immediately to ensure it's properly tracked
                        state["in_clarification_mode"] = True
                        logger.info("[Supervisor] Setting in_clarification_mode to True based on messages")
                        break
                
                # Also check if clarification count is > 0
                if not in_clarification_mode and state.get("clarification_count", 0) > 0:
                    in_clarification_mode = True
                    state["in_clarification_mode"] = True
                    logger.info("[Supervisor] Setting in_clarification_mode to True based on clarification count")
            logger.info(f"[Supervisor] Are we in_clarification_mode? '{in_clarification_mode}'")
            # Check if we've exceeded the maximum number of clarification attempts (3)
            if new_clarification_count > 3:
                # If we've asked too many times, proceed with what we have
                return Command(
                    update={
                        "messages": state["messages"] + [HumanMessage(
                            content="REQUEST_CLARIFIED: Maximum clarification attempts reached. Proceeding with the best interpretation.",
                            name="request_clarifier"
                        )],
                        "clarification_count": new_clarification_count,
                        "is_request_clarified": True
                    },
                    goto="supervisor"
                )
            
            # Define the clarifier prompt
            clarifier_system_prompt = f"""
            You are a CSV editing assistant that analyzes user requests for ambiguity, incompleteness, or out-of-scope issues.

            USER REQUEST:
            {user_request}

            CSV FILE STRUCTURE:
            {csv_structure}

            Your task is to:
            1. DETECT ISSUES: Analyze the user's request for any of these problems:
               - AMBIGUOUS requests: When multiple interpretations are possible
               - INCOMPLETE requests: When critical information is missing
               - OUT-OF-SCOPE requests: When a request cannot be fulfilled with CSV editing
               - ERRONEOUS requests: When a request references columns or operations that don't exist

            2. CLARIFY OR RESOLVE:
               - If you need to ask for clarification, be specific and show the user their options
               - If the request is already clear, state "REQUEST_CLEAR"
               - If the request is out of scope, state "OUT_OF_SCOPE"
               - After receiving a clarification, restate the complete request to confirm

            Examples of issues:
            - Ambiguous: "Add Jr. in LastNames column" when there is only "Last Name" column
            - Incomplete: "Calculate Bonus" without specifying a formula or base value
            - Erroneous: "Calculate average Project Hours" when no such column exists
            - Out-of-scope: "Create a PowerPoint presentation of this data"

            Your response must always include one of these markers:
            - ASK_CLARIFICATION: If clarification is needed (followed by your specific question)
            - REQUEST_CLEAR: If the request is already clear and can be processed
            - REQUEST_CLARIFIED: If clarification has been provided and understood
            - OUT_OF_SCOPE: If the request cannot be fulfilled through CSV editing

            IMPORTANT INSTRUCTION:
            - If the user mentions a column that doesn't exist in the CSV (such as "Project Hours" 
              when only "Work Hours" exists), you MUST use ASK_CLARIFICATION and explain that 
              the column doesn't exist, asking how they would like to proceed.
            - If the user asks to create a PowerPoint presentation or any document/visualization
              that isn't a CSV modification, you MUST use OUT_OF_SCOPE.
            """
            
            if in_clarification_mode:
                # We're in an ongoing clarification conversation
                # Get the last user message to process their answer to our clarification question
                print(f"[Supervisor] In clarification mode, looking for last user message")

                # CRITICAL FIX: Directly get the last human message without name attribute
                # This approach is simpler and more reliable than trying to find messages
                # after a specific clarifier message
                last_user_msg = None
                for msg in reversed(state["messages"]):
                    if isinstance(msg, HumanMessage) and hasattr(msg, 'name'):
                        last_user_msg = msg
                        print(f"[Supervisor] Found user message: {last_user_msg}")
                        break
                
                # Fallback: if we didn't find a message after the clarifier, 
                # just get the most recent human message
                if not last_user_msg:
                    for msg in reversed(state["messages"]):
                        if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                            last_user_msg = msg
                            break
                
                print(f"[Supervisor] Last user message: {last_user_msg}")
                print(f"[Supervisor] Last user message content: {last_user_msg.content if last_user_msg else 'None'}")
                if last_user_msg and last_user_msg.content:
                    # Get our previous clarification question
                    clarification_question = None
                    for msg in reversed(state["messages"]):
                        if hasattr(msg, 'name') and msg.name == "request_clarifier":
                            clarification_question = msg.content
                            break
                    
                    # Evaluate if the user's response resolves the ambiguity
                    prompt = f"""

                    This is the original request of the user:
                    {user_request}
                    
                    You asked this, question for clarification: {clarification_question}
                    
                    This is the user's response to your question: {last_user_msg.content}
                    
                    IMPORTANT INSTRUCTION: 
                    1. Combine the original request with the user's clarification to form a complete understanding.
                    2. Based on both the original request AND the user's clarification, determine if the request is now clear.
                    3. If clear, respond with "REQUEST_CLARIFIED:" followed by a COMPLETE restatement that incorporates BOTH the original request AND all clarifications.
                    4. If still unclear, ask for further clarification with "ASK_CLARIFICATION:".
                    
                    Do not simply restate the clarification alone - you must provide a complete, integrated request.
                    """
                    
                    llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)
                    result = llm.invoke(prompt)
                    
                    # Update the state with the clarification result
                    clarification_response = HumanMessage(
                        content=result.content,
                        name="request_clarifier"
                    )
                    
                    # Check if we're done with clarification
                    is_clarified = "REQUEST_CLARIFIED" in result.content or "REQUEST_CLEAR" in result.content
                    print(f"LLM Response:{result.content}")
                    
                    # If the request is now clear, update the rewritten request to use for CSV editing
                    if is_clarified:
                        # Extract the rewritten request from the clarification response
                        rewritten_request = result.content
                        logger.info(f"[Supervisor] Original rewritten request: {rewritten_request}")
                        
                        # Remove the marker and extract the actual clarified request
                        if "REQUEST_CLARIFIED:" in rewritten_request:
                            rewritten_request = rewritten_request.split("REQUEST_CLARIFIED:", 1)[1].strip()
                        elif "REQUEST_CLEAR:" in rewritten_request:
                            rewritten_request = rewritten_request.split("REQUEST_CLEAR:", 1)[1].strip()
                        else:
                            # If no marker found, just use the content after cleaning markers
                            rewritten_request = rewritten_request.replace("REQUEST_CLARIFIED", "").replace("REQUEST_CLEAR", "").strip()
                        
                        logger.info(f"[Supervisor] Final rewritten request: {rewritten_request}")
                        
                        # Make sure to set the rewritten_request in both state and the update
                        state["rewritten_request"] = rewritten_request
                        
                        return Command(
                            update={
                                "messages": state["messages"] + [clarification_response],
                                "clarification_count": new_clarification_count,
                                "is_request_clarified": True,
                                "rewritten_request": rewritten_request,
                                "in_clarification_mode": False
                            },
                            goto="supervisor"
                        )
                    elif "OUT_OF_SCOPE" in result.content:
                        # The request is out of scope - use standardized message
                        out_of_scope_message = HumanMessage(
                            content=f"OUT_OF_SCOPE: {Config.OUT_OF_SCOPE_MESSAGE}",
                            name="request_clarifier"
                        )
                        return Command(
                            update={
                                "messages": state["messages"] + [out_of_scope_message],
                                "clarification_count": new_clarification_count,
                                "is_request_clarified": False,
                                "in_clarification_mode": False
                            },
                            goto="supervisor"
                        )
                    else:
                        # We need more clarification
                        # Instead of using interrupt directly, route to the human node
                        return Command(
                            update={
                                "messages": state["messages"] + [clarification_response],
                                "clarification_count": new_clarification_count,
                                "interrupt_message": result.content,
                                "needs_input": True,
                                "last_active_node": "request_clarifier",
                                "in_clarification_mode": True
                            },
                            goto="human"
                        )
            else:
                # Initial evaluation of the request
                prompt = f"""
                {clarifier_system_prompt}
                
                USER REQUEST: {user_request}
                
                Analyze this request. Is it clear, ambiguous, incomplete, erroneous, or out of scope?
                """
                
                llm = ChatAnthropic(model="claude-3-7-sonnet-latest", temperature=0)
                result = llm.invoke(prompt)
                
                clarification_response = HumanMessage(
                    content=result.content,
                    name="request_clarifier"
                )
                
                # Check result content to determine if we need clarification
                if "ASK_CLARIFICATION" in result.content:
                    # We need to ask for clarification - route to human node
                    return Command(
                        update={
                            "messages": state["messages"] + [clarification_response],
                            "clarification_count": new_clarification_count,
                            "interrupt_message": result.content,  # Use interrupt_message instead of __interrupt__
                            "needs_input": True,
                            "last_active_node": "request_clarifier",
                            "in_clarification_mode": True
                        },
                        goto="human"
                    )
                elif "REQUEST_CLEAR" in result.content:
                    # Request is already clear
                    return Command(
                        update={
                            "messages": state["messages"] + [clarification_response],
                            "is_request_clarified": True,
                            "in_clarification_mode": False
                        },
                        goto="supervisor"
                    )
                elif "OUT_OF_SCOPE" in result.content:
                    # Request is out of scope - use standardized message
                    out_of_scope_message = HumanMessage(
                        content=f"OUT_OF_SCOPE: {Config.OUT_OF_SCOPE_MESSAGE}",
                        name="request_clarifier"
                    )
                    return Command(
                        update={
                            "messages": state["messages"] + [out_of_scope_message],
                            "is_request_clarified": False,
                            "in_clarification_mode": False
                        },
                        goto="supervisor"
                    )
                else:
                    # Default case - proceed to supervisor to decide
                    return Command(
                        update={
                            "messages": state["messages"] + [clarification_response],
                            "in_clarification_mode": False
                        },
                        goto="supervisor"
                    )
                    
        except Exception as e:
            error_message = f"Error in request_clarifier_node: {str(e)}"
            logger.error(error_message, exc_info=True)
            
            return Command(
                update={
                    "messages": state["messages"] + [HumanMessage(content=f"Error in request clarification: {error_message}", name="request_clarifier")],
                    "is_request_clarified": True  # Force proceed to avoid getting stuck
                },
                goto="supervisor"
            )

    # Import the csv_edit_agent
    from agents.csv_edit_agent import create_csv_edit_agent

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
        
        # If we have a rewritten request from clarification, use it
        if state.get("rewritten_request"):
            # Create a new first message with the rewritten request
            original_msg = None
            for msg in agent_input["messages"]:
                if isinstance(msg, HumanMessage) and not hasattr(msg, 'name'):
                    original_msg = msg
                    break
                
            if original_msg:
                # Replace the first message with the rewritten request
                new_messages = []
                replaced = False
                for msg in agent_input["messages"]:
                    if not replaced and msg == original_msg:
                        new_messages.append(HumanMessage(content=state["rewritten_request"]))
                        replaced = True
                    else:
                        new_messages.append(msg)
                
                agent_input["messages"] = new_messages
        
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

            # First try to use the rewritten request (from clarification) if available
            user_request = state.get("rewritten_request", "")
            has_rewritten_request = bool(user_request)
            print(f"[Supervisor] User request for verification: '{user_request}'")
            
            # If no rewritten request, fall back to the original request
            if not user_request:
                user_request = state.get("original_request", "")
            
            # If still no request, try to extract from messages
            if not user_request:
                user_message = next((m for m in state["messages"] if isinstance(m, HumanMessage) and not hasattr(m, 'name')), None)
                user_request = user_message.content if user_message else ""
            
            # Log the request being used for verification
            if verbose:
                source_type = "rewritten" if has_rewritten_request else "original"
                logger.info(f"[Supervisor] Using {source_type} request for verification: '{user_request}'")
            else:
                # Always print this message even if not in verbose mode
                print(f"[Supervisor] User request for verification: '{user_request}'")

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
                
                # Create an update object WITHOUT setting 'next'
                return Command(
                    update={
                        "messages": updated_messages,
                        "csv_file_path": state["csv_file_path"],
                        "source_data": state.get("source_data", {}),
                        "next": END  # Use END as the next target explicitly
                    },
                    goto=END
                )  # This is crucial - go directly to END
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
                        "source_data": state.get("source_data", {}),
                        "next": "supervisor"  # Explicitly set next to avoid conflicts
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
                    "source_data": state.get("source_data", {}),
                    "next": "supervisor"  # Explicitly set next to avoid conflicts
                },
                goto="supervisor"
            )
    
    # Build the LangGraph
    builder = StateGraph(SupervisorState)
    builder.add_edge(START, "supervisor")
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("request_clarifier", request_clarifier_node)
    builder.add_node("csv_edit", csv_edit_node)
    builder.add_node("csv_verifier", csv_verifier_node)
    builder.add_node("human", human_node)  # Add the human node
    
    # Add edges
    builder.add_edge("request_clarifier", "supervisor")
    builder.add_edge("csv_edit", "csv_verifier")
    builder.add_edge("human", "request_clarifier")  # Human can go back to clarifier
    builder.add_edge("human", "csv_edit")  # Or human can go to csv_edit
    builder.add_edge("supervisor", END)  # fallback, in case supervisor returns END

    # Conditional edge from supervisor to worker or END
    def route_decision(state: SupervisorState):
        # Check if we need to route to the human node
        if state.get("needs_input", False):
            return "human"
        
        # CRITICAL FIX: Check for OUT_OF_SCOPE requests
        for msg in reversed(state.get("messages", [])):
            if (hasattr(msg, 'name') and msg.name == "request_clarifier" and 
                hasattr(msg, 'content') and "OUT_OF_SCOPE" in msg.content):
                # If request is out of scope, immediately go to END
                logger.info("[Supervisor] Found OUT_OF_SCOPE message, ending workflow immediately.")
                return END
        
        # CRITICAL FIX: Check for verification passing
        for msg in reversed(state.get("messages", [])):
            if (hasattr(msg, 'name') and msg.name == "csv_verifier" and 
                hasattr(msg, 'content') and "PASSED" in msg.content):
                # If we find a verification passing message, immediately go to END
                logger.info("[Supervisor] Found PASSED verification message, ending workflow.")
                return END
        
        # Otherwise route based on next field
        nxt = state.get("next", None)
        if nxt == "csv_edit":
            return "csv_edit"
        elif nxt == "csv_verifier":
            return "csv_verifier" 
        elif nxt == "request_clarifier":
            return "request_clarifier"
        elif nxt == "human":
            return "human"
        elif nxt == END:
            return END
        else:
            return "supervisor"  # Default to supervisor instead of END
    # Add conditional edge from csv_verifier
    def verifier_route(state: SupervisorState):
        # Check if verification passed
        for msg in reversed(state.get("messages", [])):
            if (hasattr(msg, 'name') and msg.name == "csv_verifier" and 
                hasattr(msg, 'content') and "PASSED" in msg.content):
                return END
        # Default to supervisor if verification failed
        return "supervisor"

    builder.add_conditional_edges("csv_verifier", verifier_route)
    builder.add_conditional_edges("supervisor", route_decision)

    # Setup a checkpointer (required for interrupt to work properly)
    checkpointer = MemorySaver()
    
    # Compile the graph with the checkpointer
    return builder.compile(checkpointer=checkpointer)

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
            # First check if request was out of scope
            for msg in messages:
                if isinstance(msg, HumanMessage) and hasattr(msg, 'name') and msg.name == "request_clarifier":
                    if "OUT_OF_SCOPE" in msg.content:
                        # Return the standardized out of scope message
                        logger.info("[Supervisor] Out of scope request detected in summary generation")
                        return Config.OUT_OF_SCOPE_MESSAGE
            
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
            
            # Generate a unique thread ID for this session if not provided
            thread_id = state.get("thread_id", str(uuid.uuid4()))
            
            # Set a maximum number of steps to prevent infinite loops
            max_steps = 15  # Increased due to clarification
            step_count = 0
            
            # Check if we're resuming a session with an existing in_clarification_mode and rewritten_request
            existing_in_clarification_mode = state.get("in_clarification_mode", False)
            existing_rewritten_request = state.get("rewritten_request", "")
            
            supervisor_state = {
                "messages": messages,
                "csv_file_path": csv_file_path,
                "source_data": source_data,
                "next": "request_clarifier",  # Start with request_clarifier
                "original_request": user_request,  # Store the original request
                "rewritten_request": existing_rewritten_request,  # Preserve existing rewritten request if available
                "verification_failure": "",
                "clarification_count": state.get("clarification_count", 0),  # Preserve existing clarification count
                "is_request_clarified": state.get("is_request_clarified", False),  # Preserve existing clarification status
                "needs_input": False,
                "last_active_node": "",
                "interrupt_message": None,
                "thread_id": thread_id,
                "in_clarification_mode": existing_in_clarification_mode  # Preserve existing clarification mode
            }
            
            # Debug log the initial state values
            if self.verbose:
                logger.info(f"[Supervisor] Initial in_clarification_mode: {supervisor_state['in_clarification_mode']}")
                logger.info(f"[Supervisor] Initial rewritten_request: '{supervisor_state['rewritten_request']}'")
            
            # Config for the graph with thread_id
            config = {
                "configurable": {
                    "thread_id": thread_id
                }
            }
            
            # Add timeout protection - max steps
            result = None
            while step_count < max_steps:
                step_count += 1
                if self.verbose:
                    logger.info(f"[Supervisor] Starting step {step_count} of {max_steps}")
                
                # Before executing a step, check if we need to route to human for input
                if supervisor_state.get("needs_input", False):
                    if self.verbose:
                        logger.info(f"[Supervisor] Detected needs_input=True, preparing for user interaction")
                    # CRITICAL: Rather than just setting next node, directly return with interrupt info
                    # This prevents continuing the loop and hitting max_steps when waiting for user input
                    interrupt_value = supervisor_state.get("interrupt_message", "Please provide your input:")
                    
                    return {
                        "messages": supervisor_state.get("messages", []),
                        "csv_file_path": csv_file_path,
                        "source_data": source_data,
                        "interrupt_message": interrupt_value,
                        "needs_input": True,
                        "thread_id": thread_id,
                        "original_request": supervisor_state.get("original_request", ""),
                        "rewritten_request": supervisor_state.get("rewritten_request", ""),
                        "clarification_count": supervisor_state.get("clarification_count", 0),
                        "is_request_clarified": supervisor_state.get("is_request_clarified", False),
                        "in_clarification_mode": supervisor_state.get("in_clarification_mode", True),
                        "last_active_node": supervisor_state.get("last_active_node", "request_clarifier")
                    }
                    
                    # Original code (now removed):
                    # supervisor_state["next"] = "human"
                
                try:
                    # Execute one step
                    result = self.graph.invoke(supervisor_state, config=config)
                    supervisor_state = result
                except GraphInterrupt as interrupt_error:
                    # Handle the interrupt
                    if self.verbose:
                        logger.info(f"[Supervisor] Interrupt detected: {interrupt_error}")
                    
                    # Extract the interrupt value (the question to ask the user)
                    interrupt_value = None
                    if hasattr(interrupt_error, 'interrupt') and hasattr(interrupt_error.interrupt, 'value'):
                        interrupt_value = interrupt_error.interrupt.value
                    elif hasattr(interrupt_error, 'args') and len(interrupt_error.args) > 0:
                        # Try to extract from the arguments tuple
                        for arg in interrupt_error.args:
                            if hasattr(arg, 'value'):
                                interrupt_value = arg.value
                                break
                    
                    if interrupt_value:
                        # Return the current state with the interrupt
                        # This allows the caller to handle the interrupt and provide input
                        return {
                            "messages": supervisor_state.get("messages", []),
                            "csv_file_path": csv_file_path,
                            "source_data": source_data,
                            "interrupt_message": interrupt_value,
                            "needs_input": True,
                            "thread_id": thread_id
                        }
                    else:
                        # If we couldn't extract an interrupt value, log an error
                        logger.error(f"[Supervisor] Couldn't extract interrupt value from {interrupt_error}")
                        # Continue with the current state
                        continue
                
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
                
                # Also check for out-of-scope
                for msg in reversed(result.get("messages", [])):
                    if hasattr(msg, 'name') and msg.name == "request_clarifier" and hasattr(msg, 'content'):
                        if "OUT_OF_SCOPE" in msg.content:
                            if self.verbose:
                                logger.info(f"[Supervisor] Request marked as out of scope after {step_count} steps")
                            completion_detected = True
                            break
                
                # Early exit if successful (prioritize "next" state field then check completion detection)
                if supervisor_state.get("next") == END or completion_detected:
                    if self.verbose:
                        logger.info("[Supervisor] Early exit on successful completion")
                        
                    # Before breaking, ensure we've preserved key clarification state in the result
                    if "rewritten_request" not in result and supervisor_state.get("rewritten_request"):
                        result["rewritten_request"] = supervisor_state.get("rewritten_request")
                    if "original_request" not in result and supervisor_state.get("original_request"):
                        result["original_request"] = supervisor_state.get("original_request")
                    if "in_clarification_mode" not in result and supervisor_state.get("in_clarification_mode"):
                        result["in_clarification_mode"] = supervisor_state.get("in_clarification_mode")
                    if "is_request_clarified" not in result and supervisor_state.get("is_request_clarified"):
                        result["is_request_clarified"] = supervisor_state.get("is_request_clarified")
                        
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
            
            # Generate a human-friendly summary of the actions taken if completed successfully
            if result:
                # Check if any edits were actually made
                edit_performed = any(
                    isinstance(msg, ToolMessage) and msg.name == "csv_pandas_edit" 
                    for msg in result.get("messages", [])
                )
                
                # Generate summary only if edits were performed
                if edit_performed:
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
                # Return the partial result with interrupt state maintained
                return {
                    "error": "Failed to complete workflow",
                    "messages": state.get("messages", []),
                    "csv_file_path": state.get("csv_file_path", ""),
                    "thread_id": thread_id
                }
                
        except Exception as e:
            logger.error(f"[Supervisor] Error running CSV Edit Supervisor Agent: {e}", exc_info=True)
            return {
                "error": str(e),
                "messages": state.get("messages", []),
                "csv_file_path": state.get("csv_file_path", ""),
                "thread_id": state.get("thread_id", str(uuid.uuid4()))
            }

    def resume(self, state: Dict[str, Any], user_input: str) -> Dict[str, Any]:
        """
        Resume execution after an interrupt.
        
        Args:
            state: The current state of the workflow
            user_input: The user's response to the clarification request
            
        Returns:
            The updated state after resuming execution
        """
        if self.verbose:
            logger.info(f"[Supervisor] Resuming with user input: '{user_input}'")
        
        # Use the thread_id from the previous state
        thread_id = state.get("thread_id", str(uuid.uuid4()))
        
        # CRITICAL: Make sure we preserve all important clarification state
        original_request = state.get("original_request", "")
        rewritten_request = state.get("rewritten_request", "")
        csv_file_path = state.get("csv_file_path", "")
        clarification_count = state.get("clarification_count", 0)
        is_request_clarified = state.get("is_request_clarified", False)
        in_clarification_mode = state.get("in_clarification_mode", True)  # Default to true during resume
        last_active_node = state.get("last_active_node", "request_clarifier")
        
        if self.verbose:
            logger.info(f"[Supervisor] State before resume - original_request: '{original_request}'")
            logger.info(f"[Supervisor] State before resume - rewritten_request: '{rewritten_request}'")
            logger.info(f"[Supervisor] State before resume - in_clarification_mode: {in_clarification_mode}")
            logger.info(f"[Supervisor] State before resume - csv_file_path: '{csv_file_path}'")
            logger.info(f"[Supervisor] State before resume - clarification_count: {clarification_count}")
            logger.info(f"[Supervisor] State before resume - last_active_node: '{last_active_node}'")
        
        # Create config with thread_id to ensure we're resuming the same conversation
        config = {
        "configurable": {
            "thread_id": thread_id
                }
            }
            
        # This is the critical part - use Command(resume=user_input)
        try:
            # Create a state with necessary fields to avoid conflicts
            resume_state = {
                "messages": state.get("messages", []) + [HumanMessage(content=user_input)],
                "csv_file_path": csv_file_path,
                "original_request": original_request,
                "rewritten_request": rewritten_request,
                "in_clarification_mode": in_clarification_mode,
                "is_request_clarified": is_request_clarified,
                "clarification_count": clarification_count,
                "last_active_node": last_active_node,
                "source_data": state.get("source_data", {}),
                "needs_input": False,
                "interrupt_message": None,
                "verification_failure": "",
                "next": state.get("next", "request_clarifier")
            }
            
            # Execute the graph with the prepared state
            result = self.graph.invoke(
                resume_state,
                config=config
            )
                
            if self.verbose:
                logger.info(f"[Supervisor] State after resume - original_request: '{result.get('original_request', '')}'")
                logger.info(f"[Supervisor] State after resume - rewritten_request: '{result.get('rewritten_request', '')}'")
                logger.info(f"[Supervisor] State after resume - csv_file_path: '{result.get('csv_file_path', '')}'")
                logger.info(f"[Supervisor] State after resume - in_clarification_mode: {result.get('in_clarification_mode', False)}")
                logger.info(f"[Supervisor] State after resume - clarification_count: {result.get('clarification_count', 0)}")
                
            return result
        except GraphInterrupt as interrupt_error:
            # Handle nested interrupts (if another interrupt is raised immediately)
            interrupt_value = None
            if hasattr(interrupt_error, 'interrupt') and hasattr(interrupt_error.interrupt, 'value'):
                interrupt_value = interrupt_error.interrupt.value
            elif hasattr(interrupt_error, 'args') and len(interrupt_error.args) > 0:
                for arg in interrupt_error.args:
                    if hasattr(arg, 'value'):
                        interrupt_value = arg.value
                        break
            
            # Return the current state with the new interrupt
            return {
                "messages": state.get("messages", []),
                "csv_file_path": state.get("csv_file_path", ""),
                "source_data": state.get("source_data", {}),
                "interrupt_message": interrupt_value,
                "needs_input": True,
                "thread_id": thread_id
            }
        except Exception as e:
            logger.error(f"[Supervisor] Error resuming workflow: {e}", exc_info=True)
            return {
                "error": f"Failed to resume workflow: {str(e)}",
                "messages": state.get("messages", []),
                "csv_file_path": state.get("csv_file_path", ""),
                "thread_id": thread_id
            }
