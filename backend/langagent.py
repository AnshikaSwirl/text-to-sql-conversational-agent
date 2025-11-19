from langgraph.graph import StateGraph, START, END
from typing import TypedDict, Optional

from backend.sql_generator import generate_sql_gemini
from backend.database import get_table_schema, get_all_table_names
from backend.executer import run_query

from openai import AzureOpenAI
import os


# Azure OpenAI client (env vars only — required for Azure deployment)
client = AzureOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version="2024-12-01-preview",
)

DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")


# ---------- STATE DEFINITION ----------
class AgentState(TypedDict, total=False):
    query: str                           # User query
    table_name: Optional[str]            # Selected table
    sql: Optional[str]                   # Generated SQL query
    result: Optional[str]                # SQL execution result
    answer: Optional[str]                # Final summarized answer
    memory: Optional[str]                # Full conversation memory
    filters: Optional[str]               # WHERE clause tracking
    last_action: Optional[str]           # For conversational follow-ups
    last_group: Optional[str]            # Last grouping column used



# ---------- STEP 1: IDENTIFY BEST TABLE ----------
def identify_table_step(state: AgentState) -> AgentState:
    print(f"Identifying best table for query: {state['query']}")

    # If table was chosen earlier, reuse it
    if state.get("table_name"):
        print(f"Reusing table: {state['table_name']}")
        return state

    # Fetch all table names + schemas
    all_tables = get_all_table_names()
    table_schemas = {t: get_table_schema(t) for t in all_tables}

    schema_text = "\n\n".join(
        [f"Table: {t}\nColumns: {table_schemas[t]}" for t in table_schemas]
    )

    # Ask LLM to pick the best table
    prompt = f"""
You are a data expert.
Given the user's query and available tables, choose the ONE table that best matches the query.

User Query: {state['query']}
Available Tables and Schemas:
{schema_text}

Return only the table name (no explanation).
"""

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[
            {"role": "system", "content": "You are an intelligent SQL table selector."},
            {"role": "user", "content": prompt},
        ],
    )

    chosen_table = response.choices[0].message.content.strip()
    print(f"Selected table: {chosen_table}")

    state["table_name"] = chosen_table
    return state



# ---------- STEP 2: GENERATE SQL QUERY ----------
def generate_sql_step(state: AgentState) -> AgentState:
    print(f"Generating SQL for table: {state['table_name']}")

    schema = get_table_schema(state["table_name"])

    # --- Follow-up conversation shortcuts ---
    # 1. Gender switch
    if "male" in state["query"].lower() and state.get("filters"):
        new_filters = state["filters"].replace("Female", "Male")
        sql = f"SELECT * FROM {state['table_name']} WHERE {new_filters};"
        state["filters"] = new_filters
        state["last_action"] = "select_records"

    # 2. Breakdown / group-by
    elif "break down" in state["query"].lower() and state.get("filters"):
        if "userage" in state["query"].lower():
            group_col = "userage"
        elif "usergender" in state["query"].lower():
            group_col = "usergender"
        else:
            group_col = state.get("last_group", "usergender")

        sql = f"""
        SELECT {group_col},
               COUNT(*) AS review_count,
               AVG(reviewrating) AS avg_rating,
               MIN(reviewrating) AS min_rating,
               MAX(reviewrating) AS max_rating
        FROM {state['table_name']}
        WHERE {state['filters']}
        GROUP BY {group_col}
        ORDER BY review_count DESC;
        """

        state["last_action"] = "breakdown"
        state["last_group"] = group_col

    # 3. Normal LLM SQL generation
    else:
        enriched_query = f"""
Conversation so far:
{state.get('memory', '')}

User Query: {state['query']}
"""
        sql = generate_sql_gemini(enriched_query, schema, state["table_name"])

        # Attach previous filters if missing
        if state.get("filters") and "WHERE" not in sql.upper():
            sql = sql.strip().rstrip(";")
            sql += f"\nWHERE {state['filters']};"

    print(f"SQL Generated: {sql}")
    state["sql"] = sql
    return state



# ---------- STEP 3: EXECUTE SQL ON AZURE SQL ----------
def execute_sql_step(state: AgentState) -> AgentState:
    print("Executing SQL query...")

    result = run_query(state["sql"])
    print("Query executed successfully.")

    state["result"] = str(result)

    # Capture WHERE clause for conversation follow-ups
    if "WHERE" in state["sql"].upper():
        where_clause = state["sql"].split("WHERE", 1)[1]
        where_clause = (
            where_clause.split("GROUP BY")[0]
            .split("ORDER BY")[0]
            .strip()
        )
        state["filters"] = where_clause
        print(f"Captured filters: {state['filters']}")

    return state



# ---------- STEP 4: SUMMARIZE RESULTS ----------
def summarize_step(state: AgentState) -> AgentState:
    print("Summarizing results...")

    summary_prompt = f"""
Conversation Memory:
{state.get('memory', 'None')}

User Query: {state['query']}
SQL Result: {state['result']}
Table: {state['table_name']}

Write a clear, detailed answer in full sentences.
- Only describe the SQL result.
- Do not invent columns or data.
- Include comparisons or percentages when relevant.
"""

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[
            {"role": "system", "content": "You are a helpful data analyst."},
            {"role": "user", "content": summary_prompt},
        ],
    )

    answer = response.choices[0].message.content.strip()
    print("Final Answer:", answer)

    # Update conversation memory
    memory = state.get("memory", "")
    state["memory"] = f"{memory}\nUser: {state['query']}\nAI: {answer}\n"
    state["answer"] = answer

    return state



# ---------- BUILD LANGGRAPH WORKFLOW ----------
agent_graph = StateGraph(AgentState)

agent_graph.add_node("identify_table", identify_table_step)
agent_graph.add_node("generate_sql", generate_sql_step)
agent_graph.add_node("execute_sql", execute_sql_step)
agent_graph.add_node("summarize", summarize_step)

agent_graph.add_edge(START, "identify_table")
agent_graph.add_edge("identify_table", "generate_sql")
agent_graph.add_edge("generate_sql", "execute_sql")
agent_graph.add_edge("execute_sql", "summarize")
agent_graph.add_edge("summarize", END)

graph_agent = agent_graph.compile()
