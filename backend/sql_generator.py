from openai import AzureOpenAI
import os

client = AzureOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version="2024-12-01-preview"
)

DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

def generate_sql_gemini(user_query, schema, table_name):
    prompt = f"""
You are an SQL expert. Convert the user request into a valid SQL query ONLY.

TABLE NAME: {table_name}
COLUMNS: {schema}

RULES:
- Only use the table `{table_name}`
- Do not guess or rename columns
- Return ONLY SQL.
User Query: {user_query}
"""

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=[
            {"role": "system", "content": "You are a strict SQL generator."},
            {"role": "user", "content": prompt}
        ]
    )

    sql = response.choices[0].message.content.strip()
    return sql.replace("```sql", "").replace("```", "").strip()
