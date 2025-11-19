from fastapi import FastAPI, File, UploadFile, Form
import os
from backend.langagent import graph_agent
from backend.upload_utils import upload_new_table

UPLOAD_DIR = "uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="Conversational SQL Agent")

@app.post("/upload_new_table")
async def upload_new_table_api(
    file: UploadFile = File(...), 
    table_name: str = Form(...)
):
    try:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())

        result = upload_new_table(file_path, table_name)
        return {"status": "success", "message": result}

    except Exception as e:
        return {"status": "error", "message": f"Upload failed: {str(e)}"}


conversation_history = {}

@app.post("/ask_graph_agent")
def ask_graph_agent(user_query: str, session_id: str = "default"):
    try:
        previous_conversation = conversation_history.get(session_id, [])

        memory_text = "\n".join([
            f"User: {turn['user']}\nAI: {turn['bot']}"
            for turn in previous_conversation
        ])

        state = {"query": user_query, "memory": memory_text}

        result = graph_agent.invoke(state)
        answer = result.get("answer", "No response generated.")

        previous_conversation.append({"user": user_query, "bot": answer})
        conversation_history[session_id] = previous_conversation

        return {
            "response": answer,
            "conversation": previous_conversation
        }

    except Exception as e:
        return {"error": str(e)}
