import azure.functions as func
import logging
import os
import json
from neo4j import GraphDatabase
from openai import OpenAI

# --- CONFIGURATION ---
openai_client = None
neo4j_driver = None

try:
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        openai_client = OpenAI(api_key=api_key)
        
    neo_uri = os.environ.get("NEO4J_URI")
    neo_user = os.environ.get("NEO4J_USERNAME")
    neo_pass = os.environ.get("NEO4J_PASSWORD")
    if neo_uri:
        neo4j_driver = GraphDatabase.driver(neo_uri, auth=(neo_user, neo_pass))
except Exception as e:
    logging.error(f"Init Error: {e}")

query_bp = func.Blueprint()

def get_embedding(text):
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding

def vector_search(tx, question_vector, limit=5):
    # Query the Vector Index
    # Note: You must create the index 'chunk_embeddings' in Neo4j first!
    query = """
    CALL db.index.vector.queryNodes('chunk_embeddings', $limit, $embedding)
    YIELD node AS chunk, score
    MATCH (p:Policy)-[:HAS_CHUNK]->(chunk)
    RETURN 
        p.title AS policy_title, 
        p.id AS policy_id, 
        p.sector AS sector,
        chunk.content AS text, 
        score
    """
    result = tx.run(query, embedding=question_vector, limit=limit)
    return [record.data() for record in result]

@query_bp.route(route="query_policy", auth_level=func.AuthLevel.ANONYMOUS)
def query_policy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Query Policy triggered.')

    try:
        req_body = req.get_json()
        question = req_body.get('question')

        if not question:
            return func.HttpResponse("Missing 'question' in body", status_code=400)

        if not openai_client or not neo4j_driver:
             return func.HttpResponse("Database or OpenAI not configured", status_code=500)

        # 1. Embed the Question
        q_vector = get_embedding(question)

        # 2. Search Neo4j
        results = []
        with neo4j_driver.session() as session:
            results = session.execute_read(vector_search, question_vector=q_vector)

        # 3. Format Response
        # Group by Policy to show which documents are most relevant
        formatted_results = []
        for r in results:
            formatted_results.append({
                "document_title": r['policy_title'],
                "sector": r.get('sector', 'N/A'),
                "relevance_score": r['score'],
                "snippet": r['text']
            })

        return func.HttpResponse(
            json.dumps({"matches": formatted_results}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)