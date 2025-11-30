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

# --- 1. GENERAL SEARCH ---
def vector_search_general(tx, question_vector, limit=5):
    query = """
    CALL db.index.vector.queryNodes('chunk_embeddings', $limit, $embedding)
    YIELD node AS chunk, score
    MATCH (p:Policy)-[:HAS_CHUNK]->(chunk)
    RETURN p.title AS policy_title, p.id AS policy_id, chunk.content AS text, score
    """
    result = tx.run(query, embedding=question_vector, limit=limit)
    return [record.data() for record in result]

# --- 2. DEEP DIVE (Graph + Ontology) ---
def graph_search_specific_doc(tx, doc_id, question_vector):
    query = """
    CALL db.index.vector.queryNodes('chunk_embeddings', 10, $embedding)
    YIELD node AS chunk, score
    
    MATCH (p:Policy {id: $doc_id})-[:HAS_CHUNK]->(chunk)
    
    // Traverse Graph for Context
    MATCH (section:Section)-[:CONTAINS]->(chunk)
    OPTIONAL MATCH (chunk)-[:AFFECTS]->(subject:Entity)
    OPTIONAL MATCH (chunk)-[:MENTIONS]->(topic:Topic)
    
    RETURN 
        section.title AS section_title,
        chunk.legal_type AS legal_type,
        subject.name AS subject,
        chunk.content AS text,
        collect(topic.name) AS related_topics,
        score
    ORDER BY score DESC
    LIMIT 3
    """
    result = tx.run(query, doc_id=doc_id, embedding=question_vector)
    return [record.data() for record in result]

@query_bp.route(route="query_policy", auth_level=func.AuthLevel.ANONYMOUS)
def query_policy(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        question = req_body.get('question')
        if not question: return func.HttpResponse("Missing question", status_code=400)

        q_vector = get_embedding(question)
        
        results = []
        with neo4j_driver.session() as session:
            results = session.execute_read(vector_search_general, q_vector)

        unique_docs = {}
        for r in results:
            doc_id = r['policy_id']
            if doc_id not in unique_docs:
                unique_docs[doc_id] = {
                    "title": r['policy_title'],
                    "id": doc_id,
                    "best_match_snippet": r['text'],
                    "score": r['score']
                }

        return func.HttpResponse(json.dumps({"matches": list(unique_docs.values())}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

@query_bp.route(route="query_document_details", auth_level=func.AuthLevel.ANONYMOUS)
def query_document_details(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        question = req_body.get('question')
        doc_id = req_body.get('doc_id')
        
        if not question or not doc_id: return func.HttpResponse("Missing question or doc_id", status_code=400)

        q_vector = get_embedding(question)
        
        results = []
        with neo4j_driver.session() as session:
            results = session.execute_read(graph_search_specific_doc, doc_id, q_vector)

        return func.HttpResponse(json.dumps({"analysis": results}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)