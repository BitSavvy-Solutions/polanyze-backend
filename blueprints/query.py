import azure.functions as func
import logging
import os
import json
from neo4j import GraphDatabase
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from openai import OpenAI

# --- CONFIGURATION ---
openai_client = None
neo4j_driver = None
series_container = None
versions_container = None
blob_service_client = None

try:
    # OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        openai_client = OpenAI(api_key=api_key)
        
    # Neo4j
    neo_uri = os.environ.get("NEO4J_URI")
    neo_user = os.environ.get("NEO4J_USERNAME")
    neo_pass = os.environ.get("NEO4J_PASSWORD")
    if neo_uri:
        neo4j_driver = GraphDatabase.driver(neo_uri, auth=(neo_user, neo_pass))

    # Cosmos DB
    c_endpoint = os.environ.get("COSMOS_ENDPOINT")
    c_key = os.environ.get("COSMOS_KEY")
    c_db = os.environ.get("COSMOS_DATABASE")
    
    if c_endpoint:
        client = CosmosClient(url=c_endpoint, credential=c_key)
        db = client.get_database_client(c_db)
        series_container = db.get_container_client("PolicySeries")
        versions_container = db.get_container_client("PolicyVersions")

    # Blob Storage
    storage_conn_str = os.environ.get("AzureWebJobsStorage")
    if storage_conn_str:
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)

except Exception as e:
    logging.error(f"Init Error: {e}")

query_bp = func.Blueprint()

# --- HELPER FUNCTIONS ---

def get_embedding(text):
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding

def vector_search_general(tx, question_vector, limit=5):
    """
    Searches across ALL policies using vector similarity.
    """
    query = """
    CALL db.index.vector.queryNodes('chunk_embeddings', $limit, $embedding)
    YIELD node AS chunk, score
    MATCH (p:Policy)-[:HAS_CHUNK]->(chunk)
    RETURN p.title AS policy_title, p.id AS policy_id, chunk.content AS text, score
    """
    result = tx.run(query, embedding=question_vector, limit=limit)
    return [record.data() for record in result]

def graph_search_specific_doc(tx, doc_id, question_vector):
    """
    Searches WITHIN a specific policy, retrieving graph context (Section, Subject, Topics).
    """
    query = """
    CALL db.index.vector.queryNodes('chunk_embeddings', 10, $embedding)
    YIELD node AS chunk, score
    
    // Filter to ensure chunk belongs to the specific Policy ID
    MATCH (p:Policy {id: $doc_id})-[:HAS_CHUNK]->(chunk)
    
    // Traverse Graph for Context
    OPTIONAL MATCH (section:Section)-[:CONTAINS]->(chunk)
    OPTIONAL MATCH (chunk)-[:AFFECTS]->(subject:Entity)
    OPTIONAL MATCH (chunk)-[:MENTIONS]->(topic:Topic)
    
    RETURN 
        COALESCE(section.title, 'General') AS section_title,
        chunk.legal_type AS legal_type,
        COALESCE(subject.name, 'Unknown') AS subject,
        chunk.content AS text,
        collect(topic.name) AS related_topics,
        score
    ORDER BY score DESC
    LIMIT 3
    """
    result = tx.run(query, doc_id=doc_id, embedding=question_vector)
    return [record.data() for record in result]

# --- ROUTES ---

@query_bp.route(route="get_all_policies", auth_level=func.AuthLevel.ANONYMOUS)
def get_all_policies(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if not series_container:
            return func.HttpResponse("Database connection not initialized", status_code=500)

        query = "SELECT * FROM c"
        items = list(series_container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))

        return func.HttpResponse(json.dumps(items), mimetype="application/json")
    except Exception as e:
        logging.error(f"Error fetching policies: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

@query_bp.route(route="get_policy_content", auth_level=func.AuthLevel.ANONYMOUS)
def get_policy_content(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        series_id = req_body.get('series_id')
        
        if not series_id: 
            return func.HttpResponse("Missing series_id", status_code=400)

        # 1. Find Series Item (Robust Query)
        query = "SELECT * FROM c WHERE c.id = @id"
        parameters = [{"name": "@id", "value": series_id}]
        
        series_items = list(series_container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        if not series_items:
            return func.HttpResponse(f"Series ID {series_id} not found", status_code=404)
            
        series_item = series_items[0]
        latest_version_id = series_item.get('latest_version_id')

        if not latest_version_id:
             return func.HttpResponse("No version ID found", status_code=404)

        # 2. Find Version Item (Robust Query)
        v_query = "SELECT * FROM c WHERE c.id = @id"
        v_parameters = [{"name": "@id", "value": latest_version_id}]
        
        version_items = list(versions_container.query_items(
            query=v_query,
            parameters=v_parameters,
            enable_cross_partition_query=True
        ))

        if not version_items:
            return func.HttpResponse(f"Version ID {latest_version_id} not found", status_code=404)

        blob_path = version_items[0].get('blob_path')
        
        if not blob_path:
            return func.HttpResponse("Blob path not found", status_code=404)

        # 3. Download from Blob
        container_client = blob_service_client.get_container_client("policy-documents")
        blob_client = container_client.get_blob_client(blob_path)
        
        if not blob_client.exists():
             return func.HttpResponse("Blob file does not exist", status_code=404)

        content = blob_client.download_blob().readall().decode('utf-8')

        return func.HttpResponse(json.dumps({"content": content}), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error fetching content: {e}")
        return func.HttpResponse(f"Server Error: {str(e)}", status_code=500)

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
            # This calls the function defined above
            results = session.execute_read(graph_search_specific_doc, doc_id, q_vector)

        return func.HttpResponse(json.dumps({"analysis": results}), mimetype="application/json")
    except Exception as e:
        logging.error(f"Query Error: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)