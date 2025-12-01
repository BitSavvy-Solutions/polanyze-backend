import azure.functions as func
import logging
import os
import json
from neo4j import GraphDatabase
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient # <--- Added this
from openai import OpenAI

# --- CONFIGURATION ---
openai_client = None
neo4j_driver = None
series_container = None
versions_container = None # <--- Added this to look up blob path
blob_service_client = None # <--- Added this

try:
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        openai_client = OpenAI(api_key=api_key)
        
    neo_uri = os.environ.get("NEO4J_URI")
    neo_user = os.environ.get("NEO4J_USERNAME")
    neo_pass = os.environ.get("NEO4J_PASSWORD")
    if neo_uri:
        neo4j_driver = GraphDatabase.driver(neo_uri, auth=(neo_user, neo_pass))

    c_endpoint = os.environ.get("COSMOS_ENDPOINT")
    c_key = os.environ.get("COSMOS_KEY")
    c_db = os.environ.get("COSMOS_DATABASE")
    
    # Blob Connection
    storage_conn_str = os.environ.get("AzureWebJobsStorage")
    if storage_conn_str:
        blob_service_client = BlobServiceClient.from_connection_string(storage_conn_str)

    if c_endpoint:
        client = CosmosClient(url=c_endpoint, credential=c_key)
        db = client.get_database_client(c_db)
        series_container = db.get_container_client("PolicySeries")
        versions_container = db.get_container_client("PolicyVersions")

except Exception as e:
    logging.error(f"Init Error: {e}")

query_bp = func.Blueprint()

def get_embedding(text):
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding

# --- NEW ROUTE: GET FULL DOCUMENT CONTENT ---
@query_bp.route(route="get_policy_content", auth_level=func.AuthLevel.ANONYMOUS)
def get_policy_content(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        series_id = req_body.get('series_id')
        
        if not series_id: 
            return func.HttpResponse("Missing series_id", status_code=400)

        # 1. Get the latest version ID from PolicySeries
        series_item = series_container.read_item(item=series_id, partition_key=series_id)
        latest_version_id = series_item.get('latest_version_id')

        # 2. Get the Blob Path from PolicyVersions
        version_item = versions_container.read_item(item=latest_version_id, partition_key=series_id)
        blob_path = version_item.get('blob_path')
        
        if not blob_path:
            return func.HttpResponse("Blob path not found", status_code=404)

        # 3. Download Text
        container_client = blob_service_client.get_container_client("policy-documents")
        blob_client = container_client.get_blob_client(blob_path)
        content = blob_client.download_blob().readall().decode('utf-8')

        return func.HttpResponse(json.dumps({"content": content}), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error fetching content: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# --- EXISTING ROUTES BELOW (Keep get_all_policies, query_policy, etc.) ---
@query_bp.route(route="get_all_policies", auth_level=func.AuthLevel.ANONYMOUS)
def get_all_policies(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if not series_container:
            return func.HttpResponse("Database connection not initialized", status_code=500)
        query = "SELECT * FROM c"
        items = list(series_container.query_items(query=query, enable_cross_partition_query=True))
        return func.HttpResponse(json.dumps(items), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# ... (Keep vector_search_general, graph_search_specific_doc functions) ...

@query_bp.route(route="query_policy", auth_level=func.AuthLevel.ANONYMOUS)
def query_policy(req: func.HttpRequest) -> func.HttpResponse:
    # ... (Keep existing logic) ...
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
    # ... (Keep existing logic) ...
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