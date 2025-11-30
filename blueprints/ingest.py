import azure.functions as func
import logging
import os
import json
import uuid
import hashlib
import base64
from datetime import datetime
from azure.cosmos import CosmosClient
from azure.storage.queue import QueueClient

# --- CONFIGURATION ---
series_container = None
versions_container = None

try:
    c_endpoint = os.environ.get("COSMOS_ENDPOINT")
    c_key = os.environ.get("COSMOS_KEY")
    c_db = os.environ.get("COSMOS_DATABASE")
    
    if c_endpoint and c_key:
        client = CosmosClient(url=c_endpoint, credential=c_key)
        db = client.get_database_client(c_db)
        series_container = db.get_container_client("PolicySeries")
        versions_container = db.get_container_client("PolicyVersions")
except Exception as e:
    logging.error(f"❌ Cosmos Init Error: {e}")

ingest_bp = func.Blueprint()

def generate_series_id(country, entity, title):
    raw = f"{country}-{entity}-{title}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()

@ingest_bp.route(route="ingest_policy", auth_level=func.AuthLevel.ANONYMOUS)
def ingest_policy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('1. Ingest Request Received.')

    try:
        req_body = req.get_json()
        
        if not req_body:
             return func.HttpResponse("Empty Body", status_code=400)

        # Extract Fields
        title = req_body.get('title')
        country = req_body.get('country')
        entity = req_body.get('entity') # e.g. "Federal", "Provincial"
        text_content = req_body.get('text_content') # The raw text of the policy
        
        # New B2B Fields
        sector = req_body.get('sector', 'General')
        province = req_body.get('province', 'N/A')
        
        if not all([title, country, entity, text_content]):
            return func.HttpResponse("Missing required fields: title, country, entity, or text_content", status_code=400)

        # Generate IDs
        series_id = generate_series_id(country, entity, title)
        version_id = str(uuid.uuid4())

        # --- 1. SAVE TO COSMOS (Metadata & Status) ---
        if versions_container:
            version_item = {
                "id": version_id,
                "series_id": series_id,
                "status": "Queued",
                "ingested_at": datetime.utcnow().isoformat(),
                "metadata": {
                    "sector": sector,
                    "province": province,
                    "title": title
                }
                # Note: We are NOT saving text_content to Cosmos to save money/space, 
                # but you can add it here if you want a backup.
            }
            versions_container.create_item(body=version_item)

        # --- 2. SEND TO QUEUE ---
        try:
            conn_str = os.environ.get("AzureWebJobsStorage")
            if not conn_str:
                raise Exception("AzureWebJobsStorage environment variable is missing")

            queue_client = QueueClient.from_connection_string(conn_str, "policy-ingest-queue")
            
            try:
                queue_client.create_queue()
            except:
                pass 

            # Prepare Message (Includes the heavy text content)
            message_payload = {
                "series_id": series_id,
                "version_id": version_id,
                "data": {
                    "title": title,
                    "country": country,
                    "entity": entity,
                    "sector": sector,
                    "province": province,
                    "text_content": text_content
                }
            }
            
            message_string = json.dumps(message_payload)
            message_bytes = message_string.encode('utf-8')
            
            queue_client.send_message(base64.b64encode(message_bytes).decode('utf-8'))
            logging.info(f"✅ Message sent to queue for {series_id}")

        except Exception as q_error:
            logging.error(f"❌ Queue Error: {q_error}")
            return func.HttpResponse(f"Queue Error: {str(q_error)}", status_code=500)

        return func.HttpResponse(
            json.dumps({
                "message": "Ingestion initiated.", 
                "status": "Queued",
                "id": series_id
            }),
            status_code=202
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Server Error: {str(e)}", status_code=500)