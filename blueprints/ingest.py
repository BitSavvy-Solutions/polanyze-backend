# ... imports ...
# (Keep existing imports and config)
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
from azure.storage.blob import BlobServiceClient

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
        if not req_body: return func.HttpResponse("Empty Body", status_code=400)

        # Extract Fields
        title = req_body.get('title')
        country = req_body.get('country')
        entity = req_body.get('entity')
        text_content = req_body.get('text_content')
        sector = req_body.get('sector', 'General')
        province = req_body.get('province', 'N/A')
        
        # --- UPDATE LOGIC: Check if series_id is provided ---
        existing_series_id = req_body.get('series_id')

        if not all([title, country, entity, text_content]):
            return func.HttpResponse("Missing required fields", status_code=400)

        # If updating, use existing ID. If new, generate ID.
        if existing_series_id:
            series_id = existing_series_id
            logging.info(f"Updating existing policy: {series_id}")
        else:
            series_id = generate_series_id(country, entity, title)
            logging.info(f"Creating new policy: {series_id}")

        version_id = str(uuid.uuid4())

        # --- 1. UPLOAD TO BLOB STORAGE (Handle 5MB+ files) ---
        conn_str = os.environ.get("AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        
        container_name = "policy-documents"
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            container_client.create_container()

        # Upload text
        blob_name = f"{series_id}/{version_id}.txt"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(text_content, overwrite=True)
        logging.info(f"Uploaded text to Blob: {blob_name}")

        # --- 2. SAVE METADATA TO COSMOS ---
        if versions_container:
            version_item = {
                "id": version_id,
                "series_id": series_id,
                "status": "Queued",
                "ingested_at": datetime.utcnow().isoformat(),
                "metadata": { "sector": sector, "province": province, "title": title },
                "blob_path": blob_name
            }
            versions_container.create_item(body=version_item)

        # --- 3. SEND CLAIM CHECK TO QUEUE ---
        queue_client = QueueClient.from_connection_string(conn_str, "policy-ingest-queue")
        try: queue_client.create_queue()
        except: pass 

        message_payload = {
            "series_id": series_id,
            "version_id": version_id,
            "data": {
                "title": title,
                "country": country,
                "entity": entity,
                "sector": sector,
                "province": province,
                "container": container_name,
                "blob_name": blob_name
            }
        }
        
        msg_str = json.dumps(message_payload)
        queue_client.send_message(base64.b64encode(msg_str.encode('utf-8')).decode('utf-8'))
        logging.info(f"✅ Message sent to queue for {series_id}")

        return func.HttpResponse(json.dumps({"status": "Queued", "id": series_id}), status_code=202)

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Server Error: {str(e)}", status_code=500)