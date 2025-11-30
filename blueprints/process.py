import azure.functions as func
import logging
import os
import json
from datetime import datetime
from azure.cosmos import CosmosClient
from neo4j import GraphDatabase
from openai import OpenAI

# --- CONFIGURATION ---
series_container = None
versions_container = None
neo4j_driver = None
openai_client = None

try:
    # Cosmos
    c_endpoint = os.environ.get("COSMOS_ENDPOINT")
    c_key = os.environ.get("COSMOS_KEY")
    c_db = os.environ.get("COSMOS_DATABASE")
    if c_endpoint:
        client = CosmosClient(url=c_endpoint, credential=c_key)
        db = client.get_database_client(c_db)
        series_container = db.get_container_client("PolicySeries")
        versions_container = db.get_container_client("PolicyVersions")

    # Neo4j
    neo_uri = os.environ.get("NEO4J_URI")
    neo_user = os.environ.get("NEO4J_USERNAME")
    neo_pass = os.environ.get("NEO4J_PASSWORD")
    if neo_uri:
        neo4j_driver = GraphDatabase.driver(neo_uri, auth=(neo_user, neo_pass))

    # OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        openai_client = OpenAI(api_key=api_key)

except Exception as e:
    logging.error(f"❌ Init Error: {e}")

process_bp = func.Blueprint()

# --- HELPERS ---
def get_embedding(text):
    """Generates vector embedding using OpenAI"""
    if not openai_client:
        logging.error("OpenAI Client not initialized")
        return []
    
    # Clean newlines to improve embedding quality
    text = text.replace("\n", " ")
    response = openai_client.embeddings.create(
        input=text,
        model="text-embedding-3-small"
    )
    return response.data[0].embedding

def chunk_text(text, chunk_size=1000, overlap=100):
    """Splits text into overlapping chunks"""
    if not text:
        return []
    
    chunks = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = start + chunk_size
        chunks.append(text[start:end])
        start += (chunk_size - overlap)
    
    return chunks

# --- NEO4J LOGIC ---
def add_policy_and_chunks(tx, series_id, title, country, entity, sector, province, chunks_data):
    # 1. Merge the Policy Node (Metadata)
    query_policy = """
    MERGE (p:Policy {id: $series_id})
    SET p.title = $title, 
        p.country = $country,
        p.entity = $entity,
        p.sector = $sector,
        p.province = $province,
        p.last_updated = datetime()
    """
    tx.run(query_policy, series_id=series_id, title=title, country=country, entity=entity, sector=sector, province=province)

    # 2. Create Chunks and Link them
    # We use UNWIND to batch insert chunks efficiently
    query_chunks = """
    MATCH (p:Policy {id: $series_id})
    UNWIND $chunks AS chunk_data
    CREATE (c:Chunk {
        content: chunk_data.text,
        chunk_index: chunk_data.index,
        embedding: chunk_data.vector
    })
    MERGE (p)-[:HAS_CHUNK]->(c)
    """
    tx.run(query_chunks, series_id=series_id, chunks=chunks_data)

# --- THE QUEUE TRIGGER ---
@process_bp.queue_trigger(arg_name="msg", queue_name="policy-ingest-queue", connection="AzureWebJobsStorage")
def process_ingestion(msg: func.QueueMessage):
    logging.info('2. Worker triggered by Queue.')

    try:
        # 1. Parse Message
        body_json = msg.get_body().decode('utf-8')
        payload = json.loads(body_json)
        
        series_id = payload['series_id']
        version_id = payload['version_id']
        data = payload['data']

        title = data.get('title')
        text_content = data.get('text_content', '')
        
        logging.info(f"Processing: {title} (Length: {len(text_content)})")

        # 2. Chunk & Embed
        raw_chunks = chunk_text(text_content)
        logging.info(f"Generated {len(raw_chunks)} chunks.")

        chunks_payload = []
        for i, chunk_text_str in enumerate(raw_chunks):
            vector = get_embedding(chunk_text_str)
            chunks_payload.append({
                "index": i,
                "text": chunk_text_str,
                "vector": vector
            })

        # 3. Update Neo4j
        if neo4j_driver:
            with neo4j_driver.session() as session:
                session.execute_write(
                    add_policy_and_chunks, 
                    series_id=series_id, 
                    title=title, 
                    country=data.get('country'), 
                    entity=data.get('entity'),
                    sector=data.get('sector'),
                    province=data.get('province'),
                    chunks_data=chunks_payload
                )
            logging.info("✅ Neo4j Updated with Chunks")

        # 4. Update Cosmos DB Status
        if series_container and versions_container:
            # Upsert Series Metadata
            series_item = {
                "id": series_id,
                "title": title,
                "country": data.get('country'),
                "entity": data.get('entity'),
                "latest_version_id": version_id,
                "last_updated": datetime.utcnow().isoformat()
            }
            series_container.upsert_item(body=series_item)

            # Update Version Status
            try:
                version_item = versions_container.read_item(item=version_id, partition_key=series_id)
                version_item['status'] = "Ingested"
                version_item['processed_at'] = datetime.utcnow().isoformat()
                versions_container.upsert_item(body=version_item)
            except Exception as e:
                logging.warning(f"Could not update Cosmos version status: {e}")
            
            logging.info("✅ Cosmos DB Updated")

    except Exception as e:
        logging.error(f"❌ Processing Failed: {e}")
        raise e 