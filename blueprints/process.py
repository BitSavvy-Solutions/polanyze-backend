import azure.functions as func
import logging
import os
import json
from datetime import datetime
from azure.cosmos import CosmosClient
from neo4j import GraphDatabase

# --- CONFIGURATION ---
# Initialize DB connections (Cosmos + Neo4j)
series_container = None
versions_container = None
neo4j_driver = None

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

except Exception as e:
    logging.error(f"❌ DB Init Error: {e}")

process_bp = func.Blueprint()

# Neo4j Logic
def add_policy_to_graph(tx, series_id, title, country, entity, category):
    query = """
    MERGE (p:Policy {id: $series_id})
    SET p.title = $title, p.last_updated = datetime()
    MERGE (c:Country {name: $country})
    MERGE (e:Entity {name: $entity})
    MERGE (cat:Category {name: $category})
    MERGE (p)-[:APPLIES_TO]->(c)
    MERGE (p)-[:ISSUED_BY]->(e)
    MERGE (p)-[:BELONGS_TO]->(cat)
    """
    tx.run(query, series_id=series_id, title=title, country=country, entity=entity, category=category)

# --- THE QUEUE TRIGGER ---
# This function runs automatically when a message hits "policy-ingest-queue"
@process_bp.queue_trigger(arg_name="msg", queue_name="policy-ingest-queue", connection="AzureWebJobsStorage")
def process_ingestion(msg: func.QueueMessage):
    logging.info('2. Worker triggered by Queue.')

    try:
        # 1. Parse the Queue Message
        body_json = msg.get_body().decode('utf-8')
        payload = json.loads(body_json)
        
        series_id = payload['series_id']
        version_id = payload['version_id']
        data = payload['data'] # The original request body

        title = data.get('title')
        country = data.get('country')
        entity = data.get('entity')
        category = data.get('category', 'General')

        logging.info(f"Processing: {title}")

        # 2. Update Neo4j (The Graph)
        if neo4j_driver:
            with neo4j_driver.session() as session:
                session.execute_write(
                    add_policy_to_graph, 
                    series_id=series_id, 
                    title=title, 
                    country=country, 
                    entity=entity,
                    category=category
                )
            logging.info("✅ Neo4j Updated")

        # 3. Update Cosmos DB (The Document)
        # We upsert the Series and Update the Version status
        if series_container and versions_container:
            # Upsert Series
            series_item = {
                "id": series_id,
                "title": title,
                "country": country,
                "entity": entity,
                "category": category,
                "latest_version_id": version_id,
                "last_updated": datetime.utcnow().isoformat()
            }
            series_container.upsert_item(body=series_item)

            # Update Version Status to "Ingested"
            # We read the item first to ensure we don't overwrite other fields
            version_item = versions_container.read_item(item=version_id, partition_key=series_id)
            version_item['status'] = "Ingested"
            version_item['processed_at'] = datetime.utcnow().isoformat()
            versions_container.upsert_item(body=version_item)
            
            logging.info("✅ Cosmos DB Updated")

    except Exception as e:
        logging.error(f"❌ Processing Failed: {e}")
        # If this fails, the message usually goes back to the queue to retry
        raise e 