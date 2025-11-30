import azure.functions as func
import logging
import os
import json
from datetime import datetime
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobClient
from neo4j import GraphDatabase
from openai import OpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from concurrent.futures import ThreadPoolExecutor

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

# --- 1. LEGAL CHUNKING ---
def smart_legal_chunking(text):
    """
    Splits text using standard legal separators.
    Chunk size increased to 2000 to reduce API calls for large docs.
    """
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=200,
        length_function=len,
        is_separator_regex=True,
        separators=[
            r"\n\nArticle \d+",    
            r"\n\nSection \d+",    
            r"\n\n§ \d+",          
            r"\n\n\d+\.",          
            r"\n\n[A-Z\s]{5,}\n",  
            r"\n\n",               
            r"\n",                 
            " "                    
        ]
    )
    return text_splitter.split_text(text)

# --- 2. ONTOLOGY EXTRACTION (GPT-5 mini) ---
def process_single_chunk(chunk_data):
    """
    Helper function to process a single chunk (Embed + Extract).
    Used for parallel processing.
    """
    index, text = chunk_data
    
    # A. Embedding
    text_clean = text.replace("\n", " ")
    emb_resp = openai_client.embeddings.create(input=text_clean, model="text-embedding-3-small")
    vector = emb_resp.data[0].embedding

    # B. Ontology Extraction (Using 'gpt-4o-mini' as the current 'mini' model)
    system_prompt = """
    You are a Legal Ontology Extractor. Analyze the text chunk and map it to this schema:
    
    1. **section_id**: The specific clause number (e.g., "Section 10.1", "Article 5"). If no clear section ID is found, use "General".
    2. **legal_type**: Classify the text as one of: ['Obligation', 'Right', 'Prohibition', 'Definition', 'Exemption', 'General'].
       - Obligation: Describes something that MUST be done.
       - Prohibition: Describes something that MUST NOT be done.
       - Right: Describes something an entity IS ALLOWED to do or receive.
       - Definition: Provides the meaning of a term.
       - Exemption: Describes a situation where a rule does not apply.
       - General: For any other type or if classification is unclear.
    3. **subject**: The primary entity this applies to (e.g., "Employer", "Taxpayer", "Vendor", "Employee"). If unclear, use "Unknown".
    4. **topics**: List of 2-3 key legal concepts (e.g., "Liability", "Data Retention", "Tax Compliance"). If no clear topics, use an empty list.

    Return JSON only. Ensure all fields are present and `section_id` is never null or empty.
    """
    
    try:
        chat_resp = openai_client.chat.completions.create(
            model="gpt-4o-mini", # <--- This is your "GPT-5 mini" equivalent
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            response_format={"type": "json_object"}
        )
        metadata = json.loads(chat_resp.choices[0].message.content)
        
        # --- CRITICAL FIX: Ensure section_id is never null/empty ---
        if not metadata.get('section_id') or metadata.get('section_id').strip() == "":
            metadata['section_id'] = "General"
        if not metadata.get('legal_type'):
            metadata['legal_type'] = "General"
        if not metadata.get('subject'):
            metadata['subject'] = "Unknown"
        if not metadata.get('topics'):
            metadata['topics'] = []
        
    except Exception as e:
        logging.warning(f"LLM Extraction failed for chunk {index}: {e}. Using defaults.")
        metadata = {"section_id": "General", "legal_type": "General", "subject": "Unknown", "topics": []}

    return {
        "index": index,
        "text": text,
        "vector": vector,
        "metadata": metadata
    }

# --- 3. GRAPH BUILDER ---
def add_legal_graph(tx, series_id, title, country, entity, sector, province, chunks_data):
    # Merge Policy
    tx.run("""
        MERGE (p:Policy {id: $id}) 
        SET p.title = $title, p.country = $country, p.sector = $sector, p.province = $province
        """, id=series_id, title=title, country=country, sector=sector, province=province)

    # Batch Insert Chunks
    query = """
    MATCH (p:Policy {id: $series_id})
    UNWIND $batch AS item
    
    CREATE (c:Chunk {
        content: item.text,
        embedding: item.vector,
        index: item.index,
        legal_type: item.metadata.legal_type
    })
    MERGE (p)-[:HAS_CHUNK]->(c)

    // --- CRITICAL FIX: Use COALESCE for section_id ---
    // This ensures that if item.metadata.section_id is null/empty, it defaults to "General"
    MERGE (s:Section {title: COALESCE(item.metadata.section_id, 'General'), policy_id: $series_id})
    MERGE (p)-[:HAS_SECTION]->(s)
    MERGE (s)-[:CONTAINS]->(c)

    MERGE (sub:Entity {name: item.metadata.subject})
    MERGE (c)-[:AFFECTS]->(sub)

    FOREACH (topic_name IN item.metadata.topics |
        MERGE (t:Topic {name: topic_name})
        MERGE (c)-[:MENTIONS]->(t)
    )
    """
    tx.run(query, series_id=series_id, batch=chunks_data)

@process_bp.queue_trigger(arg_name="msg", queue_name="policy-ingest-queue", connection="AzureWebJobsStorage")
def process_ingestion(msg: func.QueueMessage):
    logging.info('2. Worker triggered.')
    try:
        body_json = msg.get_body().decode('utf-8')
        payload = json.loads(body_json)
        series_id = payload['series_id']
        version_id = payload['version_id']
        data = payload['data']
        title = data.get('title')

        # 1. DOWNLOAD FROM BLOB
        conn_str = os.environ.get("AzureWebJobsStorage")
        container = data.get('container')
        blob_name = data.get('blob_name')
        
        if container and blob_name:
            logging.info(f"Downloading blob: {blob_name}")
            blob_client = BlobClient.from_connection_string(conn_str, container_name=container, blob_name=blob_name)
            text_content = blob_client.download_blob().readall().decode('utf-8')
        else:
            text_content = data.get('text_content', '')

        if not text_content: 
            logging.error(f"No text content found for {series_id}. Skipping processing.")
            return

        # 2. CHUNK
        raw_chunks = smart_legal_chunking(text_content)
        logging.info(f"Generated {len(raw_chunks)} chunks. Starting extraction...")

        # 3. PARALLEL PROCESSING (Embed + Ontology)
        processed_data = []
        # Max workers should be chosen carefully based on your Function App's CPU/memory and OpenAI rate limits.
        # 5-10 is a good starting point.
        with ThreadPoolExecutor(max_workers=5) as executor: 
            chunk_args = [(i, c) for i, c in enumerate(raw_chunks)]
            # executor.map returns an iterator, convert to list to ensure all are processed
            processed_data = list(executor.map(process_single_chunk, chunk_args))

        # 4. WRITE TO NEO4J
        if neo4j_driver:
            with neo4j_driver.session() as session:
                # Insert in batches to prevent large transaction issues
                batch_size = 50 
                for i in range(0, len(processed_data), batch_size):
                    batch = processed_data[i:i + batch_size]
                    session.execute_write(
                        add_legal_graph, 
                        series_id=series_id, 
                        title=title,
                        country=data.get('country'),
                        entity=data.get('entity'),
                        sector=data.get('sector'),
                        province=data.get('province'),
                        chunks_data=batch
                    )
            logging.info(f"✅ Graph built for {title} with {len(processed_data)} chunks.")

        # 5. UPDATE COSMOS
        if series_container and versions_container:
            series_item = {
                "id": series_id, "title": title, "country": data.get('country'),
                "entity": data.get('entity'), "latest_version_id": version_id,
                "last_updated": datetime.utcnow().isoformat()
            }
            series_container.upsert_item(body=series_item)
            try:
                version_item = versions_container.read_item(item=version_id, partition_key=series_id)
                version_item['status'] = "Ingested"
                version_item['processed_at'] = datetime.utcnow().isoformat()
                versions_container.upsert_item(body=version_item)
            except Exception as e:
                logging.warning(f"Could not update Cosmos version status for {series_id}/{version_id}: {e}")
            logging.info("✅ Cosmos DB Updated")

    except Exception as e:
        logging.error(f"❌ Processing Failed for {series_id}: {e}")
        # Re-raise the exception so Azure Functions can handle retries or move to poison queue
        raise e