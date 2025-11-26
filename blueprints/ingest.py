import azure.functions as func
import logging

# 1. Create the Blueprint
ingest_bp = func.Blueprint()

# 2. Define the route
@ingest_bp.route(route="ingest_policy", auth_level=func.AuthLevel.ANONYMOUS)
def ingest_policy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Ingest Policy function triggered.')

    # This is where we will eventually put the Cosmos DB + Neo4j logic
    try:
        req_body = req.get_json()
        url = req_body.get('url')
        country = req_body.get('country')
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if url:
        return func.HttpResponse(f"Started ingestion for: {url}", status_code=200)
    else:
        return func.HttpResponse(
             "Please pass a 'url' in the request body.",
             status_code=400
        )