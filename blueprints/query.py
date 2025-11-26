import azure.functions as func
import logging

# 1. Create the Blueprint object
query_bp = func.Blueprint()

# 2. Define the route (endpoint)
# This will be available at: http://localhost:7071/api/query_policy
@query_bp.route(route="query_policy", auth_level=func.AuthLevel.ANONYMOUS)
def query_policy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Query Policy function triggered.')

    # 3. Get the JSON body from the React App
    try:
        req_body = req.get_json()
        question = req_body.get('question')
        doc_id = req_body.get('doc_id')
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if question and doc_id:
        # TODO: Connect to Neo4j here later
        return func.HttpResponse(f"Received question: '{question}' for doc: {doc_id}", status_code=200)
    else:
        return func.HttpResponse(
             "Please pass a 'question' and 'doc_id' in the request body.",
             status_code=400
        )