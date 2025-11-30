import azure.functions as func

# Import both blueprints
from blueprints.query import query_bp
from blueprints.ingest import ingest_bp
from blueprints.process import process_bp

# Initialize the Main App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Register the Blueprints
app.register_functions(query_bp)
app.register_functions(ingest_bp)
app.register_functions(process_bp)