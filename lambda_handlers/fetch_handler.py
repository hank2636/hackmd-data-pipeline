# lambda_handlers/fetch_handler.py
import json
from src.extract import arxiv_collector

def lambda_handler(event, context):
    categories = event.get("categories")  
    arxiv_collector.run_lambda(categories)
    return {"status": "ok"}
