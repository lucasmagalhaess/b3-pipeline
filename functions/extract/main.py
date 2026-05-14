import requests
import json
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage
import time

GCS_BUCKET = "b3-pipeline-496319-data-lake"
BRAPI_TOKEN = "dkLLEBPFDjX3ha7auGxNMe"

ACOES = [
    "PETR4", "VALE3", "ITUB4", "BBDC4",
    "MGLU3", "WEGE3", "ABEV3", "BBAS3"
]

def get_cotacao(symbol):
    url = f"https://brapi.dev/api/quote/{symbol}?token={BRAPI_TOKEN}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    results = response.json().get("results", [])
    return results[0] if results else None

def get_cotacoes():
    cotacoes = []
    for symbol in ACOES:
        try:
            cotacao = get_cotacao(symbol)
            if cotacao:
                cotacoes.append(cotacao)
                print(f"  {symbol}: R${cotacao['regularMarketPrice']}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  Erro em {symbol}: {e}")
    return cotacoes

def save_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"Salvo no GCS: {filename}")

@functions_framework.http
def extract_b3(request):
    try:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S")

        print("Iniciando extracao da B3...")
        cotacoes = get_cotacoes()
        print(f"Acoes extraidas: {len(cotacoes)}")

        payload = {
            "extraction_date": today,
            "extraction_timestamp": timestamp,
            "total_acoes": len(cotacoes),
            "cotacoes": cotacoes
        }

        filename = f"bronze/b3/{today}/cotacoes_{timestamp.replace(':', '-')}.json"
        save_to_gcs(payload, filename)

        return {"status": "success", "acoes": len(cotacoes), "file": filename}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
