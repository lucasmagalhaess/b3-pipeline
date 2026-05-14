import json
import functions_framework
from datetime import datetime, timezone
from google.cloud import storage, bigquery

GCS_BUCKET = "b3-pipeline-496319-data-lake"
BQ_PROJECT = "b3-pipeline-496319"
BQ_DATASET = "analytics"
BQ_TABLE = "cotacoes_gold"

def read_from_gcs(filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    return json.loads(blob.download_as_string())

def save_silver_to_gcs(data, filename):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"Silver salvo no GCS: {filename}")

def classificar_variacao(variacao_percent):
    if variacao_percent >= 2:
        return "alta_forte"
    elif variacao_percent >= 0.5:
        return "alta"
    elif variacao_percent > -0.5:
        return "estavel"
    elif variacao_percent > -2:
        return "baixa"
    else:
        return "baixa_forte"

def transform_cotacao(cotacao, extraction_date, extraction_timestamp):
    preco = cotacao.get("regularMarketPrice", 0)
    volume = cotacao.get("regularMarketVolume", 0)
    variacao = cotacao.get("regularMarketChange", 0)
    variacao_percent = cotacao.get("regularMarketChangePercent", 0)

    return {
        "symbol": cotacao.get("symbol"),
        "short_name": cotacao.get("shortName"),
        "preco_atual": float(preco) if preco else None,
        "abertura": float(cotacao.get("regularMarketOpen", 0)),
        "maxima": float(cotacao.get("regularMarketDayHigh", 0)),
        "minima": float(cotacao.get("regularMarketDayLow", 0)),
        "fechamento_anterior": float(cotacao.get("regularMarketPreviousClose", 0)),
        "variacao": float(variacao) if variacao else None,
        "variacao_percent": float(variacao_percent) if variacao_percent else None,
        "classificacao": classificar_variacao(variacao_percent) if variacao_percent else "estavel",
        "volume": int(volume) if volume else None,
        "volume_financeiro": float(preco * volume) if preco and volume else None,
        "market_cap": float(cotacao.get("marketCap", 0)) if cotacao.get("marketCap") else None,
        "minima_52s": float(cotacao.get("fiftyTwoWeekLow", 0)),
        "maxima_52s": float(cotacao.get("fiftyTwoWeekHigh", 0)),
        "extraction_date": extraction_date,
        "extraction_timestamp": extraction_timestamp
    }

def load_to_bigquery(rows):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"Erros BigQuery: {errors}")
    print(f"Inseridos {len(rows)} registros no BigQuery")

@functions_framework.http
def transform_b3(request):
    try:
        request_json = request.get_json(silent=True)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = request_json.get("filename") if request_json else None

        if not filename:
            filename = f"bronze/b3/{today}/test.json"

        print(f"Lendo arquivo: {filename}")
        raw_data = read_from_gcs(filename)

        cotacoes = raw_data.get("cotacoes", [])
        extraction_date = raw_data.get("extraction_date", today)
        extraction_timestamp = raw_data.get("extraction_timestamp", "")

        print(f"Transformando {len(cotacoes)} acoes...")
        rows = [transform_cotacao(c, extraction_date, extraction_timestamp) for c in cotacoes]

        silver_filename = filename.replace("bronze/", "silver/")
        save_silver_to_gcs({"cotacoes": rows}, silver_filename)

        load_to_bigquery(rows)

        return {"status": "success", "acoes_processadas": len(rows)}, 200

    except Exception as e:
        print(f"Erro: {e}")
        return {"status": "error", "message": str(e)}, 500
