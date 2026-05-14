terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("b3-pipeline-496319-77c3f55eebe2.json")
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = var.region
}

resource "google_bigquery_table" "cotacoes_gold" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "cotacoes_gold"
  deletion_protection = false

  schema = jsonencode([
    { name = "symbol", type = "STRING" },
    { name = "short_name", type = "STRING" },
    { name = "preco_atual", type = "FLOAT" },
    { name = "abertura", type = "FLOAT" },
    { name = "maxima", type = "FLOAT" },
    { name = "minima", type = "FLOAT" },
    { name = "fechamento_anterior", type = "FLOAT" },
    { name = "variacao", type = "FLOAT" },
    { name = "variacao_percent", type = "FLOAT" },
    { name = "classificacao", type = "STRING" },
    { name = "volume", type = "INTEGER" },
    { name = "volume_financeiro", type = "FLOAT" },
    { name = "market_cap", type = "FLOAT" },
    { name = "minima_52s", type = "FLOAT" },
    { name = "maxima_52s", type = "FLOAT" },
    { name = "extraction_date", type = "STRING" },
    { name = "extraction_timestamp", type = "STRING" }
  ])
}
