# TODO

- [ ] Upgrade manual retrieval to vector-index RAG:
  - Generate embeddings for manual chunks in `pdm_<industry>.bronze.manual_reference_chunks`.
  - Create Databricks Vector Search index per industry (or shared multi-industry index).
  - Implement hybrid retrieval (vector similarity + lexical overlap) with reranking.
  - Keep explicit source citations in chat responses and UI citation panels.
  - Add bootstrap/job step to refresh embeddings/index on manual uploads.

- [ ] Add end-to-end asset coverage validation:
  - Build a single coverage check (endpoint/query) for configured assets vs Silver/Gold tables.
  - Report per-asset presence across `silver.sensor_features`, `gold.feature_vectors`, and `gold.pdm_predictions`.
  - Surface missing assets with likely cause (no recent features, model missing, scoring skipped/fallback).
