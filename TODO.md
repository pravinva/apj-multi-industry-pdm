# TODO

- [ ] Upgrade manual retrieval to vector-index RAG:
  - Generate embeddings for manual chunks in `pdm_<industry>.bronze.manual_reference_chunks`.
  - Create Databricks Vector Search index per industry (or shared multi-industry index).
  - Implement hybrid retrieval (vector similarity + lexical overlap) with reranking.
  - Keep explicit source citations in chat responses and UI citation panels.
  - Add bootstrap/job step to refresh embeddings/index on manual uploads.
