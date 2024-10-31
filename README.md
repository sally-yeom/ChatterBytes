# Development of an Economic Forecasting GenAI Model

Project for Databricks GenAI World Cup

The ChatterBytes team has developed a GenAI model to generate economic forecasts. The motivation behind choosing this topic was to explore whether an LLM model could predict economic trends similarly to how humans anticipate trends based on the FOMC.

We utilized a RAG and LLM structure, implementing RAG with LangChain. When a question is asked, the RAG extracts relevant documents, which are then fed into the prompt for the LLM. To enhance quality, prompt engineering was applied throughout the development process. All development was conducted in Databricks' serverless environment.

Databricks Tools Utilized:
 - Workspace
 - Catalog
 - Compute
 - Dashboards
 - Experiments
 - Registered Models
 - Serving Endpoints
 - Datasets Utilized:

Full transcripts of FOMC press conferences from March 18, 2015, to September 18, 2024 (covering the past 10 years).
The FOMC press conference transcripts are open datasets available at [Federal Reserve](https://www.federalreserve.gov/aboutthefed.htm).

Models Utilized:
 - Embedding Model: bge_small_en_v1_5_v3
 - LLM Model: llama_v3_8b_instruct

Process Steps:
 1. Text Reading
 2. Chunking
 3. Embedding
 4. VectorDB Indexing
 5. Retriever
 6. Generate LLM Chat Response

The demo page can be viewed at [LINK](https://vimeo.com/1025217497?share=copy).
