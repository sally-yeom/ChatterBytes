from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient

from langchain_community.vectorstores import DatabricksVectorSearch 
from langchain_community.embeddings import DatabricksEmbeddings

import databricks.sdk.service.catalog as c
import time, os
import config


def get_vs_endpoint():
    vsc = VectorSearchClient()

    def endpoint_exists(vsc, endpoint_name):
        try:
            vsc.get_endpoint(endpoint_name)
            return True
        except Exception as e:
            return False
    
    if not endpoint_exists(vsc, config.ENDPOINT['vector_search']):
        vsc.create_endpoint(name=config.ENDPOINT['vector_search'], endpoint_type='STANDARD')



def create_vs_index():
    spark.sql(f"ALTER TABLE {source_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    def index_exists(vsc, endpoint_name, index_name):
        try:
            vsc.get_index(endpoint_name, index_name)
            return True
        except Exception as e:
            return False
    
    if not index_exists(vsc, config.ENDPOINT['vector_search'], config.PATH['index']):
        vsc.create_delta_sync_index(
            endpoint_name=config.ENDPOINT['vector_search'],
            index_name=config.PATH['index'],
            source_table_name=config.PATH['source'],
            pipeline_type='TRIGGERED',
            primary_key=config.COLUMN['pk'],
            embedding_source_column=config.COLUMN['emb_src_col'],
            embedding_model_endpoint_name=config.ENDPOINT['emb_model']
        )
    else:
        vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_path)



def get_retriever(persist_dir: str = None):
    vsc = VectorSearchClient()
    vs_index = vsc.get_index(config.ENDPOINT['vector_search'], config.PATH['index'])
    vectorstore = DatabricksVectorSearch(vs_index, text_column="content", embedding=config.ENDPOINT['emb_model'])
    
    return vectorstore.as_retriever()
