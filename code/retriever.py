import os
import time

from pyspark.sql import SparkSession
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain.schema import BaseRetriever
from databricks.vector_search.client import VectorSearchClient
import databricks.sdk.service.catalog as c

import config


class Retriever:
    def __init__(self):
        self.vector_search_endpoint = config.ENDPOINT['vector_search']
        self.emb_model_endpoint = config.ENDPOINT['emb_model']
        self.index_path = config.PATH['index']
        self.source_path = config.PATH['source']
        self.pk_col = config.COLUMN['pk']
        self.emb_src_col = config.COLUMN['emb_src_col']

    def get_vs_endpoint() -> None:
        vsc = VectorSearchClient()

        def endpoint_exists(vsc: VectorSearchClient, endpoint_name: str) -> bool:
            try:
                vsc.get_endpoint(endpoint_name)
                print(f"Endpoint {endpoint_name} exists. Success getting endpoint.")
                return True
            except Exception as e:
                return False
        
        if not endpoint_exists(vsc, config.ENDPOINT['vector_search']):
            print('Creating endpoint...')
            vsc.create_endpoint(name=config.ENDPOINT['vector_search'], endpoint_type='STANDARD')
    
    def create_vs_index() -> None:
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"ALTER TABLE {config.PATH['source']} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

        vsc = VectorSearchClient()

        def index_exists(vsc, endpoint_name, index_name) -> bool:
            try:
                vsc.get_index(endpoint_name, index_name)
                return True
            except Exception as e:
                return False
        
        if not index_exists(vsc, self.vector_search_endpoint, self.index_path):
            vsc.create_delta_sync_index(
                endpoint_name=self.vector_search_endpoint,
                index_name=self.index_path,
                source_table_name=self.source_path,
                pipeline_type='TRIGGERED',
                primary_key=self.pk_col,
                embedding_source_column=self.emb_src_col,
                embedding_model_endpoint_name=self.emb_model_endpoint
            )
        else:
            vsc.get_index(self.vector_search_endpoint, self.index_path)
    
    def get_retriever(persist_dir: str = None) -> BaseRetriever:
        vsc = VectorSearchClient()
        vs_index = vsc.get_index(self.vector_search_endpoint, self.index_path)
        vectorstore = DatabricksVectorSearch(vs_index, text_column="content", embedding=self.emb_model_endpoint)
        
        return vectorstore.as_retriever()