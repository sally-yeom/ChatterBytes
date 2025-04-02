import os
from typing import List

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

import config
from chunker import Chunker


class DataManager:
    def __init__(self, pdf_path: str = config.PATH['pdf'], source_path: str = config.PATH['source']) -> None:
        self.pdf_path = pdf_path
        self.source_path = source_path
    
    def load_fomc_pdf() -> List[str]:
        pdf_files = [os.path.join(self.pdf_path, filename) for filename in os.listdir(self.pdf_path)]
        all_chunks = []
        
        chunker = Chunker()

        for path in pdf_files:
            chunks = chunker.load_and_chunk_pdf_with_metadata(path)
            all_chunks.extend(chunks)
            
        return all_chunks
    
    def save_chunked_data(all_chunks: List[str]) -> None:
        data = pd.DataFrame({
            'date': [c['metadata']['date'] for c in all_chunks],
            'year': [c['metadata']['year'] for c in all_chunks],
            'month': [c['metadata']['month'] for c in all_chunks],
            'day': [c['metadata']['day'] for c in all_chunks],
            'chunk_id': [c['metadata']['chunk_id'] for c in all_chunks],
            'content': [c['content'] for c in all_chunks]
        })

        schema = StructType([
            StructField('date', StringType(), True),
            StructField('year', IntegerType(), True),
            StructField('month', IntegerType(), True),
            StructField('day', IntegerType(), True),
            StructField('chunk_id', StringType(), True),
            StructField('content', StringType(), True)  
        ])

        spark = SparkSession.builder.getOrCreate()
        
        data_df = spark.createDataFrame(data, schema)
        data_df.write.option('mergeSchema', 'true').mode('overwrite').saveAsTable(self.source_path)






