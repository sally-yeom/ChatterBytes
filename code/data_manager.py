from pyspark.sql import SparkSession
from pyspark.sql.types import *
from chunker import *

import pandas as pd
import config


def get_pdf():
    pdf_files = [os.path.join(path, filename) for filename in os.listdir(config.ENDPOINT['vector_search'])]
    all_chunks = []

    for pdf_path in pdf_paths:
        chunks = load_and_chunk_pdf_with_metadata(pdf_path)
        all_chunks.extend(chunks)
    
    return all_chunks


def create_save_df(all_chunks):
    data = pd.DataFrame({
        'date': [c['metadata']['date'] for c in all_chunks],
        'chunk_id': [c['metadata']['chunk_id'] for c in all_chunks],
        'content': [c['content'] for c in all_chunks]
    })

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("chunk_id", StringType(), True),
        StructField("content", StringType(), True)  
    ])

    data_df = spark.createDataFrame(data, schema)
    data_df.write.option("mergeSchema", "true").mode('overwrite').saveAsTable(config.PATH["source"])
