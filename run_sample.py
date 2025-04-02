# Databricks notebook source
bye_message = 'Thank you. Have a nice day. :)'

# COMMAND ----------

# MAGIC %pip install langchain faiss-cpu transformers pypdf datasets langchain-community torch mlflow databricks_genai_inference sentence-transformers databricks-vectorsearch databricks-sdk databricks-sql-connector
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys

print(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'code'))


from chatter import Chatter
from IPython.display import clear_output

# COMMAND ----------

# MAGIC %%time
# MAGIC
# MAGIC main.run()

# COMMAND ----------

def FOMC_Chatter(query):
    question = {'query': query}
    result = Chatter().chat(question)['result']
    clear_output(wait=True)
    print(result)

# COMMAND ----------

FOMC_Chatter('Tell me about the main topics discussed in the most recent FOMC meeting.')

# COMMAND ----------

FOMC_Chatter('Summarize the views on consumer prices of 2024 in chronological order.')

# COMMAND ----------

FOMC_Chatter('Explain the direction of interest rates during the COVID-19 period.')

# COMMAND ----------

FOMC_Chatter('Give me an economic outlook for January next year.')

# COMMAND ----------

print(bye_message)
