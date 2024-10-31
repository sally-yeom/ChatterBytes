from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate 
from langchain_community.chat_models import ChatDatabricks

from pyspark.sql import SparkSession

from datetime import datetime, timedelta

from retriever import *

import config
import ast


def create_date_gen_model(query):
    today = datetime.today().strftime('%Y-%m-%d')
    one_year_ago = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')

    date_prompt = f'''
    I have FOMC meeting documents.

    Based on the question, please determine which periods (from when to when) should be used. The periods are as follows, with the oldest being '2015-03-18' and the most recent being '2024-09-18'. The answer should be only in the JSON format {{'from' : 'yyyy-mm-dd', 'to' : 'yyyy-mm-dd'}}. Don't use sentences.Be sure to keep this in mind. 
    REMEMBER, **Today is '{today}'**

    Refer to the example below when providing your answer.
    [Sample1]
    Question: "Analyze the economic trends over the past year."
    Answer: {{'from': '{one_year_ago}', 'to': '{today}'}}

    [Sample2]
    Question: "Tell me about the interest rate outlook from the most recent FOMC meeting."
    Answer: {{'from': '2024-09-18', 'to': '2024-09-18'}}

    ###
    Question: {query}
    Answer: 
    '''

    DEPLOYMENT_URL = 'chatterbytes_llama_v3_8b_instruct'
    model = ChatDatabricks(endpoint=DEPLOYMENT_URL)

    step1_pred = model.invoke(date_prompt)

    date_dict = ast.literal_eval(step1_pred.content)
    
    return date_dict['from'], date_dict['to']



def create_filtering_date(st, ed):
    spark = SparkSession.builder.getOrCreate()
    df = spark.table('workspace.default.fomc_presconf_10years').select('date').distinct()

    vals = [v.date for v in df.collect()]

    cond = []

    for v in vals:
        if v >= st and v <= ed:
            cond.append(v)
        
    return {
        'date' : cond
    }