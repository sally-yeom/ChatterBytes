from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate 
from langchain_community.chat_models import ChatDatabricks

from retriever import *

import config


def create_chat_model():
    chat_model = ChatDatabricks(endpoint=config.ENDPOINT['llm_model'], max_tokens=200)
        
    TEMPLATE = """
    You are an assistant for Economic outlook. You provide predictions about both the past and future economies. If the question is not related to one of these topics, kindly decline to answer. If you don't know the answer, just say that you don't know, don't try to make up an answer. Keep the answer as concise as possible. Use the following pieces of context to answer the question at the end:
    {context}
    Question: {question}
    Answer:
    """

    prompt = PromptTemplate(template=TEMPLATE, input_variables=["context", "question"])
    chain = RetrievalQA.from_chain_type(
        llm=chat_model, 
        chain_type="stuff" ,
        retriever=get_retriever(),
        chain_type_kwargs={"prompt": prompt}
    )

    return chain