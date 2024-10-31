from data_manager import *
from retriever import *
from chat_model import *

import config


def chat_model_response(question):
    return create_chat_model(question).invoke(question)


def run(indexing=True):
    print(f' [ START ] Get pdf')
    all_chunks = get_pdf()
    print(f' [ END ] Get pdf')
    print(f' [ START ] Save chunked data')
    create_save_df(all_chunks)
    print(f' [ END ] Save chunked data')
    
    if indexing:
        print(f' [ START ] Create vectorstore endpoint')
        get_vs_endpoint()
        print(f' [ END ] Create vectorstore endpoint')
        print(f' [ START ] Create vectorstore index')
        create_vs_index()
        print(f' [ END ] Create vectorstore index')


if __name__ == "__main__":
    run(True)