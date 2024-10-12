from data_manager import *
from retriever import *
from chat_model import *

import config


def run(indexing=True):
    all_chunks = get_pdf()
    create_save_df(all_chunks)
    chain = create_chat_model()

    if indexing:
        get_vs_endpoint()
        create_vs_index()
        
    question = {'query': 'Give me an economic outlook for January next year.'}
    answer = chain.invoke(question)

    print(answer)


if __name__ == "__main__":
    run(True)