import argparse

from data_manager import DataManager
from retriever import Retriever
from chatter import Chatter


def chat(question):
    return Chatter().get_response(question)

def process(indexing=True):
    data_manager = DataManager()
    print(f' [ START ] Get pdf')
    all_chunks = data_manager.load_fomc_pdf()
    print(f' [ END ] Get pdf')
    print(f' [ START ] Save chunked data')
    data_manager.save_chunked_data(all_chunks)
    print(f' [ END ] Save chunked data')
    
    if indexing:
        retriever = Retriever()
        print(f' [ START ] Create vectorstore endpoint')
        retriever.get_vs_endpoint()
        print(f' [ END ] Create vectorstore endpoint')
        print(f' [ START ] Create vectorstore index')
        retriever.create_vs_index()
        print(f' [ END ] Create vectorstore index')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run ChatterBytes pipeline')
    parser.add_argument('--mode', choices=['process', 'chat'], required=True, help='Choose "process" for data processing/indexing or "chat" to ask a question')
    parser.add_argument('--question', type=str, help='Question to ask the chat model (required for chat mode)')
    parser.add_argument('--no-indexing', default=True, help="Skip indexing step when mode is 'run'")

    args = parser.parse_args()

    if args.mode == "process":
        process(indexing=args.no_indexing)

    elif args.mode == "chat":
        if not args.question:
            print("❗ You must provide a --question for chat mode.")
        else:
            answer = chat(args.question)
            print(f"\n💬 Model response:\n{answer}")
    