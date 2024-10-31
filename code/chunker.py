from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from datetime import datetime
import os

def extract_date_from_filename(filename):
    basename = os.path.basename(filename)
    date_str = basename.split('conf')[1].replace('.pdf', '')
    return datetime.strptime(date_str, '%Y%m%d').date()

def generate_chunk_id(date, chunk_number):
    return f"{date}_{str(chunk_number).zfill(3)}"

def extract_date_from_filename(filename):
    basename = os.path.basename(filename)
    date_str = basename.split('conf')[1].replace('.pdf', '')
    return datetime.strptime(date_str, '%Y%m%d').date()

def generate_chunk_id(date, chunk_number):
    return f"{date}_{str(chunk_number).zfill(3)}"

def load_and_chunk_pdf_with_metadata(pdf_path, chunk_size=3000, chunk_overlap=1000):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()

    date = extract_date_from_filename(pdf_path)

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    all_chunks = []
    for document in documents:
        chunks = text_splitter.split_text(document.page_content)
        for i, chunk in enumerate(chunks):
            chunk_doc = {
                'content': chunk,
                'metadata': {
                    'date': str(date),
                    'year': int(date.year),
                    'month': int(date.month),
                    'day': int(date.day),
                    'chunk_id': generate_chunk_id(date, i + 1)
                }
            }
            all_chunks.append(chunk_doc)
    
    return all_chunks