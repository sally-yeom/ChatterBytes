from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from datetime import datetime
import os

def extract_date_from_filename(filename):
    # 파일명에서 날짜 추출 (예: 'document_20231012.pdf'에서 '2023-10-12' 추출)
    basename = os.path.basename(filename)
    date_str = basename.split('conf')[1].replace('.pdf', '')
    return datetime.strptime(date_str, '%Y%m%d').date()

def generate_chunk_id(date, chunk_number):
    # 날짜와 청크 번호로 고유 ID 생성 (예: '2023-10-12_001')
    return f"{date}_{str(chunk_number).zfill(3)}"

def load_pdf_with_metadata(pdf_path):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()
    # 각 문서에 날짜 메타데이터 추가
    date = extract_date_from_filename(pdf_path)
    for doc in documents:
        doc.metadata['date'] = str(date)
    return documents

def extract_date_from_filename(filename):
    # 파일명에서 날짜 추출 (예: 'document_20231012.pdf'에서 '2023-10-12' 추출)
    basename = os.path.basename(filename)
    date_str = basename.split('conf')[1].replace('.pdf', '')
    return datetime.strptime(date_str, '%Y%m%d').date()

def generate_chunk_id(date, chunk_number):
    return f"{date}_{str(chunk_number).zfill(3)}"

def load_pdf_with_metadata(pdf_path):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()
    # 각 문서에 날짜 메타데이터 추가
    date = extract_date_from_filename(pdf_path)
    for doc in documents:
        doc.metadata['date'] = str(date)
    return documents

def load_and_chunk_pdf_with_metadata(pdf_path, chunk_size=1000, chunk_overlap=300):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()

    # 파일명에서 날짜 추출
    date = extract_date_from_filename(pdf_path)

    # 청크 생성기 (오버랩 있는 텍스트 분할)
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    all_chunks = []
    for document in documents:
        chunks = text_splitter.split_text(document.page_content)
        for i, chunk in enumerate(chunks):
            # 청크에 메타데이터 추가
            chunk_doc = {
                'content': chunk,
                'metadata': {
                    'date': str(date),
                    'chunk_id': generate_chunk_id(date, i + 1)  # 청크 번호로 고유 ID 생성
                }
            }
            all_chunks.append(chunk_doc)
    
    return all_chunks