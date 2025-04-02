import os
from datetime import datetime
from typing import List

from langchain.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

import config


class Chunker:
    def __init__(self, chunk_size: int = config.CHUNK_SIZE, chunk_overlap: int = config.CHUNK_OVERLAP) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk_data(self, data: List[str]) -> List[str]:
        return [data[i:i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]
    
    def extract_date_from_filename(filename: str) -> datetime:
        basename = os.path.basename(filename)
        date_str = basename.split('conf')[1].replace('.pdf', '')
        return datetime.strptime(date_str, '%Y%m%d').date()
    
    def generate_chunk_id(date: str, chunk_number: int) -> str:
        return f"{date}_{str(chunk_number).zfill(3)}"
    
    def load_and_chunk_pdf_with_metadata(pdf_path: str) -> List[str]:
        loader = PyPDFLoader(pdf_path)
        documents = loader.load()

        date = self.extract_date_from_filename(pdf_path)
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)

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
                        'chunk_id': self.generate_chunk_id(date, i + 1)
                    }
                }
                all_chunks.append(chunk_doc)
        
        return all_chunks