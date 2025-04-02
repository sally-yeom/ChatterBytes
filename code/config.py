PATH = {
    "pdf": "/Workspace/Users/Data/",
    "source": "workspace.default.fomc_presconf_10years",
    "index": "workspace.default.fomc_presconf_10years_index"
}

ENDPOINT = {
    "vector_search": "fomc_transcript_endpoint",
    "emb_model": "chatterbytes_bge_small_en_v1_5_v3",
    "pre_llm_model": "chatterbytes_llama_v3_8b_instruct",
    "post_llm_model": "chatterbytes_dbrx_instruct"   
}

COLUMN = {
    "pk": "chunk_id",
    "emb_src_col": "content"
}

CHUNK_SIZE = 3000
CHUNK_OVERLAP = 1000
CHATTER_MAX_TOKENS = 512