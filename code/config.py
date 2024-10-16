# PATH = {
#     "pdf": "/Workspace/Users/Data/",
#     "source": "workspace.default.fomc_presconf_10years_summary",
#     "index": "workspace.default.fomc_presconf_10years_index_summary"
# }

# ENDPOINT = {
#     "vector_search": "fomc_transcript_summary_endpoint",
#     "emb_model": "chatterbytes_bge_small_en_v1_5_v3",
#     "llm_model": "chatterbytes_dbrx_instruct"
# }

# COLUMN = {
#     "pk": "date",
#     "emb_src_col": "content"
# }


PATH = {
    "pdf": "/Workspace/Users/Data/",
    "source": "workspace.default.fomc_presconf_10years",
    "index": "workspace.default.fomc_presconf_10years_index"
}

ENDPOINT = {
    "vector_search": "fomc_transcript_endpoint",
    "emb_model": "chatterbytes_bge_small_en_v1_5_v3",
    "llm_model": "chatterbytes_dbrx_instruct"
}

COLUMN = {
    "pk": "chunk_id",
    "emb_src_col": "content"
}