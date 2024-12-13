from elasticsearch import Elasticsearch
from fastapi import FastAPI

app = FastAPI()

es = Elasticsearch("http://localhost:9200")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/search/")
async def search_document(index: str, field: str, query: str, size: int):
    """
    Elasticsearch에서 검색 수행

    Args:
        index (str): 검색할 인덱스 이름
        query (str): 검색할 키워드
        size (int): 반환할 결과 수 (기본값: 10)
    Returns:
        dict: 검색 결과
    """
    if not es.ping():
        return {"error" : "Elasticsearch server is not reachable"}
    
    search_body = {
        "query" : {
            "match": {
                field : query
            }
        }
    }

    response = es.search(index=index, body=search_body, size=size)
    return response["hits"]["hits"]


@app.post("/")
async def add_document(index: str, doc_id: str, content: str):
    """
    Elasticsearch에 문서 추가.
    
    Args:
        index (str): 저장할 인덱스 이름
        doc_id (str): 문서 ID
        content (str): 문서 내용
    Returns:
        dict: 결과
    """
    document = {"content" : content}
    response = es.index(index=index, id=doc_id, document=document)
    return response