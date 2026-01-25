from pydantic import BaseModel, Field
from typing import List, Optional


class SemanticSearchRequest(BaseModel):
    """
    Request body cho API semantic search
    """
    query: str = Field(
        ...,
        min_length=1,
        description="Câu truy vấn ngôn ngữ tự nhiên"
    )
    top_k: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Số lượng kết quả trả về"
    )


class SemanticSearchResult(BaseModel):
    """
    Một kết quả semantic search
    """
    score: float = Field(
        ...,
        description="Độ tương đồng cosine"
    )
    file_hash: Optional[str] = Field(
        None,
        description="Hash của file nguồn"
    )
    chunk_id: Optional[str] = Field(
        None,
        description="ID của chunk"
    )
    section_id: Optional[str] = Field(
        None,
        description="ID của section"
    )
    text: Optional[str] = Field(
        None,
        description="Nội dung text của chunk"
    )
    token_estimate: Optional[int] = Field(
        None,
        description="Ước lượng số token"
    )


class SemanticSearchResponse(BaseModel):
    """
    Response cho API semantic search
    """
    query: str
    results: List[SemanticSearchResult]
