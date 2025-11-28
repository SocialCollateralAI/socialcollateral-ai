from typing import Any, Dict, List, Optional

from pydantic import BaseModel


# Sub-Models untuk Detail Group
class Header(BaseModel):
    name: str
    location_city: str
    location_village: str
    member_count: int
    risk_badge: str
    trust_score: int
    loan_eligibility: str
    total_loan_amount: int


class PrimaryDriver(BaseModel):
    text: str
    payment_score: int
    social_score: int


class Metrics(BaseModel):
    cycle: int
    repayment_rate: float
    avg_delay: str


class Neighbor(BaseModel):
    id: str
    name: str
    risk: str
    distance: str
    relation: str


class Overview(BaseModel):
    primary_driver: PrimaryDriver
    metrics: Metrics
    neighbors: List[Neighbor]
    max_plafon_recommendation: Optional[int] = None


class Trends(BaseModel):
    repayment_history: List[Dict[str, Any]]  # [{"month": "Jan", "rate": 98}, ...]
    asset_growth: List[Dict[str, Any]]
    stats: Dict[str, Any]
    seasonality_heatmap: List[int]


class Insights(BaseModel):
    social_graph: Dict[str, Any]
    cv: Dict[str, Any]
    prediction: Dict[str, Any]
    recommendation_text: str


class Decision(BaseModel):
    last_audit: str
    is_locked: bool


# Main Group Model
class GroupDetail(BaseModel):
    id: str
    type: str  # healthy | toxic | medium
    header: Header
    overview: Overview
    trends: Trends
    insights: Insights
    decision: Decision
    # Koordinat untuk Graph
    x: Optional[int] = 0
    y: Optional[int] = 0


# Graph Specific Models
class GraphNodeAttributes(BaseModel):
    label: str
    x: int
    y: int
    size: int
    color: str
    risk_badge: str


class GraphNode(BaseModel):
    key: str
    attributes: GraphNodeAttributes


class GraphEdge(BaseModel):
    key: str
    source: str
    target: str
    attributes: Dict[str, Any]


class GraphTopology(BaseModel):
    nodes: List[GraphNode]
    edges: List[GraphEdge]
