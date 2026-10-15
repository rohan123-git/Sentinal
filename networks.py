"""
SENTINEL - Fraud Networks & Syndicates API Router.

Provides investigative endpoints for querying detected multi-account fraud rings,
shared infrastructure syndicates, and coordinated money mule operations.
"""

# ─── Standard Library & FastAPI Imports ───────────────────────────────────────
from typing import List, Dict, Any
from fastapi import APIRouter

# ─── Internal Application Imports ─────────────────────────────────────────────
from ..graph.engine import graph_engine

# ─── Router Instantiation ─────────────────────────────────────────────────────
router = APIRouter(prefix="/api/networks", tags=["Fraud Networks"])


# ─── Fraud Networks Endpoints ─────────────────────────────────────────────────
@router.get(
    "",
    response_model=List[Dict[str, Any]],
    summary="List Detected Fraud Networks",
    description="Retrieve all identified coordinated multi-account fraud rings and collusion clusters discovered in the graph."
)
def get_detected_fraud_networks():
    """
    Execute graph syndicate detection to identify multi-party collusion clusters.

    Scans the relationship graph for groups of 3 or more accounts operating through
    identical physical hardware (device farms) or proxy IP gateways, returning
    investigative summaries with risk scores and participating entity tallies.

    Returns:
        List[Dict[str, Any]]: List of detected fraud network summaries.
    """
    networks = graph_engine.detect_fraud_networks()
    return networks

