"""
SENTINEL - Graph Visualization & Ego-Network API Router.

Exposes graph visualization endpoints serving serialized node-link subgraphs
for interactive Cytoscape.js canvases in the frontend dashboard.
Supports transaction ego-networks and full syndicate network topologies.
"""

# ─── Standard Library & FastAPI Imports ───────────────────────────────────────
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Query

# ─── Internal Application Imports ─────────────────────────────────────────────
from ..graph.engine import graph_engine

# ─── Router Instantiation ─────────────────────────────────────────────────────
router = APIRouter(prefix="/api/graph", tags=["Graph Analysis"])


# ─── Graph Subgraph Endpoints ─────────────────────────────────────────────────
@router.get(
    "/transaction/{tx_id}",
    summary="Get Transaction Ego-Network",
    description="Extract a local neighborhood subgraph (nodes & edges) centered on a specific transaction up to N hops."
)
def get_transaction_subgraph(
    tx_id: str,
    depth: int = Query(2, ge=1, le=3, description="Neighborhood traversal radius in graph hops")
):
    """
    Extract an ego-network subgraph around a target transaction ID.

    Traverses relationships to connected accounts, physical devices, IP addresses,
    and merchants to display topological risk context in frontend graph visualizers.

    Args:
        tx_id: Root transaction identifier (e.g. 'T9001').
        depth: Traversal expansion depth (bounded 1 to 3 hops).

    Returns:
        Dict[str, Any]: Dictionary containing formatted 'nodes' and 'edges' arrays.
    """
    subgraph = graph_engine.get_transaction_subgraph(tx_id, depth=depth)
    return subgraph


@router.get(
    "/network/{network_id}",
    summary="Get Fraud Ring Network Subgraph",
    description="Retrieve the complete multi-party collusion graph for a detected syndicate with shared nodes highlighted."
)
def get_network_subgraph(
    network_id: str
):
    """
    Retrieve the interactive node-link graph of a detected fraud ring.

    Highlights shared nexus entities (such as rooted devices or proxy IP addresses)
    and maps the interconnected accounts and merchant transaction terminals.

    Args:
        network_id: Unique syndicate network ID (e.g. 'NET-001').

    Returns:
        Dict[str, Any]: Formatted graph payload with styled nodes and relationship edges.
    """
    subgraph = graph_engine.get_network_subgraph(network_id)
    return subgraph

