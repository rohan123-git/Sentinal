"""
SENTINEL - Graph Analytics & Collusion Network Package.

Supports dual-execution graph analysis:
  1. Neo4j Cypher Graph Engine: Production persistent graph database with APOC support.
  2. In-Memory NetworkX MultiDiGraph: Zero-setup real-time topological analysis
     with deterministic fallback if Neo4j is offline.

Detects entity link collusion:
  - Multi-account device farms (e.g. single rooted phone operating multiple bank accounts)
  - Proxy and VPN exit node concentration
  - Coordinated mule rings and cyclical fund laundering pathways

Modules:
  - `engine`: GraphEngine class managing graph ingestion, ego-network extraction,
              and collusion ring discovery.
"""

# ─── Package Exports ─────────────────────────────────────────────────────────
from .engine import graph_engine, GraphEngine

__all__ = [
    "graph_engine",
    "GraphEngine"
]
