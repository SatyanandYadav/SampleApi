from fastapi import FastAPI, Form, Body
from typing import Dict, List, Set, Any
import json
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
origins = ["http://localhost:8000", "localhost:8000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Ping": "Pong"}


def is_dag(graph: Dict[str, List[str]]) -> bool:
    def dfs(node: str, visited: Set[str], stack: Set[str]) -> bool:
        if node in stack:
            return False
        if node in visited:
            return True

        visited.add(node)
        stack.add(node)

        for neighbor in graph.get(node, []):
            if not dfs(neighbor, visited, stack):
                return False

        stack.remove(node)
        return True

    visited = set()
    for node in graph:
        if node not in visited:
            if not dfs(node, visited, set()):
                return False
    return True


@app.post("/pipelines/parse")
async def parse_pipeline(pipeline: Dict[Any, Any]):
    try:
        nodes = pipeline.get("nodes", [])
        edges = pipeline.get("edges", [])


        num_nodes = len(nodes)
        num_edges = len(edges)

        # Create a graph representation
        graph = {node["id"]: [] for node in nodes}
        for edge in edges:
            source = edge["source"]
            target = edge["target"]
            graph[source].append(target)

        is_dag_result = is_dag(graph)

        return {"num_nodes": num_nodes, "num_edges": num_edges, "is_dag": is_dag_result}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format"}
    except KeyError as e:
        return {"error": f"Missing required field: {str(e)}"}
