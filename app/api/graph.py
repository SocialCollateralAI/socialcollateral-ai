from fastapi import APIRouter

from app.services.data_service import data_service

router = APIRouter()


@router.get("/graph")
async def get_graph_topology():
    groups = data_service.get_all_groups()
    nodes = []
    edges = []

    # Build Nodes (Grup)
    for gid, g in groups.items():
        color = "#10B981"  # Green (Healthy)
        if g["type"] == "toxic":
            color = "#EF4444"  # Red (Danger)
        elif g["type"] == "medium":
            color = "#F59E0B"  # Orange/Yellow (Medium Risk)

        nodes.append(
            {
                "key": gid,
                "attributes": {
                    "label": g["header"]["name"],
                    "x": g.get("x", 0),
                    "y": g.get("y", 0),
                    "size": g["size"],
                    "color": color,
                    "risk_badge": g["header"]["risk_badge"],  # filter FE
                    "location_city": g["header"]["location_city"],
                    "location_village": g["header"]["location_village"],
                    "trust_score": g["header"]["trust_score"],
                },
            }
        )

        # Build Edges (Relasi)
        for neighbor in g["overview"].get("neighbors", []):
            target_name = neighbor["name"]
            target_id = None

            # Cari ID tetangga by name (Scan database)
            for potential_id, potential_group in groups.items():
                if potential_group["header"]["name"] == target_name:
                    target_id = potential_id
                    break

            if target_id:
                # Sort ID biar A->B dan B->A dianggap sama (Undirected Edge)
                edge_tuple = sorted([gid, target_id])
                edge_key = f"edge_{edge_tuple[0]}_{edge_tuple[1]}"

                edges.append(
                    {
                        "key": edge_key,
                        "source": gid,
                        "target": target_id,
                        "attributes": {
                            "size": 2,
                            "color": "#cbd5e1",
                            "type": "line",
                            "label": neighbor.get(
                                "relation", "Tetangga"
                            ),  # Label di garis"
                        },
                    }
                )

    # Hapus duplikat edge (karena A bertetangga B, B bertetangga A)
    unique_edges = list({e["key"]: e for e in edges}.values())

    return {"nodes": nodes, "edges": unique_edges}
