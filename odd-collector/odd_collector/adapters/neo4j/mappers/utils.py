from collections import defaultdict, namedtuple
from typing import NamedTuple

from odd_collector.adapters.neo4j.mappers import NodeMetadata, RelationshipMetadata


def _get_node_name(node_labels):
    _res = ""
    for label in node_labels:
        if _res != "":
            _res += ":"
        _res += label
    return _res


def _group_nodes_by_labels(nodes_map: dict[str, list[NamedTuple]], items: list):
    for node in items:
        metadata = NodeMetadata(*node)
        node_name = _get_node_name(metadata.node_labels)
        n = nodes_map.get(node_name)
        if n:
            n.append(metadata)
        else:
            nodes_map[node_name] = [metadata]


def _group_nodes_by_relationships_presence(
    nodes_map: dict[str, list[NamedTuple]], items: list
):
    for node in items:
        metadata = RelationshipMetadata(*node)

        for labels in (metadata.source_node_labels, metadata.target_node_labels):
            node_name = _get_node_name(labels)
            n = nodes_map.get(node_name)
            if n:
                if metadata not in n:
                    n.append(metadata)
            else:
                nodes_map[node_name] = [metadata]


def _aggregate_relationships(relationships: list) -> list:
    # Aggregate by (source_node, rel_type, target_node, sorted rel_properties)
    aggregated_data = defaultdict(lambda: [0, set()])

    for row in relationships:
        key = (tuple(row[0]), row[1], tuple(row[2]), frozenset(row[4]))
        aggregated_data[key][0] += row[3]
        aggregated_data[key][1].update(row[4])

    result = [
        [list(key[0]), key[1], list(key[2]), count, list(properties)]
        for key, (count, properties) in aggregated_data.items()
    ]
    return result
