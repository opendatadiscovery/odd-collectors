from collections import defaultdict, namedtuple
from typing import NamedTuple


def _group_by_labels(
    nodes_map: dict[str, list[NamedTuple]], namedtuple_func: namedtuple, items: list
):
    for node in items:
        metadata: NamedTuple = namedtuple_func(*node)
        node_name: str = _get_node_name(metadata.node_labels)
        n = nodes_map.get(node_name)
        if n:
            n.append(metadata)
        else:
            nodes_map[node_name] = [metadata]


def _get_node_name(node_labels):
    _res = ""
    for label in node_labels:
        if _res != "":
            _res += ":"
        _res += label
    return _res


def _group_relationships(relationships: list) -> list:
    # Group by (source_node, rel_type, target_node, sorted rel_properties)
    grouped_data = defaultdict(lambda: [0, set()])

    for row in relationships:
        key = (tuple(row[0]), row[1], tuple(row[2]), frozenset(row[4]))
        grouped_data[key][0] += row[3]
        grouped_data[key][1].update(row[4])

    result = [
        [list(key[0]), key[1], list(key[2]), count, list(properties)]
        for key, (count, properties) in grouped_data.items()
    ]
    return result
