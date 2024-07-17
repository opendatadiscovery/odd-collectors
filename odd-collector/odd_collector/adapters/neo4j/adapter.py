import logging

from cachetools.func import ttl_cache
from neo4j import GraphDatabase
from odd_collector.domain.plugin import Neo4jPlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Neo4jGenerator

from .mappers.nodes import map_nodes
from .mappers.relationships import map_relationships

_find_all_nodes: str = (
    "MATCH (n) return distinct labels(n) as labels, count(*), keys(n) order by labels"
)

_find_all_nodes_relations: str = (
    "MATCH (n)-[r]-() RETURN distinct labels(n), type(r), count(*)"
)

_find_all_relationships: str = """
    MATCH (s)-[r]->(t)
    RETURN DISTINCT
        labels(s) as source_node, type(r) as rel_type, labels(t) as target_node,
        count(*) as rel_count, keys(r) as rel_properties
"""


class Adapter(BaseAdapter):
    config: Neo4jPlugin
    generator: Neo4jGenerator
    # TODO: to connect CACHE_TTL with default_pulling_interval
    #  (requires changes in odd-collector-sdk, might affect other adapters)
    CACHE_TTL = 60  # in seconds, after this time cache of _get_metadata() clears

    def __init__(self, config: Neo4jPlugin) -> None:
        super().__init__(config)

    def create_generator(self) -> Neo4jGenerator:
        return Neo4jGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    @staticmethod
    def __query(tx, cyp: str) -> list:
        return tx.run(cyp).values()

    def __execute(self, connection, cyp: str) -> list:
        try:
            with connection.session() as session:
                return session.read_transaction(self.__query, cyp)
        except Exception as e:
            logging.error(f"Failed to load metadata for query: {cyp}")
            logging.exception(e)
        return []

    def __connect(self):
        return GraphDatabase.driver(
            f"bolt://{self.config.host}:{self.config.port}",
            auth=(self.config.user, self.config.password),
            encrypted=False,
        )

    @ttl_cache(ttl=CACHE_TTL)
    def _get_metadata(self) -> dict[str, list]:
        with self.__connect() as connect:
            return {
                "nodes": self.__execute(connect, _find_all_nodes),
                "relations": self.__execute(connect, _find_all_nodes_relations),
                # TODO: to remove "relations" duplication logic
                "relationships": self.__execute(connect, _find_all_relationships),
            }

    @property
    def _nodes(self) -> list:
        return self._get_metadata()["nodes"]

    # TODO: remove this as a duplication
    @property
    def _relations(self) -> list:
        return self._get_metadata()["relations"]

    @property
    def _relationships(self) -> list:
        return self._get_metadata()["relationships"]

    @property
    def _node_entities(self) -> dict[str, DataEntity]:
        try:
            nodes, relations = self._nodes, self._relations
            return map_nodes(self.generator, nodes, relations)
        except Exception as e:
            logging.error("Failed to load metadata for nodes")
            logging.exception(e)
        return []

    @property
    def _relationship_entities(self) -> list[DataEntity]:
        try:
            node_entities, relationships = self._node_entities, self._relationships
            return map_relationships(self.generator, relationships, node_entities)
        except Exception as e:
            logging.error("Failed to load metadata for relationships")
            logging.exception(e)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[
                *self._node_entities.values(),
                *self._relationship_entities,
            ],
        )
