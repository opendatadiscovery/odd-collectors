import copy
from json import loads
from typing import Any, Dict, List

import requests

from odd_collector_aws.adapters.dms.mappers.endpoints import EndpointEngine


class SelectionMappingRule:
    def __init__(self, rules_node: Dict[str, Any]):
        self.rule_id: str = rules_node["rule-id"]

        object_locator = rules_node["object-locator"]

        self.schema_name: str = (
            None
            if object_locator["schema-name"] == "%"
            else object_locator["schema-name"]
        )
        self.table_name: str = (
            None
            if object_locator["table-name"] == "%"
            else object_locator["table-name"]
        )
        self.include: bool = False if rules_node["rule-action"] == "exclude" else True


class EntitiesExtractor:
    def __init__(
        self,
        rules_nodes: List[Dict[str, Any]],
        platform_host_url: str,
        endpoint_engine: EndpointEngine,
    ):
        self.endpoint_engine = endpoint_engine
        self.platform_host_url = platform_host_url
        self.rules_nodes = rules_nodes

    def __request_items_by_deg(self, oddrn: str) -> List[Dict[str, str]]:
        url = f"{self.platform_host_url}/ingestion/entities/degs/children"
        params = {"oddrn": oddrn}
        resp = requests.get(url=url, params=params)
        return loads(resp.content)["items"]

    def __extract_tables_oddrns_from_items(
        self, deg_oddrn: str, accum_list: List[str]
    ) -> List[str]:
        items_from_request = self.__request_items_by_deg(deg_oddrn)
        if len(items_from_request) == 0:
            return []
        for item in items_from_request:
            entity_type = item["type"]
            if entity_type == "TABLE":
                accum_list.append(item["oddrn"])
            else:
                if entity_type == "DATABASE_SERVICE":
                    self.__extract_tables_oddrns_from_items(
                        deg_oddrn=item["oddrn"], accum_list=accum_list
                    )
                else:
                    raise NotImplementedError("not implemented entity type yet")

        return accum_list

    def __create_selection_rules_list(self) -> List[SelectionMappingRule]:
        return [
            SelectionMappingRule(rule_node)
            for rule_node in self.rules_nodes
            if rule_node["rule-type"] == "selection"
        ]

    def __one_schema_one_table_strategy(
        self, schema_name: str, table_name: str
    ) -> List[str]:
        """
        returns one table from one schema
        """
        gen = copy.copy(self.endpoint_engine.get_generator())
        table_oddrn = self.endpoint_engine.get_oddrn_for_table_schema_names(
            gen, schema_name, table_name
        )
        schema_oddrn = self.endpoint_engine.get_oddrn_for_schema_name(gen, schema_name)
        accum = []
        tables_oddrns_in_platform = self.__extract_tables_oddrns_from_items(
            schema_oddrn, accum
        )
        if table_oddrn in tables_oddrns_in_platform:
            return [table_oddrn]
        return []

    def __one_schema_all_tables_strategy(self, schema_name: str) -> List[str]:
        """
        returns all tables from one schema

        """
        gen = copy.copy(self.endpoint_engine.get_generator())
        gen.set_oddrn_paths(**{self.endpoint_engine.schemas_path_name: schema_name})
        schema_oddrn = self.endpoint_engine.get_oddrn_for_schema_name(gen, schema_name)
        accum = []
        tables_oddrns_in_platform = self.__extract_tables_oddrns_from_items(
            schema_oddrn, accum
        )
        return tables_oddrns_in_platform

    def __all_schemas_one_table_strategy(self, table_name: str) -> List[str]:
        """
        returns one table with equal name from all schemas
        """
        tables_in_platform = self.__all_strategy()
        gen = copy.copy(self.endpoint_engine.get_generator())
        db_deg_oddrn = gen.get_data_source_oddrn()
        items = self.__request_items_by_deg(db_deg_oddrn)
        schemas_oddrns_in_platform: List[str] = []
        for item in items:
            if item["type"] == "DATABASE_SERVICE":
                schemas_oddrns_in_platform.append(item["oddrn"])
            else:
                raise NotImplementedError("not database")

        tables_oddrns_to_return: List[str] = []
        for schema_oddrn in schemas_oddrns_in_platform:
            table_oddrn = self.endpoint_engine.extend_schema_oddrn_with_table_name(
                schema_oddrn, table_name
            )
            if table_oddrn in tables_in_platform:
                tables_oddrns_to_return.append(table_oddrn)

        return tables_oddrns_to_return

    def __all_strategy(self) -> List[str]:
        """

        returns all tables from all schemas
        """
        gen = copy.copy(self.endpoint_engine.get_generator())
        db_deg_oddrn = gen.get_data_source_oddrn()
        accum = []
        tables_oddrns_in_platform = self.__extract_tables_oddrns_from_items(
            db_deg_oddrn, accum
        )
        return tables_oddrns_in_platform

    def __get_oddrns_based_on_rule(self, rule: SelectionMappingRule) -> List[str]:
        if rule.schema_name is not None:
            if rule.table_name is not None:
                return self.__one_schema_one_table_strategy(
                    rule.schema_name, rule.table_name
                )
            else:
                return self.__one_schema_all_tables_strategy(rule.schema_name)
        else:
            if rule.table_name is not None:
                return self.__all_schemas_one_table_strategy(rule.table_name)
            else:
                return self.__all_strategy()

    def get_oddrns_list(self):
        all_tables_oddrns = self.__all_strategy()
        include_oddrns: List[str] = []
        exclude_oddrns: List[str] = []
        rules_list = self.__create_selection_rules_list()
        for rule in rules_list:
            oddrns = self.__get_oddrns_based_on_rule(rule)
            if rule.include:
                include_oddrns += oddrns
            else:
                exclude_oddrns += oddrns

        oddrns_after_exclusion = [
            oddrn for oddrn in all_tables_oddrns if oddrn not in exclude_oddrns
        ]
        if len(include_oddrns) == 0:
            return oddrns_after_exclusion
        return [oddrn for oddrn in include_oddrns if oddrn in oddrns_after_exclusion]
