import logging
from copy import deepcopy
from typing import List

from odd_models.models import DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import VerticaGenerator


def extract_transformer_data(
    sql: str, oddrn_generator: VerticaGenerator
) -> DataTransformer:
    if sql is None:
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))

    try:
        inputs, outputs = sql_parser.get_response()
    except Exception:
        logging.error(f"Couldn't parse inputs and outputs from {sql}", exc_info=True)
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator),
        outputs=get_oddrn_list(outputs, oddrn_generator),
        sql=sql,
    )


def get_oddrn_list(tables, oddrn_generator: VerticaGenerator) -> List[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)
    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(oddrn_generator.get_oddrn_by_path("tables", table_name))
    return response
