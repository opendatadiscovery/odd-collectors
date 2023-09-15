from typing import Iterable

from botocore.client import BaseClient

from .paginator_config import PaginatorConfig


def fetch_paginator(conf: PaginatorConfig, client: BaseClient) -> Iterable:
    paginator = client.get_paginator(operation_name=conf.op_name)

    token = None
    while True:
        sdk_response = paginator.paginate(
            **conf.parameters,
            PaginationConfig={"MaxItems": conf.page_size, "StartingToken": token},
        )

        for entity in sdk_response.build_full_result()[conf.list_fetch_key]:
            yield (
                entity if conf.mapper is None else conf.mapper(entity, conf.mapper_args)
            )

        if sdk_response.resume_token is None:
            break

        token = sdk_response.resume_token
