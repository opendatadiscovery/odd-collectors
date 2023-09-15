import os
import re
from typing import TextIO

import yaml


class ParserError(Exception):
    ...


def parse_yaml(stream: TextIO) -> dict:
    """
    Load a yaml configuration file and resolve any environment variables
    :return: dict
    """
    loader = yaml.SafeLoader
    tag = "!ENV"
    pattern = re.compile(".*?\${(\w+)}.*?")

    loader.add_implicit_resolver(tag, pattern, None)

    def constructor(loader_, node):
        value = loader_.construct_scalar(node)
        match = pattern.findall(value)

        if match:
            for g in match:
                env_var = os.getenv(g)
                if env_var:
                    value = value.replace(f"${{{g}}}", env_var)
                else:
                    raise ParserError(
                        f"Environment variable {g} not found. Please check your environment variables."
                    )
            return value

        return value

    loader.add_constructor(tag, constructor)

    return yaml.load(stream, Loader=loader)
