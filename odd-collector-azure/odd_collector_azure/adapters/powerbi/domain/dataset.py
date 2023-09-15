from typing import List, Optional


class Dataset:
    def __init__(
        self,
        id: str,
        name: str,
        owner: Optional[str] = None,
        datasources: Optional[List[str]] = None,
    ):
        self.owner = owner
        self.id = id
        self.name = name
        self.datasources = datasources or []

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            datasets=self.name,
        )
        return oddrn_generator.get_oddrn_by_path("datasets")
