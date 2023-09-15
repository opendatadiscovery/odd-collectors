from typing import List, Optional


class Dashboard:
    def __init__(
        self, id: str, display_name: str, datasets: Optional[List[str]] = None
    ):
        self.id = id
        self.display_name = display_name
        self.datasets = datasets or []

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            datasets=self.display_name,
        )
        return oddrn_generator.get_oddrn_by_path("dashboards")
