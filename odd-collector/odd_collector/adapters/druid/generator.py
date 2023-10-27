from odd_collector.adapters.druid.path_model import DruidPathsModel
from oddrn_generator import Generator
from oddrn_generator.server_models import HostnameModel


class DruidGenerator(Generator):
    source = "druid"
    paths_model = DruidPathsModel
    server_model = HostnameModel
