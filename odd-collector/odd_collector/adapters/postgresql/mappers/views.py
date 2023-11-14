from funcy import lmap, partial, silent
from odd_models import DataEntity, DataEntityType, DataSet
from odd_models.models import DataTransformer
from oddrn_generator import PostgresqlGenerator

from ..models import Table
from .columns import map_column
from .metadata import get_table_metadata
from .utils import has_vector_column


def map_view(generator: PostgresqlGenerator, view: Table):
    generator.set_oddrn_paths(
        **{"schemas": view.table_schema, "views": view.table_name}
    )
    map_view_column = partial(map_column, generator=generator, path="views")

    # If view contains vector column we consider it as a vector store, otherwise - an ordinary view
    data_entity_type = (
        DataEntityType.VECTOR_STORE if has_vector_column(view.columns)
        else DataEntityType.VIEW
    )

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=data_entity_type,
        owner=view.table_owner,
        description=view.description,
        metadata=[get_table_metadata(entity=view)],
        dataset=DataSet(
            rows_number=silent(int)(view.table_rows),
            field_list=lmap(map_view_column, view.columns),
        ),
        data_transformer=DataTransformer(
            sql=view.view_definition, inputs=[], outputs=[]
        ),
    )
