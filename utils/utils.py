from logging import WARN

from peewee import Model

from utils.logger import logger_md


def generate_models(table_names, meta_type):
    models = {}
    for table_name in table_names:
        # 动态创建Model类
        model_name = table_name.capitalize()
        model_meta = type('Meta', (object,), {'table_name': table_name})
        model_attrs = {
            'Meta': model_meta,
            '__module__': __name__
        }
        model: Model | meta_type | type = type(model_name, (meta_type,), model_attrs)
        models[table_name] = model

        if not model.table_exists():
            logger_md.log(
                WARN,
                f"{model} not exists, try create {table_name} in {model._meta.database.database}"
            )
            model.create_table()

    return models
