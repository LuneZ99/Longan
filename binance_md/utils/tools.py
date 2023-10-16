from peewee import Model


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
        # 确保连接到数据库
        # model._meta.database.connect()
        # 如果表不存在，创建它
        model.create_table(safe=True)

    return models
