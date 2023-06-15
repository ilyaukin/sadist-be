from mongomoron import query_one, update_one

from db import conn, app_config


class Config(dict):
    def __getitem__(self, item):
        config_data = conn.execute(query_one(app_config).filter(app_config._id == item))
        if config_data:
            return config_data['value']
        return None

    def __setitem__(self, key, value):
        conn.execute(update_one(app_config, upsert=True)
                     .filter(app_config._id == key)
                     .set({'value': value})
                     )

    def get_or_set(self, key, value):
        """
        get value if it is in config, or write a new value
        """
        ex_value = self[key]
        if ex_value is None:
            self[key] = value
            return value
        return ex_value


config = Config()
