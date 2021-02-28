from typing import Dict

from db import conn, geo_country
from detailization.abstract_detailizer import AbstractDetailizer
from mongomoron import query_one


class CountryDetailizer(AbstractDetailizer):
    def get_details(self, value: str) -> Dict[str, object]:
        country = conn.execute(
            query_one(geo_country).filter(geo_country.name == value)
        )

        if country:
            return {
                'country': {
                    'name': country['name'],
                    'coordinates': country['loc']['coordinates']
                }
            }
        return {}
