from typing import Dict

from app import _db
from detailization.abstract_detailizer import AbstractDetailizer


class CountryDetailizer(AbstractDetailizer):
    def get_details(self, value: str) -> Dict[str, object]:
        country = _db()['geo_country'].find_one({'name': value})
        if country:
            return {
                'country': {
                    'name': country['name'],
                    'coordinates': country['loc']['coordinates']
                }
            }
        return {}