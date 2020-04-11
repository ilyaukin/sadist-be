import re
from typing import Dict

from app import _db
from detailization.abstract_detailizer import AbstractDetailizer


class CityDetailizer(AbstractDetailizer):
    country_cache = {}

    def get_details(self, value: str) -> Dict[str, object]:
        city_name_candidates = []
        comma_separated_parts = re.split(r'\s*,\s*', value)
        if len(comma_separated_parts) > 1:
            city_name_candidates.append(comma_separated_parts[0])
        else:
            words = re.split(r'\s+', value)
            for n in reversed(range(max(len(words), 3))):
                city_name_candidates.append(' '.join(words[:n + 1]))
        for city_name_candidate in city_name_candidates:
            # todo consider other parts to distinguish
            # cities with the same name in different countries
            cursor = _db()['geo_city'].find({'name': city_name_candidate}) \
                .sort([('population', -1)]) \
                .limit(1)
            for city in cursor:
                country = self._get_country(city['country_code'])
                return {
                    'city': {
                        'name': city['name'],
                        'coordinates': city['loc']['coordinates']
                    },
                    'country': {
                        'name': country['name'],
                        'coordinates': country['loc']['coordinates']
                    }
                }
        return {}

    def _get_country(self, country_code):
        country = self.country_cache.get(country_code)
        if country:
            return country

        country = _db()['geo_country'].find_one({'_id': country_code})
        self.country_cache[country_code] = country
        return country
