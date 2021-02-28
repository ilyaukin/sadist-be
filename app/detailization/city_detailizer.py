import re
from typing import Dict

from db import conn, geo_city, geo_country
from detailization.abstract_detailizer import AbstractDetailizer
from mongomoron import query, query_one


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
            cursor = conn.execute(
                query(geo_city) \
                    .filter(geo_city.name == city_name_candidate)
            )

            cursor \
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

    def _get_country(self, country_code: str):
        country = self.country_cache.get(country_code)
        if country:
            return country

        country = conn.execute(
            query_one(geo_country).filter(geo_country._id == country_code)
        )
        self.country_cache[country_code] = country
        return country
