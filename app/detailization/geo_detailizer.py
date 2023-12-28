from typing import Hashable, Any, Iterable

from mongomoron import query
from pymongo.cursor import Cursor

from db import conn, geo_city, dl_geo
from detailization.bow_detailizer import BowDetailizer


class GeoDetailizer(BowDetailizer):
    """
    A detailizer which is mapping geo location as a plain text
    to the city and country, details are in format {"city": {
    "id": <city id>, "country": {"id": <country id>}}

    Given sample data, we split all sample locations to words,
    each word is a bulb in the first layer of the model. The resulting
    city, country, or just country, is a bulb in the last layer of the model.
    We train the model against this data.

    To get detail for the location text we split that text to words,
    and, if at least one word matches the known input words, put
    it against the model. Otherwise, fallback to search by name or altname.

    Alternatively, Levenstein distance can be used instead of exact match,
    to cover typos in words.
    """

    threshold = 1 / 3
    labels = ['city', 'country']

    def __init__(self):
        super().__init__('geo')

    def _ttok(self, t: Any) -> Hashable:
        key = t.get('city_id'), t.get('country_id')
        if key == (None, None):
            raise Exception('empty target')
        return key

    def _ktot(self, k: Hashable) -> Any:
        city_id, country_id = k
        result = {}
        if city_id:
            result.update({'city': {'id': city_id}})
        if country_id:
            result.update({'country': {'id': country_id}})
        return result

    def _get_samples(self) -> Iterable[dict]:
        return conn.execute(query(dl_geo))

    def _fallback(self, s: str):
        """
        Search city/country by name
        @param s: name
        @return: Target, including name and coordinates
        """

        # if no other way to distinguish cities, cities with bigger
        # population are considered more likely
        def prioritize(cursor: Cursor):
            return cursor.sort([('population', -1)])

        city_queries = [
            query(geo_city).filter(geo_city.name == s.strip()),
            query(geo_city).filter(geo_city.alternatenames == s.lower().strip()),
        ]

        for q in city_queries:
            for city in prioritize(conn.execute(q)):
                return {'city': {'id': city['_id']}, 'country': {'id': city['country_code']}}

        return {}
