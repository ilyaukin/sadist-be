"""
The module contains functions to work with geo objects not
related to the particular detailizer
"""
from db import conn, geo_country
from mongomoron import query_one

country_cache = {}


def serialize_city(city: dict, add_country: bool = False) -> dict:
    """
    serializes city record
    @param city: record from `geo_city` collection
    @param add_country: if needed to add country which this city belongs to
    @return: city object for frontend
    """
    result = {
        'city': {
            'id': city['_id'],
            'name': city['name'],
            'coordinates': city['loc']['coordinates']
        }
    }
    if add_country:
        result.update(serialize_country(_get_country(city['country_code'])))
    return result


def serialize_country(country: dict) -> dict:
    """
    serializes country record
    @param country: record from `geo_country` collection
    @return: country object for frontend
    """
    return {
        'country': {
            'id': country['_id'],
            'name': country['name'],
            'coordinates': country['loc']['coordinates']
        }
    }


def _get_country(country_code: str) -> dict:
    """
    get country by code
    @param country_code: two letter country code
    @return: country record
    """
    global country_cache

    if country_code not in country_cache:
        country_cache[country_code] = conn.execute(
            query_one(geo_country).filter(geo_country._id == country_code)
        )
    return country_cache[country_code]
