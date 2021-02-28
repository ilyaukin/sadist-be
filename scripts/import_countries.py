"""
A script to import countries to mongodb.
Country list can be downloaded from geonames:
https://download.geonames.org/export/dump/countryInfo.txt

1. Import cities, see
https://github.com/arusanov/geonames-mongo-import

2. Run this script against countryInfo.txt.
Cities info will be used to define average geo location
of each country.
"""
from argparse import ArgumentParser

from db import conn, geo_city, geo_country
from mongomoron import query, insert_many


def import_countries(filename: str):
    countries = []

    file = open(filename)
    for lin in file:
        if lin.startswith('#'):
            continue
        fields = lin.split('\t')
        code = fields[0]
        name = fields[4]
        countries.append({'_id': code, 'name': name})

    cities_by_country = dict((country['_id'], [])
                             for country in countries)

    for city in conn.execute(query(geo_city)):
        cities_by_country[city['country_code']].append(city)

    for country in countries:
        cities = cities_by_country.get(country['_id'])
        if not  cities:
            continue
        country_coordinates = \
            sum(city['loc']['coordinates'][0]
                for city in cities) / len(cities), \
            sum(city['loc']['coordinates'][1]
                for city in cities) / len(cities)
        country.update(
            {'loc': {'type': 'Point', 'coordinates': country_coordinates}})

    conn.execute(insert_many(geo_country, countries))


if __name__ == '__main__':
    argparser = ArgumentParser(description='Import countries')
    argparser.add_argument('--file', dest='filename', help='CSV file')
    args = argparser.parse_args()
    import_countries(args.filename)
