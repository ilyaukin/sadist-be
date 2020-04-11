from typing import Dict

from detailization.abstract_detailizer import AbstractDetailizer
from detailization.city_detailizer import CityDetailizer
from detailization.country_detailizer import CountryDetailizer


class GeoDetailizer(AbstractDetailizer):
    """
    Umbrella detailizer to apply city and country detailizers
    """

    threshold = 1/3
    labels = ['city', 'country']

    def __init__(self):
        self.city_detailizer = CityDetailizer()
        self.country_detailizer = CountryDetailizer()

    def get_details(self, value: str) -> Dict[str, object]:
        details = self.city_detailizer.get_details(value)
        if details:
            return details

        return self.country_detailizer.get_details(value)
