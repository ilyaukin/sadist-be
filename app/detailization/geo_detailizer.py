import pickle
import re
import time
from typing import Dict, List, Tuple, Optional

import bson
import numpy as np
from mongomoron import query, update_one, query_one
from pymongo.cursor import Cursor
from tinynn.core.layer import Dense, ReLU
from tinynn.core.loss import SoftmaxCrossEntropy
from tinynn.core.model import Model
from tinynn.core.net import Net
from tinynn.core.optimizer import Adam
from tinynn.utils.data_iterator import BatchIterator

from app import logger
from db import conn, dl_geo, geo_city, geo_country, nn_model
from detailization.abstract_detailizer import AbstractDetailizer
from detailization.geo_helper import serialize_city, serialize_country


class GeoDetailizer(AbstractDetailizer):
    """
    The detailizer which is mapping geo location as a plain text
    to the city and country, details are in format {"city": {
    "name": <city name>, "coordinates": [lat, lng]}, "country": {
    "name": <country name>, "coordinates" :[lat, lng]}}

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

    def learn(self, **kwargs):
        _train_model(**kwargs)
        _save_model()

    def get_details(self, value: str) -> Dict[str, object]:
        if not model:
            _load_model()

        result = _predict(value)
        return result or _fallback(value)


wtoi_map = {}
ttoi_map = {}
targets = []
model: Optional[Model] = None


def _normalize(s: str) -> str:
    """
    Normalize the word, currently only transform to lower case,
    possibly better to remove accent here.
    """
    return s.lower()


def _get_bow(s: str) -> List[str]:
    """
    Get a bag of words by the text
    """
    words = re.split(r'[^\w]+', s)
    return [_normalize(w) for w in words if w]


def _wtoi(w: str) -> int:
    """
    Word to index. For the new word, adds it to the end.
    """
    if not w:
        raise Exception('empty word')

    if w not in wtoi_map:
        wtoi_map[w] = len(wtoi_map)
    return wtoi_map[w]


def _ttoi(t: dict) -> int:
    """
    Target to index. For the new target, adds it to the end.
    """
    key = t.get('city_id'), t.get('country_id')
    if key == (None, None):
        raise Exception('empty target')

    if key not in ttoi_map:
        ttoi_map[key] = len(ttoi_map)
    return ttoi_map[key]


def _itot(i: int) -> dict:
    """
    Index to target.
    Includes name and coordinates.
    """
    return targets[i]


def _get_sample_data() -> List[Tuple[List[int], int]]:
    """
    Get data for learning in format input, output.
    At the same time initialize wtoi and ttoi and itot
    """
    data = []
    for sample in conn.execute(query(dl_geo)):
        text = sample['text']
        target = sample['labels'][0]
        if not target:
            continue
        data.append(([_wtoi(w) for w in _get_bow(text)], _ttoi(target)))
    _init_targets()
    return data


def _train_model(**kwargs):
    sample_data = _get_sample_data()

    w_count = len(wtoi_map)
    t_count = len(ttoi_map)
    global model
    model = _create_model(w_count, t_count)
    epoch_count = kwargs.get('epoch_count', 10)
    batch_size = kwargs.get('batch_size', 30)

    iterator = BatchIterator(batch_size=batch_size)

    # because of weird tinynn API, we have to put
    # all sample instances and targets in a pre-initialized
    # 2d arrays.
    all_sample_instance = np.zeros((len(sample_data), w_count))
    all_sample_target = np.zeros((len(sample_data), t_count))
    counter = 0
    for ii, o in sample_data:
        for i in ii:
            all_sample_instance[counter][i] = 1
        all_sample_target[counter][o] = 1
        counter += 1

    for epoch in range(epoch_count):
        t = time.time()
        for sample_instance, sample_target in iterator(all_sample_instance,
                                                       all_sample_target):
            model_output = model.forward(sample_instance)
            loss, grads = model.backward(model_output, sample_target)
            model.apply_grads(grads)
        logger.info('Epoch %d took %.3fs for learning', epoch,
                    time.time() - t)


def _create_model(w_count, t_count):
    layers = [
        # Dense(num_out=w_count),
        # ReLU(),
        Dense(num_out=int(3 / 4 * w_count + 1 / 4 * t_count)),
        ReLU(),
        Dense(num_out=t_count)
    ]

    net = Net(layers)

    logger.info('Following model will be used:\n%s', net)

    return Model(net=net, loss=SoftmaxCrossEntropy(), optimizer=Adam())


def _save_model():
    maps = pickle.dumps((wtoi_map, ttoi_map))
    m = model.dumps()
    conn.execute(
        update_one(nn_model, upsert=True)
        .filter(nn_model._id == 'geo')
        .set({
            'maps': bson.Binary(maps),
            'model': bson.Binary(m),
        })
    )


def _load_model():
    global wtoi_map, ttoi_map, model
    record = conn.execute(
        query_one(nn_model)
        .filter(nn_model._id == 'geo')
    )
    if not record:
        raise Exception('No geo model in the DB')
    wtoi_map, ttoi_map = pickle.loads(record['maps'])
    model = _create_model(len(wtoi_map), len(ttoi_map))
    model.loads(record['model'])
    _init_targets()


def _predict(s: str):
    words = _get_bow(s)
    ii = []
    for w in words:
        if w in wtoi_map:
            ii.append(wtoi_map[w])
    if ii:
        test_instance = np.zeros((len(wtoi_map),))
        for i in ii:
            test_instance[i] = 1
        model_output = model.forward(test_instance)
        o = np.argmax(model_output)
        return _itot(o)
    return None


def _fallback(s: str):
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
            return serialize_city(city, add_country=True)

    return {}


def _init_targets():
    """
    Initializes `targets` list
    """
    global targets
    targets = [{} for _ in range(len(ttoi_map))]
    cities = dict((city['_id'], city) for city in conn.execute(
        query(geo_city).filter(
            geo_city._id.in_(set(k[0] for k in ttoi_map)))
    ))
    countries = dict((country['_id'], country) for country in conn.execute(
        query(geo_country).filter(
            geo_country._id.in_(set(k[1] for k in ttoi_map)))
    ))
    for k, index in ttoi_map.items():
        city_id, country_id = k
        if city_id:
            targets[index].update(serialize_city(cities[city_id]))
        if country_id:
            targets[index].update(serialize_country(countries[country_id]))
