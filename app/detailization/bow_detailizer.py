import pickle
import re
from typing import Any, Optional, Dict, List, Hashable, Iterable

import numpy as np
from sklearn.neural_network import MLPClassifier

from app import logger
from db import replace_grid_file, read_grid_file
from detailization.abstract_detailizer import AbstractDetailizer


class BowDetailizer(AbstractDetailizer):
    """
    A detailizer by deep learning model over bag of words.
    Initial text is separated to the words. Each word is a '1'
    in the initial level of the model. Each classified value
    is a '1' in the last level of the model

    """
    targets: list[Any]
    wtoi_map: dict[str, int]
    ttoi_map: dict[Any, int]

    def __init__(self, model_name: str):
        self.targets = []
        self.model_name = model_name
        self.ttoi_map = {}
        self.model: Optional[MLPClassifier] = None
        self.wtoi_map = {}

    def learn(self, **kwargs):
        self._train_model(**kwargs)
        self._save_model()

    def get_details(self, value: str) -> Dict[str, object]:
        if not self.model:
            self._load_model()

        return self._predict(value)

    def _normalize(self, s: str) -> str:
        """
        Normalize the word, currently only transform to lower case,
        possibly better to remove accent here.
        """
        return s.lower()

    def _get_bow(self, s: str) -> List[str]:
        """
        Get a bag of words by the text
        """
        words = re.split(r'[^\w]+', s)
        return [self._normalize(w) for w in words if w]

    def _wtoi(self, w: str) -> int:
        """
        Word to index. For the new word, adds it to the end.
        """
        if not w:
            raise Exception('empty word')

        if w not in self.wtoi_map:
            self.wtoi_map[w] = len(self.wtoi_map)
        return self.wtoi_map[w]

    def _ttok(self, t: Any) -> Hashable:
        """
        Target to key. Convert target to hashable object
        @param t:
        @return:
        """
        raise NotImplementedError()

    def _ttoi(self, t: Any) -> int:
        """
        Target to index. For the new target, adds it to the end.
        """
        key = self._ttok(t)

        if key not in self.ttoi_map:
            self.ttoi_map[key] = len(self.ttoi_map)
            self.targets.append(self._ktot(key))
        return self.ttoi_map[key]

    def _itot(self, i: int) -> Any:
        """
        Index to target.
        """
        return self.targets[i]

    def _ktot(self, k: Hashable) -> Any:
        """
        Key to target
        @param k: key returned by `_ttok`
        @return: Target object to be used as a classified (a.k.a detailed) value
        """
        raise NotImplemented()

    def _get_samples(self) -> Iterable[dict]:
        """
        Return labelled data as iterable of {'text': ..., 'labels': [...]},
        in the same format as labelling interface saves
        @return:
        """
        raise NotImplemented()

    def _warmup_instance(self, s: str) -> Any:
        """
        Consider new sample of instance
        @param s: Sample instance
        @return: Any mediatory result that will be used as cache in `_encode_instance`
        """
        return [self._wtoi(w) for w in self._get_bow(s)]

    def _warmup_target(self, t: Any) -> Any:
        return self._ttoi(t)

    def _encode_instance(self, s: str, cache: Any=None) -> Optional[np.ndarray]:
        ii = cache or [self.wtoi_map[w] for w in self._get_bow(s) if w in self.wtoi_map]
        if not ii:
            return None
        w_count = len(self.wtoi_map)
        instance = np.zeros(w_count)
        for i in ii:
            instance[i] = 1
        return instance

    def _encode_target(self, t: Any, cache: Any=None) -> Any:
        if (cache is None):
            raise Exception('Call _warmup_target first!')
        # scikit-learn use solo index as a target
        return cache

    def _decode_target(self, target: Any) -> Any:
        return self._itot(target)

    def _train_model(self, **kwargs):

        # convert samples to tuples (text, label, instance cache, target cache)
        samples = [(sample['text'], sample['labels'][0],
                    self._warmup_instance(sample['text']), self._warmup_target(sample['labels'][0]))
                   for sample in self._get_samples() if sample['labels'][0]]

        # move all instances and targets to 2d arrays
        sample_instance = np.array([self._encode_instance(s, c) for s, _, c, _ in samples])
        sample_target = np.array([self._encode_target(t, c) for _, t, _, c in samples])

        # create and train model
        self.model = self._create_model(**kwargs)
        self.model.fit(sample_instance, sample_target)

    def _create_model(self, **kwargs):
        w_count = len(self.wtoi_map)
        t_count = len(self.ttoi_map)
        epoch_count = kwargs.get('epoch_count', 100)
        batch_size = kwargs.get('batch_size', 30)

        model = MLPClassifier(
            solver='adam' if w_count > 2000 else 'lbfgs',
            activation='relu',
            random_state=0,
            max_iter=epoch_count,
            batch_size=batch_size,
            hidden_layer_sizes=[int(3 / 4 * w_count + 1 / 4 * t_count)],
            verbose=True,
        )

        logger.info('Following model will be used:\n%s', model)

        return model

    def _save_model(self):
        wtoi_data = pickle.dumps(self.wtoi_map)
        ttoi_data = pickle.dumps(self.ttoi_map)
        model_data = pickle.dumps(self.model)
        replace_grid_file(wtoi_data, f'nn_map_input_{self.model_name}')
        replace_grid_file(ttoi_data, f'nn_map_target_{self.model_name}')
        replace_grid_file(model_data, f'nn_model_{self.model_name}')

    def _load_model(self):
        wtoi_data = read_grid_file(f'nn_map_input_{self.model_name}')
        ttoi_data = read_grid_file(f'nn_map_target_{self.model_name}')
        model_data = read_grid_file(f'nn_model_{self.model_name}')
        if not wtoi_data or not ttoi_data or not model_data:
            raise Exception(f'No {self.model_name} model in the DB')
        self.wtoi_map = pickle.loads(wtoi_data)
        self.ttoi_map = pickle.loads(ttoi_data)
        self.model = pickle.loads(model_data)

        self.targets = len(self.ttoi_map) * [None]
        for k, index in self.ttoi_map.items():
            self.targets[index] = self._ktot(k)

    def _predict(self, s: str):
        test_instance0 = self._encode_instance(s)
        if test_instance0 is not None:
            test_instance = np.array([test_instance0])
            model_output = self.model.predict(test_instance)
            return self._decode_target(model_output[0])
        return self._fallback(s)

    def _fallback(self, s: str):
        return None
