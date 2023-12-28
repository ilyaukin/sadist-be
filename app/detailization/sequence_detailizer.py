import re
from typing import List, Union, Iterable, Dict

import pycrfsuite
from mongomoron import query

from db import conn, dl_seq_label, dl_seq, replace_grid_file, read_grid_file
from detailization.abstract_detailizer import AbstractDetailizer
from detailization.bow_detailizer import BowDetailizer


class CharType:
    WHITESPACE = 0
    DIGIT = 1
    WORD = 2
    PUNCTUATION = 3


class SequenceDetailizer(AbstractDetailizer):
    """
    A detailizer to split input into a sequence of tokens
    and match it to the sequence of labels.
    Details will be in format {"sequence": [{"token": <token>, "label": <label>}, ...]}
    """

    def __init__(self):
        self.model = None
        self.tagger = None
        self.model_name = 'seq'
        self.seq_labels = list(l['_id'] for l in conn.execute(query(dl_seq_label)))

    def learn(self, **kwargs):
        self._train_model(**kwargs)
        self._save_model()

    def get_details(self, value: str) -> Dict[str, object]:
        if not self.tagger:
            self._load_model()

        return self._predict(value)

    def split(self, s: str) -> List[str]:
        """
        Split initial string to the sequence of the so-called "tokens".
        TOKENS HERE ARE JUST PROPOSALS, ACTUAL TOKENS MAY DIFFER
        @param s: Initial string
        @return: Sequence of tokens
        """
        c_type: int
        token_type: int = 0
        token: str = ''
        token_list = []
        for c in s:
            c_type = self._get_char_type(c)
            if token_type == c_type:
                token += c
            else:
                if token:
                    token_list.append(token)
                token_type = c_type
                token = c
        if token:
            token_list.append(token)
        return token_list

    def get_default_sequence(self, s: str) -> List[dict]:
        """
        Default labeled sequence with labels such as "word", "number",
         "whitespace", "separator"
        @return: Default labeled sequence
        """
        c_to_label = {
            CharType.WORD: 'word',
            CharType.DIGIT: 'number',
            CharType.WHITESPACE: 'whitespace',
            CharType.PUNCTUATION: 'separator',
        }
        return [{'token': token, 'label': c_to_label[self._get_char_type(token)]} for
                token in self.split(s)]

    def _get_char_type(self, c: str) -> int:
        if re.match(r'\d', c):
            return CharType.DIGIT
        elif re.match(r'\w', c):
            return CharType.WORD
        elif re.match(r'\s', c):
            return CharType.WHITESPACE
        else:
            return CharType.PUNCTUATION

    def _normalize(self, token: str) -> Union[str, int]:
        """
        To limit possible amount of tokens
        let limit length of the word for each type of character,
        e.g. word with more than 20 letters is just a gibberish.
        """
        token = token.lower()
        token_type = self._get_char_type(token)
        if token_type == CharType.WHITESPACE:
            return CharType.WHITESPACE
        elif token_type == CharType.DIGIT:
            if len(token) <= 4:
                return token
            else:
                return CharType.DIGIT
        elif token_type == CharType.WORD:
            if len(token) <= 20:
                return token
            else:
                return CharType.WORD
        else:
            return token

    def _get_features(self, tokens: Iterable[str]) -> List[List[str]]:
        """
        Get features for the list of tokens
        @param tokens: List of tokens
        @return: List of features
        """

        ff = [{
            'token': token,
            'chartype': self._get_char_type(token),
            'token_normal': self._normalize(token),
            'token_len': len(token),
        } for token in tokens]
        for i, f in enumerate(ff[1:]):
            f.update(dict((f'{k}[-1]', v) for k, v in ff[i - 1].items()))
        for i, f in enumerate(ff[2:]):
            f.update(dict((f'{k}[2]', v) for k, v in ff[i - 2].items()))
        for i, f in enumerate(ff[:-1]):
            f.update(dict((f'{k}[1]', v) for k, v in ff[i + 1].items()))
        for i, f in enumerate(ff[:-2]):
            f.update(dict((f'{k}[2]', v) for k, v in ff[i + 2].items()))

        # crfsuite wants a format of '='-separated key-value strings
        return [[f'{k}={v}' for k, v in f.items()] for f in ff]

    def _get_samples(self) -> Iterable[dict]:
        return conn.execute(query(dl_seq))

    def _train_model(self, **kwargs):
        trainer = pycrfsuite.Trainer(verbose=True)
        for sample in self._get_samples():
            xseq = self._get_features(item['token'] for item in sample['labels'][0])
            yseq = [item['label'] for item in sample['labels'][0]]
            trainer.append(xseq, yseq)

        # just params from the example, not though on them
        trainer.set_params({
            'c1': .1,
            'c2': .01,
            'max_iterations': 200,
            'feature.possible_transitions': True,
        })

        trainer.train(f'/tmp/{self.model_name}.mod')

    def _save_model(self):
        with open(f'/tmp/{self.model_name}.mod', 'rb') as file:
            data = file.read()
            replace_grid_file(data, f'nn_model_{self.model}')

    def _load_model(self):
        with open(f'/tmp/{self.model_name}.mod', 'wb') as file:
            data = read_grid_file(f'nn_model_{self.model}')
            file.write(data)

        self.tagger = pycrfsuite.Tagger()
        self.tagger.open(f'/tmp/{self.model_name}.mod')

    def _predict(self, s: str):
        # TODO consider case when actual token are merged
        # of automatically split... Like 'New York'
        tokens = self.split(s)

        xpred = self._get_features(tokens)
        ypred = self.tagger.tag(xpred)

        return {'sequence': [{'token': token, 'label': label} for token, label in zip(tokens, ypred)]}
