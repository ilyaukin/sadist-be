from typing import List, Set, Dict, Hashable, Iterable, Tuple, Union, Optional

from app import _db, app
from bson import ObjectId
from classification.abstract_classifier import AbstractClassifier


class PatternClassifier(AbstractClassifier):
    """
    Let say we have following data:
    (1) Washington, DC
    (2) 200.000
    (3) NYC
    (4) San Diego, CA
    (5) 300k
    The idea is to classify data using patterns of how it looks
    like. Let say 0-level patterns are texts themselves,
    1-level patterns can be defined by grouping similar-looking
    characters, like
    (1) \w{10}, \w{2}
    (2) \d{3}.\d{3}
    (3) \w{3}
    (4) \w{3} \w{5}, \w{2}
    (5) \d{3}k
    Here we use pre-existing knowledge what 'letters' and 'digits'
    are.

    Idea #2 is to use likelihood of one character following anohter one.
    Assume p(a,b) is likelihood of b following a. For the set of characters
    (a[1],a[2],...,a[m]) we can calculate coupling measure 
    sum(p(a[i],a[j]),1<=i,j<=m)/m. Let start our cluster with character 
    a[1] which maximizes p(a[1],a[1]), and include to the cluster
    any character which increases coupling of the cluster, than repeat this 
    procedure to the rest of the characters etc.
    Clusters will be characters of 1-level patterns, and sequences of
    (cluster, count) will be 1-level patterns. Than we can apply dividing
    by clusters to them and get 2-level characters and patterns, until the
    next step does not diminish number of clusters.
    
    For the each pattern of each level we store number of labelled samples
    matched this pattern and number of each label, so we are able to
    check a text against all patterns and find the most reliable one.
    """

    def learn(self, i: Iterable[Tuple[str, str]]):
        self._clear_db()

        patterns: Dict[Pattern, SampleCount] = dict()

        for text, label in i:
            pattern = Pattern(level=0, sequence=text)
            patterns.setdefault(pattern, SampleCount())
            patterns[pattern].add_label(label)

        pattern_elements: Set[Tuple[Hashable, int]] = set()
        likelihood: Dict[
            Tuple[Tuple[Hashable, int], Tuple[Hashable, int]], float] = dict()

        def update_likelihood(patterns: Dict[Pattern, SampleCount]):
            pattern_elements.clear()
            likelihood.clear()
            count_prev_element_to_element: \
                Dict[Tuple[Tuple[Hashable, int], Tuple[
                    Hashable, int]], int] = dict()
            count_prev_element_total: Dict[Tuple[Hashable, int]] = dict()

            for pattern, sample_count in patterns.items():
                total = sample_count.count_total
                a: Union[None, Tuple[Hashable, int]] = None
                for b in pattern.get_pattern():
                    if a:
                        count_prev_element_to_element.setdefault((a, b), 0)
                        count_prev_element_to_element[a, b] += total
                        count_prev_element_total.setdefault(a, 0)
                        count_prev_element_total[a] += total
                    pattern_elements.add(b)
                    a = b

            for a in pattern_elements:
                for b in pattern_elements:
                    if a in count_prev_element_total.keys():
                        likelihood[a, b] = \
                            count_prev_element_to_element.get((a, b), 0) / \
                            count_prev_element_total[a]

        def coupling(cluster: Set[Tuple[Hashable, int]]):
            return sum((likelihood.get((a, b), 0))
                       for a in cluster for b in cluster) / \
                   len(cluster)

        level = 0

        while True:
            prev_level_patterns = dict((pattern, sample_count)
                                       for pattern, sample_count in
                                       patterns.items()
                                       if pattern.level == level)
            update_likelihood(prev_level_patterns)

            level += 1

            chars: List[Char] = list()
            cluster: Set[Tuple[Hashable, int]] = set()
            cluster_coupling = 0
            prev_level_pattern_element_count = len(pattern_elements)
            prev_level_pattern_element_to_char: Dict[
                Tuple[Hashable, int], Char] = \
                dict()

            def add_char(cluster: Set[Tuple[Hashable, int]]):
                char = Char(level=level, cluster=cluster)
                chars.append(char)
                for prev_level_pattern_element in cluster:
                    prev_level_pattern_element_to_char[
                        prev_level_pattern_element] = char

            while len(pattern_elements):
                pattern_element_with_max_coupling = \
                    max(pattern_elements,
                        key=lambda pattern_element: coupling(
                            cluster.union({pattern_element})))
                if coupling(cluster.union(
                    pattern_element_with_max_coupling)) >= cluster_coupling:
                    cluster.add(pattern_element_with_max_coupling)
                    cluster_coupling = coupling(cluster)
                    pattern_elements.remove(pattern_element_with_max_coupling)
                else:
                    add_char(cluster)
                    cluster = set()
                    cluster_coupling = 0
            if cluster:
                add_char(cluster)

            if not (len(chars) < prev_level_pattern_element_count):
                break

            # save current level chars
            for char in chars:
                char.save()

            for prev_level_pattern, sample_count in prev_level_patterns.items():
                pattern = Pattern(level=level,
                                  sequence=[prev_level_pattern_element_to_char[
                                                prev_level_pattern_element]
                                            for prev_level_pattern_element
                                            in
                                            prev_level_pattern.get_pattern()])
                patterns.setdefault(pattern, SampleCount())
                patterns[pattern].merge(sample_count)

        # save all level  patterns
        for pattern, sample_count in patterns.items():
            pattern.save(sample_count)

    def classify(self, s: str):
        level = 0
        s_pattern = Pattern(level=0, sequence=s)

        while True:
            # check if the s's pattern of current level
            # matches one of patterns
            patterns = Cache.get_patterns_by_level(level)
            sample_count = patterns.get(s_pattern)
            if sample_count:
                # make democracy here:
                # if some label got 50+% samples,
                # choose this label
                for label, count in sample_count.count_by_label.items():
                    if count / sample_count.count_total > .5:
                        app.logger.info('Matched s=%s by %s with vote %s' %
                                        (s, s_pattern, sample_count))
                        return label

            # make a pattern of a higher level
            level += 1
            chars = Cache.get_chars_by_level(level)
            if not chars:
                break
            sequence: List[Char] = list()
            for pattern_element in s_pattern.pattern:
                char = Cache.get_char_by_pattern_element(pattern_element)
                if not char:
                    app.logger.warn('Pattern element (%s, %s) does not match'
                                    'any char of level %d' %
                                    (pattern_element[0], pattern_element[1],
                                     level))
                    return None
                sequence.append(char)

            s_pattern = Pattern(level=level,
                                sequence=sequence)

        return None

    def _clear_db(self):
        # re-write table, todo: implement incremental update
        _db()['cl_pattern'].delete_many({})
        _db()['cl_pattern_char'].delete_many({})

        # clear cache
        Cache.clear()


class Char(object):
    """
    Character of a higher level
    """

    def __init__(self, level: int, cluster: Set[Tuple[Hashable, int]]):
        self._id = 0
        self.level = level
        self.char = frozenset(cluster)

    def __hash__(self):
        return self.char.__hash__()

    def __eq__(self, other):
        return isinstance(other, Char) and self.char == other.char

    def __repr__(self):
        return 'Char(level=%s, char=%s)' % (self.level, self.char)

    def serialize(self):
        return {
            'level': self.level,
            'char': [(self.id_or_literal(char), count)
                     for char, count in self.char]
        }

    def save(self):
        self._id = _db()['cl_pattern_char'] \
            .insert_one(self.serialize()) \
            .inserted_id

    @staticmethod
    def id_or_literal(char):
        """
        _id in db for 1+-level characters,
        literal character for 0-level
        :param char: 
        :return: 
        """
        if isinstance(char, Char):
            if not char._id:
                char.save()
            return char._id
        return char

    @staticmethod
    def deserialize(record: dict) -> 'Char':
        char = Char(level=record['level'],
                    cluster=set((Char.object_or_literal(char), count)
                                for char, count in record['char']))
        if '_id' in record:
            char._id = record['_id']
        return char

    @staticmethod
    def object_or_literal(char):
        return Cache.get_char(char) if isinstance(char, ObjectId) else char

    def __str__(self):
        return str(self.serialize())


class Pattern(object):
    def __init__(self, level: int, sequence: Iterable[Hashable]):
        self._id = 0
        self.level = level
        # sequence of chars that pattern consist of.
        # different sequences not necessarily make different
        # patterns, we can group repeating chars, e.g.
        # 'aaaaaaaaa' and 'aaaaaaaaaa' may make the same pattern.
        self.sequence = sequence

        self.pattern = self._make_pattern(sequence)

    def __hash__(self):
        return tuple(self.pattern).__hash__()

    def __eq__(self, other):
        return isinstance(other, Pattern) and self.pattern == other.pattern

    def __repr__(self):
        return 'Pattern(level=%s, sequence=%s)' % (self.level, self.sequence)

    def get_pattern(self) -> Iterable[Tuple[Hashable, int]]:
        return self.pattern

    def serialize(self, sample_count: 'SampleCount' = None):
        d = {
            'level': self.level,
            'pattern': [(Char.id_or_literal(char), count)
                        for char, count in self.pattern]
        }
        if sample_count:
            d.update(sample_count.serialize())
        return d

    def save(self, sample_count: 'SampleCount'):
        self._id = _db()['cl_pattern'] \
            .insert_one(self.serialize(sample_count=sample_count)) \
            .inserted_id

    def __str__(self):
        return str(self.serialize())

    @staticmethod
    def deserialize(record: dict) -> Tuple['Pattern', 'SampleCount']:
        pattern = Pattern(level=record['level'], sequence=())
        pattern.pattern = [(Char.object_or_literal(char), count)
                           for char, count in record['pattern']]
        if '_id' in record:
            pattern._id = record['_id']

        sample_count = SampleCount()
        sample_count.count_total = record['countTotal']
        sample_count.count_by_label = record['countByLabel']

        return pattern, sample_count

    def _make_pattern(self, sequence):
        pattern = []

        c_prev: Hashable = None
        c_prev_count = 0
        for c in Terminated(sequence):
            if c_prev and c_prev != c:
                pattern.append((c_prev, self._factorize(c_prev_count)))
                c_prev_count = 0
            c_prev = c
            c_prev_count += 1

        return pattern

    def _factorize(self, count):
        # ad-hoc algorithm of grouping the similar count values
        base = 10
        result = 1
        while count > base:
            count //= base
            result *= base
        result *= count
        return result


class SampleCount(object):
    def __init__(self):
        self.count_total = 0
        self.count_by_label = dict()

    def add_label(self, label: str):
        self.count_total += 1
        self.count_by_label.setdefault(label, 0)
        self.count_by_label[label] += 1

    def merge(self, sample_count: 'SampleCount'):
        self.count_total += sample_count.count_total
        for label, count in sample_count.count_by_label.items():
            self.count_by_label.setdefault(label, 0)
            self.count_by_label[label] += count

    def serialize(self):
        return {
            'countTotal': self.count_total,
            'countByLabel': self.count_by_label
        }

    def __str__(self):
        return str(self.serialize())


class Terminator(object):
    def __repr__(self):
        return 'Terminator()'


class Terminated(Iterable):
    def __init__(self, i: Iterable):
        self.i = i

    def __iter__(self):
        for item in self.i:
            yield item

        yield Terminator()

    def __repr__(self):
        return 'Terminated(%s)' % self.i


class Cache(object):
    """
    Cached on-demand chars, patterns, and pattern elements
    """
    chars: Dict[ObjectId, Char] = dict()
    chars_by_level: Dict[int, Set[Char]] = dict()
    patterns_by_level: Dict[int, Dict[Pattern, SampleCount]] = dict()
    pattern_element_to_char: Dict[Tuple[Hashable, int], Char] = dict()

    @staticmethod
    def get_char(_id: ObjectId) -> Char:
        if _id in Cache.chars:
            return Cache.chars[_id]

        record = _db()['cl_pattern_char'].find_one({'_id': _id})
        if not record:
            raise KeyError(_id)
        Cache.chars[_id] = Char.deserialize(record)
        return Cache.chars[_id]

    @staticmethod
    def get_chars_by_level(level: int) -> Set[Char]:
        if level in Cache.chars_by_level:
            return Cache.chars_by_level[level]

        chars = set(Char.deserialize(char) for
                    char in _db()['cl_pattern_char'].find({'level': level}))
        Cache.chars_by_level[level] = chars
        if chars:
            for char in chars:
                Cache.chars[char._id] = char
                for pattern_element in char.char:
                    Cache.pattern_element_to_char[pattern_element] = char

        return chars

    @staticmethod
    def get_patterns_by_level(level: int) -> Dict[Pattern, SampleCount]:
        if level in Cache.patterns_by_level:
            return Cache.patterns_by_level[level]

        patterns = dict(Pattern.deserialize(record)
                        for record in
                        _db()['cl_pattern'].find({'level': level}))
        Cache.patterns_by_level[level] = patterns
        return patterns

    @staticmethod
    def get_char_by_pattern_element(pattern_element: Tuple[Hashable, int]) -> \
        Optional[Char]:
        return Cache.pattern_element_to_char.get(pattern_element)

    @staticmethod
    def clear():
        Cache.chars.clear()
        Cache.chars_by_level.clear()
        Cache.patterns_by_level.clear()
        Cache.pattern_element_to_char.clear()
