import datetime
import math
from dataclasses import dataclass
from numbers import Number
from typing import Dict, Union, Optional, Any, List, Iterable, Tuple

import pymongo
from bson import ObjectId
from dateutil.relativedelta import relativedelta
from mongomoron import *

from db import geo_city, geo_country, ds_classification, conn
from serializer import DO
from singleton_mixin import SingletonMixin


@dataclass
class EqPredicate(DO):
    op = 'eq'
    value: Any


@dataclass
class InPredicate(DO):
    op = 'in'
    values: list


@dataclass
class InRangePredicate(DO):
    op = 'inrange'
    float_ = Union[int, float]
    range_min: Number
    range_max: Number


@dataclass
class GtPredicate(DO):
    op = 'gt'
    value: Number


@dataclass
class GtePredicate(DO):
    op = 'gte'
    value: Number


@dataclass
class LtPredicate(DO):
    op = 'lte'
    value: Number


@dataclass
class LtePredicate(DO):
    op = 'lte'
    value: Number


@dataclass
class OrPredicate(DO):
    op = 'or'
    expression: List['Predicate']


@dataclass
class AndPredicate(DO):
    op = 'and'
    expression: List['Predicate']


@dataclass
class NotPredicate(DO):
    op = 'not'
    expression: 'Predicate'


Predicate = Union[EqPredicate, InPredicate, InRangePredicate, \
    GtPredicate, GtePredicate, LtPredicate, LtePredicate, OrPredicate, \
    AndPredicate, NotPredicate]


@dataclass
class RangeReducer(DO):
    type = 'range'
    min: Optional[float] = None
    max: Optional[float] = None
    step: Optional[float] = None


@dataclass
class AccumulateProps(DO):
    action = 'accumulate'
    col: str
    label: Optional[str]
    predicate: Optional[Predicate] = None
    accumulater: Optional[str] = None


@dataclass
class GroupProps(DO):
    action = 'group'
    col: str
    label: Optional[str]
    predicate: Optional[Predicate] = None
    reducer: Optional[RangeReducer] = None


VizProps = Union[AccumulateProps, GroupProps]


@dataclass
class VizBaseMeta(DO):
    key: str
    stringrepr: Optional[str]


@dataclass
class VizGraphMeta(VizBaseMeta):
    type: str
    props: GroupProps
    children: Dict[str, 'VizMeta']
    labelselector: Optional[str] = None


@dataclass
class VizPointMeta(VizBaseMeta):
    props: AccumulateProps


VizMeta = Union[VizGraphMeta, VizPointMeta]


Visualization = VizMeta


@dataclass
class MultiselectFilterProposal(DO):
    col: str
    label: str
    values: List[Any]
    selected: List[Any]
    labelselector: str = None
    valueselector: str = None
    valuefield: str = None
    type: str = 'multiselect'


@dataclass
class RangeFilterProposal(DO):
    col: str
    label: str
    min: Number
    max: Number
    labelformat: str = None
    type: str = 'range'


@dataclass
class SearchFilterProposal(DO):
    term: Any
    type: str = 'search'


Filtering = Union[MultiselectFilterProposal, RangeFilterProposal, SearchFilterProposal]


class Category(SingletonMixin):
    """
    Category is a class. That means, for a label, assigned during
    classification, it can be a corresponding category, which is
    responsible for working with details of this class of data.
    E.g., for currency it can convert all to a single one.
    For timestamp, format it etc. etc. etc.

    Category itself contains common methods for working with
    categories. This likely be extended in the future.
    """

    def __init__(self):
        self.label = self.__class__.__key__

    @staticmethod
    def by_label(label: str) -> Optional['Category']:
        """
        Get category object by label. Difference with `get()` is
        that label can refer to a deeper field, such as `city.id`.
        This method gets a category related to the field in the
        first level of details
        @param label: Label (~ a DB field in the classification
        collection relative to details)
        @return: Category or None
        """
        ff = label.split('.')
        return Category.get(ff[0])

    def join(self, p: AggregationPipelineBuilder,
             local_field: Field) -> AggregationPipelineBuilder:
        """
        In an aggregation pipeline, join data corresponding to the category
        @param p: Pipeline
        @param local_field: Field in pipeline which defines a category info. Usually
        it's in format {id: <_id in the dictionary>}

        @return: the same pipeline
        """
        return p

    @staticmethod
    def join_by_dict(p: AggregationPipelineBuilder,
                     local_field: Field,
                     collection: Collection,
                     d: Dict[str, str]):
        """
        Common implementation `join` by dictionary collection
        @param local_field:
        @param p: Aggregation pipeline
        @param collection: Dictionary collection
        @param d: Mapping of collection fields to the result fields
        @return: the same pipeline
        """
        return p.lookup(collection, local_field=local_field.id, foreign_field='_id', as_='f1') \
            .add_fields(f2=document.f1[0]) \
            .add_fields(**{local_field._name: cond(
            local_field,
            dict_(**dict((name, document.f2.get_field(value)) for name, value in d.items())),
            None)}) \
            .unset('f1', 'f2')

    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        """
        Get all suggested visualization for given DS, column and label
        @param ds_list_record: DS list record
        @param col: Column name
        @return: Visualization list
        """
        return []

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        """
        Get all suggested filtering for given DS, column and label
        @param ds_list_record: DS list record
        @param col Column name
        @return: Filtering list
        """
        return []

    @staticmethod
    def get_all_visualization(ds_list_record: dict) -> Dict[str, List[Visualization]]:
        """
        Get all suggested visualization for all columns according to assigned labels
        @param ds_list_record: DS list record
        @return: Map of suggested visualization for each column
        """
        result = {}
        for col, label, category in Category.iter_categories(ds_list_record):
            result.setdefault(col, [])
            result[col] += category.get_visualization(ds_list_record, col)
        return result

    @staticmethod
    def get_all_filtering(ds_list_record: dict) -> Dict[str, List[Filtering]]:
        """
        Get all suggested filtering for all columns according to assigned labels
        @param ds_list_record: DS list record
        @return: Map of suggested filtering for each column
        """
        result = {}
        for col, label, category in Category.iter_categories(ds_list_record):
            result.setdefault(col, [])
            result[col] += category.get_filtering(ds_list_record, col)
        return result

    @staticmethod
    def iter_categories(ds_list_record: dict) -> Iterable[Tuple[str, str, 'Category']]:
        """
        Iterate known categories of given DS in format col, label, category
        @param ds_list_record: DS list record
        @return: Iterable of tuples col, label, category
        """
        d = ds_list_record.get('detailization')
        if isinstance(d, dict):
            for col, details in d.items():
                if isinstance(details, dict) and 'labels' in details \
                        and isinstance(details.get('labels'), list):
                    for label in details.get('labels'):
                        category = Category.get(label)
                        if category:
                            yield col, label, category

    def get_values(self, ds_id: Union[str, ObjectId], col: str) -> List[Any]:
        """
        Return values of this category in the given column of given DS
        @param ds_id: DS id
        @param col: Column name
        @return: List of values
        """
        p = aggregate(ds_classification[ds_id]) \
            .match(document.col == col) \
            .project(v=document.details.get_field(self.label)) \
            .match(document.v.exists())
        self.join(p, document.v)

        # group to make unique values
        p.group(document.v)

        # if category has `name` field, sort alphabetically
        p.sort((document._id.name, pymongo.ASCENDING))

        return list(record['_id'] for record in conn.execute(p))

    def get_boundaries(self, ds_id: Union[str, ObjectId], col: str, label: Optional[str] = None) \
            -> Tuple[float, float]:
        """
        Get effective boundaries for the numerical values, all values
        outside them will consider outliers.

        IQR method is used, i.e. range of the central half of values, plus
        1.5 of width of this range to left and to right are included.
        @see https://en.wikipedia.org/wiki/Interquartile_range
        @param ds_id: DS id
        @param col: Column name
        @param label: Field path relative to details. Defaulting to self.label
        @return: Tuple min, max
        """
        field = document.details.get_field(label or self.label)
        p = aggregate(ds_classification[ds_id]) \
            .match(document.col == col). \
            group(None, min=min_(field), max=max_(field), p=percentile(field, [.25, .75]))
        for record in conn.execute(p):
            abs_min = record['min']
            abs_max = record['max']
            p_min = record['p'][0]
            p_max = record['p'][1]
            center = .5 * (p_min + p_max)
            b_min = max(center - 2 * (p_max - p_min), abs_min)
            b_max = min(center + 2 * (p_max - p_min), abs_max)
            # add a shift here because an upper border is non-inclusive in grouping
            return b_min, b_max + .01 * (b_max - b_min)

    def get_ranges(self, ds_id: Union[str, object], col: str, reducer: dict) -> \
            Optional[List[Tuple[Tuple[float, float], str]]]:
        """
        Get ranges for the grouping of numerical values by ranges.
        @param ds_id: DS id
        @param col: Column name
        @param reducer: RangeReducer passed from the frontend.
        TODO cope with DO deserialization and make it a proper object type
        @return: List of ranges in format ((min, max), range id)
        """
        return None


@Category.sub('datetime')
class DatetimeCategory(Category):
    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [VizGraphMeta(
            key=f'{col} timeline',
            stringrepr='Show timeline',
            type='histogram',
            props=GroupProps(
                col=col,
                label='datetime.timestamp',
                reducer=RangeReducer()
            ),
            labelselector='id.name',
            children={},
        )]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        b_min, b_max = self.get_boundaries(ds_list_record['_id'], col, 'datetime.timestamp')
        return [RangeFilterProposal(
            col=col,
            label='datetime.timestamp',
            min=b_min,
            max=b_max,
            labelformat='datetime',
        )]

    def get_ranges(self, ds_id: Union[str, object], col: str, reducer: dict) -> \
            Optional[List[Tuple[Tuple[float, float], str]]]:
        if 'min' not in reducer or 'max' not in reducer:
            timestamp_min, timestamp_max = self.get_boundaries(ds_id, col, 'datetime.timestamp')

        # define boundaries
        timestamp_min = reducer.get('min', timestamp_min)
        timestamp_max = reducer.get('max', timestamp_max)

        # define a step within boundaries, to match
        # natural time steps (years, months...)
        steps = [
            (1, '%d %b %Y %H:%M:%S', relativedelta(seconds=1)),
            (60, '%d %b %Y %H:%M', relativedelta(minutes=1)),
            (3600, '%d %b %Y %I%p', relativedelta(hours=1)),
            (86400, '%d %b %Y', relativedelta(days=1)),
            (2629800, '%b %Y', relativedelta(months=1)),
            (31557600, '%Y', relativedelta(years=1)),
        ]
        for step in steps:
            step_size, _, _ = step
            if (timestamp_max - timestamp_min) / step_size <= 100:
                break

        # fill ranges with given step
        ranges = []
        _, fmt, delta = step
        t = datetime.datetime.fromtimestamp(timestamp_min)
        while t.timestamp() < timestamp_max:
            interval = t.strftime(fmt)
            t = datetime.datetime.strptime(interval, fmt)
            interval_start = t.timestamp()

            t += delta
            interval_end = t.timestamp()

            ranges.append(((interval_start, interval_end), interval))

        return ranges


@Category.sub('city')
class CityCategory(Category):
    def join(self, p: AggregationPipelineBuilder,
             local_field: Field) -> AggregationPipelineBuilder:
        return self.join_by_dict(p, local_field, geo_city, dict(id='_id', name='name', loc='loc'))

    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [VizGraphMeta(
            key=f'{col} city',
            stringrepr='Show cities',
            type='globe',
            props=GroupProps(col=col, label='city'),
            labelselector='id.name',
            children={},
        )]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        return [MultiselectFilterProposal(
            col=col,
            label='city',
            values=self.get_values(ds_list_record['_id'], col),
            selected=[],
            labelselector='name',
            valueselector='id',
            valuefield='city.id'
        )]


@Category.sub('country')
class CountryCategory(Category):
    def join(self, p: AggregationPipelineBuilder,
             local_field: Field) -> AggregationPipelineBuilder:
        return self.join_by_dict(p, local_field, geo_country, dict(id='_id', name='name', loc='loc'))

    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [VizGraphMeta(
            key=f'{col} country',
            stringrepr='Show counties',
            type='globe',
            props=GroupProps(col=col, label='country'),
            labelselector='id.name',
            children={},
        )]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        return [MultiselectFilterProposal(
            col=col,
            label='country',
            values=self.get_values(ds_list_record['_id'], col),
            selected=[],
            labelselector='name',
            valueselector='id',
            valuefield='country.id'
        )]


@Category.sub('number')
class NumberCategory(Category):
    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [VizPointMeta(
            key=f'{col} average',
            stringrepr='Show average',
            props=AccumulateProps(col=col, label='number', accumulater='avg'),
        ), VizPointMeta(
            key=f'{col} median',
            stringrepr='Show median',
            props=AccumulateProps(col=col, label='number', accumulater='median'),
        ), VizGraphMeta(
            key=f'{col} distribution',
            stringrepr='Show distribution',
            type='histogram',
            props=GroupProps(col=col, label='number', reducer=RangeReducer()),
            labelselector='id.name',
            children={},
        )]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        b_min, b_max = self.get_boundaries(ds_list_record['_id'], col, 'number')
        return [RangeFilterProposal(
            col=col,
            label='number',
            min=b_min,
            max=b_max,
        )]

    def get_ranges(self, ds_id: Union[str, object], col: str, reducer: dict) -> \
            Optional[List[Tuple[Tuple[float, float], str]]]:
        if 'min' not in reducer or 'max' not in reducer:
            b_min, b_max = self.get_boundaries(ds_id, col, 'number')

        # define boundaries
        b_min = reducer.get('min', b_min)
        b_max = reducer.get('max', b_max)

        # define step within boundaries to be a round number
        # as much as possible
        p = int(math.log(b_max - b_min, 10)) - 1
        step = 10 ** p
        b = b_min - (b_min % step)
        ranges = []
        while b < b_max:
            ranges.append(((b, b + step), f'{b} \u2014 {b + step}'))
            b += step

        return ranges


@Category.sub('money')
class MoneyCategory(NumberCategory):
    # TODO convert currency etc...
    pass


@Category.sub('gender')
class GenderCategory(Category):
    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [VizGraphMeta(
            key=f'{col} gender',
            stringrepr='Show distribution',
            type='histogram',
            props=GroupProps(col=col, label='gender'),
            labelselector='id',
            children={},
        )]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        return [MultiselectFilterProposal(
            col=col,
            label='gender',
            values=self.get_values(ds_list_record['_id'], col),
            selected=[],
        )]
