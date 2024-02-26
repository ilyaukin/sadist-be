from dataclasses import dataclass
from typing import Dict, Union, Optional, Any, List, Iterable, Tuple

import mongomoron.mongomoron
import pymongo
from bson import ObjectId
from mongomoron import document, aggregate, cond, dict_

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
class VizProps(DO):
    col: str
    label: Optional[str]
    action: str
    predicate: Union[EqPredicate, InPredicate] = None
    accumulater: Optional[str] = None


@dataclass
class Visualization(DO):
    key: str
    type: str
    props: VizProps
    stringrepr: str
    labelselector: str
    children: Optional[Dict[str, 'Visualization']] = None


@dataclass
class MultiselectFilterProposal(DO):
    col: str
    label: str
    values: List[Any]
    selected: List[Any]
    labelselector: str
    valueselector: str
    valuefield: str
    type: str = 'multiselect'


@dataclass
class SearchFilterProposal(DO):
    term: Any
    type: str = 'search'


Filtering = Union[MultiselectFilterProposal, SearchFilterProposal]


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

    def join(self, p: mongomoron.mongomoron.AggregationPipelineBuilder,
             local_field: mongomoron.mongomoron.Field) -> mongomoron.mongomoron.AggregationPipelineBuilder:
        """
        In an aggregation pipeline, join data corresponding to the category
        @param p: Pipeline
        @param local_field: Field in pipeline which defines a category info. Usually
        it's in format {id: <_id in the dictionary>}

        @return: the same pipeline
        """
        return p

    @staticmethod
    def join_by_dict(p: mongomoron.mongomoron.AggregationPipelineBuilder,
                     local_field: mongomoron.mongomoron.Field,
                     collection: mongomoron.Collection,
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


@Category.sub('city')
class CityCategory(Category):
    def join(self, p: mongomoron.mongomoron.AggregationPipelineBuilder,
             local_field: mongomoron.mongomoron.Field) -> mongomoron.mongomoron.AggregationPipelineBuilder:
        return self.join_by_dict(p, local_field, geo_city, dict(id='_id', name='name', loc='loc'))

    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [Visualization(
            key=f'{col} city',
            type='globe',
            props=VizProps(action='group', col=col, label='city'),
            stringrepr='Show cities',
            labelselector='id.name')
        ]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        return [MultiselectFilterProposal(
            col=col,
            label='city',
            values=self.get_values(ds_list_record['_id'], col),
            selected=[],
            labelselector='name',
            valueselector='id',
            valuefield='city.id')
        ]


@Category.sub('country')
class CountryCategory(Category):
    def join(self, p: mongomoron.mongomoron.AggregationPipelineBuilder,
             local_field: mongomoron.mongomoron.Field) -> mongomoron.mongomoron.AggregationPipelineBuilder:
        return self.join_by_dict(p, local_field, geo_country, dict(id='_id', name='name', loc='loc'))

    def get_visualization(self, ds_list_record: dict, col: str) -> List[Visualization]:
        return [Visualization(
            key=f'{col} country',
            type='globe',
            props=VizProps(action='group', col=col, label='country'),
            stringrepr='Show countries',
            labelselector='id.name')
        ]

    def get_filtering(self, ds_list_record: dict, col: str) -> List[Filtering]:
        return [MultiselectFilterProposal(
            col=col,
            label='country',
            values=self.get_values(ds_list_record['_id'], col),
            selected=[],
            labelselector='name',
            valueselector='id',
            valuefield='country.id')
        ]
