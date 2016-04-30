from itertools import chain
from collections import namedtuple

from sqlalchemy import select

from hiku.expr import S, define
from hiku.graph import Graph, Edge, Link
from hiku.types import StringType, IntegerType, OptionType
from hiku.sources import sqlalchemy as sa
from hiku.sources.graph import SubGraph, Expr

from .model import session, Planet, Feature, FeaturePlanet
from .model import _Climate, _Terrain


def _(string):
    return string


planets_query = sa.FieldsQuery(session, Planet.__table__)

features_query = sa.FieldsQuery(session, Feature.__table__)

to_planets_query = sa.LinkQuery(
    session,
    edge='planet',
    from_column=FeaturePlanet.__table__.c.feature_id,
    to_column=FeaturePlanet.__table__.c.planet_id,
    to_list=True,
)

to_features_query = sa.LinkQuery(
    session,
    edge='feature',
    from_column=FeaturePlanet.__table__.c.planet_id,
    to_column=FeaturePlanet.__table__.c.feature_id,
    to_list=True,
)

_GRAPH = Graph([
    Edge('planet', [
        sa.Field('id', planets_query),
        sa.Field('name', planets_query),
        sa.Field('climate', planets_query),
        sa.Field('terrain', planets_query),
        sa.Link('features', to_features_query, requires='id'),
    ]),

    Edge('feature', [
        sa.Field('id', features_query),
        sa.Field('title', features_query),
        sa.Field('director', features_query),
        sa.Field('producer', features_query),
        sa.Field('episode_num', features_query),
        sa.Field('release_date', features_query),
        sa.Link('planets', to_planets_query, requires='id'),
    ]),
])


def enum_map(attrs, mapping):
    value = namedtuple('Value', attrs)
    return {e: value(*v) for e, v in mapping.items()}


CLIMATE = enum_map(['ident', 'title'], {
    _Climate.arid: ('arid', _('Arid')),
    _Climate.temperate: ('temperate', _('Temperate')),
    _Climate.tropical: ('tropical', _('Tropical')),
})


@define(None)
def climate(value):
    if value is not None:
        return ', '.join(sorted(CLIMATE[v].title for v in value))
    return None


TERRAIN = enum_map(['ident', 'title'], {
    _Terrain.desert: ('desert', _('Desert')),
    _Terrain.grasslands: ('grasslands', _('Grasslands')),
    _Terrain.mountains: ('mountains', _('Mountains')),
})


@define(None)
def terrain(value):
    if value is not None:
        return ', '.join(sorted(TERRAIN[v].title for v in value))
    return None


def all_planets():
    rows = session.execute(select([Planet.__table__.c.id])).fetchall()
    return [r.id for r in rows]


def all_features():
    rows = session.execute(select([Feature.__table__.c.id])).fetchall()
    return [r.id for r in rows]


sg_feature = SubGraph(_GRAPH, 'feature')
sg_planet = SubGraph(_GRAPH, 'planet')


GRAPH = Graph([
    Edge('feature', [
        Expr('id', sg_feature, IntegerType, S.this.id),
        Expr('title', sg_feature, StringType, S.this.title),
        Expr('director', sg_feature, StringType, S.this.director),
        Expr('producer', sg_feature, StringType, S.this.producer),
        Expr('episode-num', sg_feature, IntegerType, S.this.episode_num),
        sa.Link('planets', to_planets_query, requires='id'),
    ]),
    Edge('planet', [
        Expr('id', sg_planet, IntegerType, S.this.id),
        Expr('name', sg_planet, StringType, S.this.name),
        Expr('climate', sg_planet, OptionType(StringType),
             climate(S.this.climate)),
        Expr('terrain', sg_planet, OptionType(StringType),
             terrain(S.this.terrain)),
        sa.Link('features', to_features_query, requires='id'),
    ]),
    Link('planets', all_planets, edge='planet', requires=None, to_list=True),
    Link('features', all_features, edge='feature', requires=None, to_list=True),
])
