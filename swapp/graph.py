from itertools import chain
from collections import namedtuple

from sqlalchemy import select

from hiku.expr import Expr, S, define
from hiku.graph import Graph, Edge, link
from hiku.types import StringType, IntegerType, OptionType
from hiku.sources.graph import subquery_fields
from hiku.sources.sqlalchemy import fields as sa_fields, link as sa_link

from .model import session, Planet, Feature, FeaturePlanet
from .model import _Climate, _Terrain


def _(string):
    return string


@sa_link.many('feature', session,
              from_=FeaturePlanet.__table__.c.planet_id,
              to=FeaturePlanet.__table__.c.feature_id)
def planet_to_features(expr):
    return expr


@sa_link.many('planet', session,
              from_=FeaturePlanet.__table__.c.feature_id,
              to=FeaturePlanet.__table__.c.planet_id)
def feature_to_planets(expr):
    return expr


_GRAPH = Graph([
    Edge('planet', chain(
        sa_fields(session, Planet.__table__, [
            'id',
            'name',
            'climate',
            'terrain',
        ]),
        [
            planet_to_features('features', requires='id'),
        ],
    )),
    Edge('feature', chain(
        sa_fields(session, Feature.__table__, [
            'id',
            'title',
            'director',
            'producer',
            'episode_num',
            'release_date',
        ]),
        [
            feature_to_planets('planets', requires='id'),
        ],
    )),
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


@link.many('planet', requires=False)
def planets():
    rows = session.execute(select([Planet.__table__.c.id])).fetchall()
    return [r.id for r in rows]


@link.many('feature', requires=False)
def features():
    rows = session.execute(select([Feature.__table__.c.id])).fetchall()
    return [r.id for r in rows]


GRAPH = Graph([
    Edge('feature', chain(
        subquery_fields(_GRAPH, 'feature', [
            Expr('id', IntegerType, S.this.id),
            Expr('title', StringType, S.this.title),
            Expr('director', StringType, S.this.director),
            Expr('producer', StringType, S.this.producer),
            Expr('episode-num', IntegerType, S.this.episode_num),
        ]),
        [
            feature_to_planets('planets', requires='id'),
        ],
    )),
    Edge('planet', chain(
        subquery_fields(_GRAPH, 'planet', [
            Expr('id', IntegerType, S.this.id),
            Expr('name', StringType, S.this.name),
            Expr('climate', OptionType(StringType), climate(S.this.climate)),
            Expr('terrain', OptionType(StringType), terrain(S.this.terrain)),
        ]),
        [
            planet_to_features('features', requires='id'),
        ],
    )),
    planets('planets'),
    features('features'),
])
