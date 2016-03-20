from itertools import chain
from collections import namedtuple

from sqlalchemy import select

from hiku.expr import Expr, S, define
from hiku.graph import Graph, Edge, Link
from hiku.types import StringType, IntegerType
from hiku.sources.graph import subquery_fields
from hiku.sources.sqlalchemy import db_fields, db_link

from .model import session, Planet, Feature, FeaturePlanet
from .model import _Climate, _Terrain


def _(string):
    return string


_GRAPH = Graph([
    Edge(Planet.__table__.name, chain(
        db_fields(session, Planet.__table__, [
            'id',
            'name',
            'climate',
            'terrain',
        ]),
        [
            db_link(session, 'features', 'id',
                    FeaturePlanet.__table__.c.planet_id,
                    FeaturePlanet.__table__.c.feature_id,
                    to_list=True, edge=Feature.__table__.name),
        ],
    )),
    Edge(Feature.__table__.name, chain(
        db_fields(session, Feature.__table__, [
            'id',
            'title',
            'director',
            'producer',
            'episode_num',
            'release_date',
        ]),
        [
            db_link(session, 'planets', 'id',
                    FeaturePlanet.__table__.c.feature_id,
                    FeaturePlanet.__table__.c.planet_id,
                    to_list=True, edge=Planet.__table__.name),
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
    return ', '.join(sorted(CLIMATE[v].title for v in (value or [])))


TERRAIN = enum_map(['ident', 'title'], {
    _Terrain.desert: ('desert', _('Desert')),
    _Terrain.grasslands: ('grasslands', _('Grasslands')),
    _Terrain.mountains: ('mountains', _('Mountains')),
})


@define(None)
def terrain(value):
    return ', '.join(sorted(TERRAIN[v].title for v in (value or [])))


def query_planets():
    rows = session.execute(select([Planet.__table__.c.id])).fetchall()
    return [r.id for r in rows]


def query_features():
    rows = session.execute(select([Feature.__table__.c.id])).fetchall()
    return [r.id for r in rows]


GRAPH = Graph([
    Edge('feature', chain(
        subquery_fields(_GRAPH, Feature.__table__.name, [
            Expr('id', IntegerType, S.this.id),
            Expr('title', StringType, S.this.title),
            Expr('director', StringType, S.this.director),
            Expr('producer', StringType, S.this.producer),
            Expr('episode-num', IntegerType, S.this.episode_num),
        ]),
        [
            db_link(session, 'planets', 'id',
                    FeaturePlanet.__table__.c.feature_id,
                    FeaturePlanet.__table__.c.planet_id,
                    to_list=True, edge=Planet.__table__.name),
        ],
    )),
    Edge('planet', chain(
        subquery_fields(_GRAPH, Planet.__table__.name, [
            Expr('id', IntegerType, S.this.id),
            Expr('name', StringType, S.this.name),
            Expr('climate', StringType, climate(S.this.climate)),
            Expr('terrain', StringType, terrain(S.this.terrain)),
        ]),
        [
            db_link(session, 'features', 'id',
                    FeaturePlanet.__table__.c.planet_id,
                    FeaturePlanet.__table__.c.feature_id,
                    to_list=True, edge=Feature.__table__.name),
        ],
    )),
    Link('planets', None, 'planet', query_planets, to_list=True),
    Link('features', None, 'feature', query_features, to_list=True),
])
