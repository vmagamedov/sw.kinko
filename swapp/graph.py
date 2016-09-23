from collections import namedtuple

from sqlalchemy import select

from hiku.expr import S, define
from hiku.graph import Graph, Edge, Link, Root
from hiku.types import String, Integer, Optional, Sequence, TypeRef, Unknown
from hiku.engine import pass_context
from hiku.sources import sqlalchemy as sa
from hiku.sources.graph import SubGraph, Expr

from .model import Planet, Feature, FeaturePlanet
from .model import _Climate, _Terrain


def _(string):
    return string


SA_ENGINE = 'sa-engine'

planets_query = sa.FieldsQuery(SA_ENGINE, Planet.__table__)

features_query = sa.FieldsQuery(SA_ENGINE, Feature.__table__)

to_planets_query = sa.LinkQuery(
    Sequence[TypeRef['planet']],
    SA_ENGINE,
    from_column=FeaturePlanet.__table__.c.feature_id,
    to_column=FeaturePlanet.__table__.c.planet_id,
)

to_features_query = sa.LinkQuery(
    Sequence[TypeRef['feature']],
    SA_ENGINE,
    from_column=FeaturePlanet.__table__.c.planet_id,
    to_column=FeaturePlanet.__table__.c.feature_id,
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


@define(Unknown)
def climate(value):
    if value is not None:
        return ', '.join(sorted(CLIMATE[v].title for v in value))
    return None


TERRAIN = enum_map(['ident', 'title'], {
    _Terrain.desert: ('desert', _('Desert')),
    _Terrain.grasslands: ('grasslands', _('Grasslands')),
    _Terrain.mountains: ('mountains', _('Mountains')),
})


@define(Unknown)
def terrain(value):
    if value is not None:
        return ', '.join(sorted(TERRAIN[v].title for v in value))
    return None


@pass_context
def all_planets(ctx):
    rows = ctx[SA_ENGINE].execute(select([Planet.__table__.c.id])).fetchall()
    return [r.id for r in rows]


@pass_context
def all_features(ctx):
    rows = ctx[SA_ENGINE].execute(select([Feature.__table__.c.id])).fetchall()
    return [r.id for r in rows]


sg_feature = SubGraph(_GRAPH, 'feature')
sg_planet = SubGraph(_GRAPH, 'planet')


GRAPH = Graph([
    Edge('feature', [
        Expr('id', sg_feature, Integer, S.this.id),
        Expr('title', sg_feature, String, S.this.title),
        Expr('director', sg_feature, String, S.this.director),
        Expr('producer', sg_feature, String, S.this.producer),
        Expr('episode-num', sg_feature, Integer, S.this.episode_num),
        sa.Link('planets', to_planets_query, requires='id'),
    ]),
    Edge('planet', [
        Expr('id', sg_planet, Integer, S.this.id),
        Expr('name', sg_planet, String, S.this.name),
        Expr('climate', sg_planet, Optional[String],
             climate(S.this.climate)),
        Expr('terrain', sg_planet, Optional[String],
             terrain(S.this.terrain)),
        sa.Link('features', to_features_query, requires='id'),
    ]),
    Root([
        Link('planets', Sequence[TypeRef['planet']],
             all_planets, requires=None),
        Link('features', Sequence[TypeRef['feature']],
             all_features, requires=None),
    ]),
])
