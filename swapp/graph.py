from itertools import chain

from sqlalchemy import select

from hiku.graph import Graph, Edge, Link
from hiku.sources.sqlalchemy import db_fields, db_link

from .model import session, Planet, Feature, FeaturePlanet


def query_planets():
    rows = session.execute(select([Planet.__table__.c.id])).fetchall()
    return [r.id for r in rows]


def query_features():
    rows = session.execute(select([Feature.__table__.c.id])).fetchall()
    return [r.id for r in rows]


GRAPH = Graph([
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
                    to_list=True, edge='feature'),
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
                    to_list=True, edge='planet'),
        ],
    )),
    Link('planets', None, 'planet', query_planets, to_list=True),
    Link('features', None, 'feature', query_features, to_list=True),
])
