import json
import datetime
from enum import Enum

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import Integer, Unicode, Date, String
from sqlalchemy.types import TypeDecorator, Numeric
from sqlalchemy.schema import MetaData, Column, Table, ForeignKey


metadata = MetaData()

_last_id = 0


def gen_id():
    global _last_id
    _last_id += 1
    return _last_id


class IntEnum(TypeDecorator):
    impl = Integer

    def __init__(self, enum):
        self._enum = enum
        super(IntEnum, self).__init__()

    def process_bind_param(self, value, dialect):
        if value is not None:
            return value.value
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = self._enum(value)
        return value


class JSONEnumSet(TypeDecorator):
    impl = String

    def __init__(self, enum):
        self._enum = enum
        super(JSONEnumSet, self).__init__()

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(list(v.value for v in value))
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = frozenset(self._enum(v) for v in json.loads(value))
        return value


class Record(object):

    @classmethod
    def to_dict(cls):
        return {name: getattr(cls, name) for name in dir(cls)
                if not name.startswith('__')}


class _Climate(Enum):
    arid = 1
    temperate = 2
    tropical = 3


class _Terrain(Enum):
    desert = 1
    grasslands = 2
    mountains = 3


class _Gender(Enum):
    male = 1
    female = 2


class Feature(Record):
    __table__ = Table(
        'feature',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('title', Unicode),
        Column('director', Unicode),
        Column('producer', Unicode),
        Column('episode_num', Integer),
        Column('release_date', Date),
    )


class Planet(Record):
    __table__ = Table(
        'planet',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', Unicode),
        Column('climate', JSONEnumSet(_Climate)),
        Column('terrain', JSONEnumSet(_Terrain)),
    )


class Character(Record):
    __table__ = Table(
        'character',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', Unicode),
        Column('gender', IntEnum(_Gender)),
        Column('birth_year', Unicode),
        Column('home_planet_id', ForeignKey('planet.id')),
    )


class Starship(Record):
    __table__ = Table(
        'starship',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', Unicode),
        Column('model', Unicode),
        Column('crew', Integer),
        Column('passengers', Integer),
        Column('cargo_capacity', Numeric),
        Column('manufacturer', Unicode),
        Column('hyperdrive_rating', Numeric),
        Column('class', Unicode),
    )


class FeaturePlanet(Record):
    __table__ = Table(
        'feature_planet',
        metadata,
        Column('feature_id', ForeignKey('feature.id'), primary_key=True),
        Column('planet_id', ForeignKey('planet.id'), primary_key=True),
    )

    @classmethod
    def create(cls, feature_id, planet_id):
        return type('{}Link'.format(cls.__name__), (cls,),
                    {'feature_id': feature_id, 'planet_id': planet_id})


class FeatureStarship(Record):
    __table__ = Table(
        'feature_starship',
        metadata,
        Column('feature_id', ForeignKey('feature.id'), primary_key=True),
        Column('starship_id', ForeignKey('starship.id'), primary_key=True),
    )

    @classmethod
    def create(cls, feature_id, starship_id):
        return type('{}Link'.format(cls.__name__), (cls,),
                    {'feature_id': feature_id, 'starship_id': starship_id})


class Tatooine(Planet):
    id = gen_id()
    name = "Tatooine"
    climate = {_Climate.arid}
    terrain = {_Terrain.desert}


class Alderaan(Planet):
    id = gen_id()
    name = "Alderaan"
    climate = {_Climate.temperate}
    terrain = {_Terrain.grasslands, _Terrain.mountains}


class YavinIV(Planet):
    id = gen_id()
    name = "Yavin IV"
    climate = {_Climate.temperate, _Climate.tropical}
    terrain = {_Terrain.grasslands, _Terrain.mountains}


class Hoth(Planet):
    id = gen_id()
    name = "Hoth"


class Dagobah(Planet):
    id = gen_id()
    name = "Dagobah"


class Bespin(Planet):
    id = gen_id()
    name = "Bespin"


class OrdMantell(Planet):
    id = gen_id()
    name = "Ord Mantell"


class ANewHope(Feature):
    id = gen_id()
    title = "A New Hope"
    director = "George Lucas"
    producer = "Gary Kurtz, Rick McCallum"
    episode_num = 4
    release_date = datetime.date(1977, 5, 25)


class TheEmpireStrikesBack(Feature):
    id = gen_id()
    title = "The Empire Strikes Back"
    director = "Irvin Kershner"
    producer = "Gary Kutz, Rick McCallum"
    episode_num = 5
    release_date = datetime.date(1980, 5, 17)


class LukeSkywalker(Character):
    id = gen_id()
    name = "Luke Skywalker"
    gender = _Gender.male
    birth_year = '19BBY'
    home_planet_id = Tatooine.id


class DarthVader(Character):
    id = gen_id()
    name = "Darth Vader"
    gender = _Gender.male
    birth_year = '41.9BBY'
    home_planet_id = Tatooine.id


class LeiaOrgana(Character):
    id = gen_id()
    name = "Leia Organa"
    gender = _Gender.female
    birth_year = '19BBY'
    home_planet_id = Alderaan.id


class Yoda(Character):
    id = gen_id()
    name = "Yoda"
    gender = _Gender.male
    birth_year = '896BBY'
    # home_planet = 28


class ObiWanKenobi(Character):
    id = gen_id()
    name = "Obi-Wan Kenobi"
    gender = _Gender.male
    birth_year = '57BBY'
    # home_planet = 20


class HanSolo(Character):
    id = gen_id()
    name = "Han Solo"
    gender = _Gender.male
    birth_year = '29BBY'
    # home_planet = 22


class Chewbacca(Character):
    id = gen_id()
    name = "Chewbacca"
    gender = _Gender.male
    birth_year = '200BBY'
    # home_planet = 14


class MillenniumFalcon(Starship):
    id = gen_id()
    name = "Millennium Falcon"
    model = "YT-1300 light freighter"
    crew = 4
    passengers = 6
    cargo_capacity = '100000.0'
    manufacturer = "Corellian Engineering Corporation"
    hyperdrive_rating = '0.5'
    class_ = "Light freighter"


class DeathStarI(Starship):
    id = gen_id()
    name = "Death Star"
    model = "DS-1 Orbital Battle Station"
    crew = 342953
    passengers = 843342
    cargo_capacity = '1000000000000.0'
    manufacturer = ("Imperial Department of Military Research, "
                    "Sienar Fleet Systems")
    hyperdrive_rating = '4.0'
    class_ = "Deep Space Mobile Battlestation"


class StarDestroyer(Starship):
    id = gen_id()
    name = "Star Destroyer"
    model = "Imperial I-class Star Destroyer"
    crew = 47060
    # passengers =
    cargo_capacity = '36000000.0'
    manufacturer = "Kuat Drive Yards"
    hyperdrive_rating = '2.0'
    class_ = "Star Destroyer"


def persist(engine, items):
    for item in items:
        engine.execute(item.__table__.insert(), item.to_dict())


def setup():
    sa_engine = create_engine(
        'sqlite://',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
    )
    metadata.create_all(sa_engine)
    persist(sa_engine, [
        ANewHope,
        TheEmpireStrikesBack,
    ])
    persist(sa_engine, [
        Tatooine,
        Alderaan,
        YavinIV,
        Hoth,
        Dagobah,
        Bespin,
        OrdMantell,
    ])
    persist(sa_engine, [
        FeaturePlanet.create(ANewHope.id, Tatooine.id),
        FeaturePlanet.create(ANewHope.id, Alderaan.id),
        FeaturePlanet.create(ANewHope.id, YavinIV.id),
        FeaturePlanet.create(TheEmpireStrikesBack.id, Hoth.id),
        FeaturePlanet.create(TheEmpireStrikesBack.id, Dagobah.id),
        FeaturePlanet.create(TheEmpireStrikesBack.id, Bespin.id),
        FeaturePlanet.create(TheEmpireStrikesBack.id, OrdMantell.id),
    ])
    persist(sa_engine, [
        LukeSkywalker,
        DarthVader,
        LeiaOrgana,
        Yoda,
        ObiWanKenobi,
        HanSolo,
        Chewbacca,
    ])
    persist(sa_engine, [
        MillenniumFalcon,
        DeathStarI,
        StarDestroyer,
    ])
    persist(sa_engine, [
        FeatureStarship.create(ANewHope.id, MillenniumFalcon.id),
        FeatureStarship.create(ANewHope.id, DeathStarI.id),
        FeatureStarship.create(ANewHope.id, StarDestroyer.id),
    ])
    return sa_engine
