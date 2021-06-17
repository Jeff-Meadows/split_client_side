import datetime
from functools import wraps
import threading

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import StaticPool

from splitio.models.impressions import Label


db = threading.local()
_db_session_lock = threading.Lock()
DeclarativeBase = declarative_base()


def db_session(method):
    """
    Decorator for a method that needs exclusive access to the database.
    If a DB session exists for this thread already, the method will be called directly.
    If not, a DB session will be created. The session will be closed after the method returns.
    """
    @wraps(method)
    def db_session_method(self, *args, **kwargs):
        if hasattr(db, 'session'):
            return method(self, *args, **kwargs)
        with _db_session_lock:
            db.session = self._db_session_maker()  # pylint:disable=protected-access
            try:
                return method(self, *args, **kwargs)
            finally:
                db.session.close()
                del db.session
    return db_session_method


class DbClient:
    """
    Client for interacting with the Split SQLite database.
    """

    # pylint:disable=no-self-use

    def __init__(self, config=None):
        super().__init__()
        # default to config suitable for in-memory SQLite usage
        default_config = {
            'sql.url': 'sqlite://',
            'sql.connect_args': {'check_same_thread': False},
            'sql.poolclass': StaticPool,
        }
        default_config.update(config or {})
        self._db_engine = sqlalchemy.engine_from_config(configuration=default_config, prefix='sql.')
        DeclarativeBase.metadata.create_all(self._db_engine)
        self._db_session_maker = sessionmaker(bind=self._db_engine, autoflush=True)

    @db_session
    def get_all(self, model, *filters, group_by=None, order_by=None, direction='asc'):
        """
        Get all records of a certain type.
        :param model:           DB model class to search for.
        :type model:            :class:`DeclarativeBase`
        :param group_by:        Optional column to order by.
        :type group_by:         :class:`sqlalchemy.Column`
        :param order_by:        Optional column name to order by.
        :type order_by:         `str`
        :param direction:       Optional direction to order by (desc or asc)
        :type direction:        `str`
        :param filters:         Optional iterable of sqlalchemy filters to apply to the search.
        :rtype:                 `list` of `DeclarativeBase`
        """
        query = db.session.query(model).filter(*filters)
        if order_by is not None:
            query = query.order_by(getattr(order_by, direction)())
        if group_by is not None:
            query = query.group_by(group_by)
        return query.all()

    @db_session
    def get_count(self, model, *filters):
        query = db.session.query(model).filter(*filters)
        return query.count()

    @db_session
    def get_one_or_none(self, model, *filters):
        query = db.session.query(model).filter(*filters)
        return query.one_or_none()

    @db_session
    def get_first(self, model, *filters):
        query = db.session.query(model).filter(*filters)
        return query.first()

    @db_session
    def merge_and_commit(self, *records):
        """
        Add or update a new record into the DB.
        :param record:          Record to add or update.
        :type record:           :class:`DeclarativeBase`
        """
        for record in records:
            db.session.merge(record)
        db.session.commit()

    @db_session
    def update_or_insert(self, model, filter_kwargs, update_kwargs):
        existing_record = db.session.query(model).filter_by(**filter_kwargs).one_or_none()
        if existing_record is None:
            filter_kwargs.update(update_kwargs)
            db.session.add(model(**filter_kwargs))
        else:
            for key, value in update_kwargs.items():
                setattr(existing_record, key, value)
        db.session.commit()

    @db_session
    def delete_all(self, model, *filters):
        """
        Delete all records of a certain type.
        :param model:           DB model class to search for.
        :type model:            :class:`DeclarativeBase`
        :param filters:         Optional iterable of sqlalchemy filters to apply to the search.
        """
        db.session.query(model).filter(*filters).delete()
        db.session.commit()

    @db_session
    def pop(self, model, limit=None, *filters, order_by=None, direction='asc'):  # pylint:disable=keyword-arg-before-vararg
        query = db.session.query(model).filter(*filters)
        if order_by is not None:
            query = query.order_by(getattr(order_by, direction)())
        if limit is not None:
            query = query.limit(limit)
        rows = query.all()
        for row in rows:
            db.session.delete(row)
        db.session.commit()
        return rows

    @db_session
    def increment_or_create(self, model, column, *filters, **create_kwargs):
        record = db.session.query(model).filter(*filters).one_or_none()
        if record is None:
            record = model(**create_kwargs)
            try:
                db.session.add(record)
                db.session.commit()
            except Exception:  # pylint:disable=broad-except
                record = db.session.query(model).filter(*filters).one()
        setattr(record, column.name, column + 1)
        db.session.commit()


def build(config):
    return DbClient(config)


class SplitModel(DeclarativeBase):
    __tablename__ = 'split_splits'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.String(120))
    traffic_type_name = sqlalchemy.Column(sqlalchemy.String(120), index=True)
    json_data = sqlalchemy.Column(sqlalchemy.Text)


class MetadataModel(DeclarativeBase):
    __tablename__ = 'split_metadata'

    name = sqlalchemy.Column(sqlalchemy.String(30), primary_key=True)
    number = sqlalchemy.Column(sqlalchemy.Integer)


class SegmentModel(DeclarativeBase):
    __tablename__ = 'split_segments'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.String(120))
    change_number = sqlalchemy.Column(sqlalchemy.Integer)
    keys = relationship('SegmentKeyModel', lazy='joined')


class SegmentKeyModel(DeclarativeBase):
    __tablename__ = 'split_segment_keys'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.Text)
    model_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('split_segments.id'))
    model = relationship('SegmentModel', back_populates='keys', lazy='joined')


class MySegmentModel(DeclarativeBase):
    __tablename__ = 'split_my_segments'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    traffic_key = sqlalchemy.Column(sqlalchemy.String(120))
    segment_name = sqlalchemy.Column(sqlalchemy.String(120))


class ImpressionModel(DeclarativeBase):
    __tablename__ = 'split_impressions'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    status = sqlalchemy.Column(sqlalchemy.Enum(
        Label.EXCEPTION,
        Label.KILLED,
        Label.NO_CONDITION_MATCHED,
        Label.NOT_IN_SPLIT,
        Label.NOT_READY,
        Label.SPLIT_NOT_FOUND,
    ))
    created_at = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now)
    json_data = sqlalchemy.Column(sqlalchemy.Text)


class EventModel(DeclarativeBase):
    __tablename__ = 'split_events'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    created_at = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.now)
    json_data = sqlalchemy.Column(sqlalchemy.Text)
    size = sqlalchemy.Column(sqlalchemy.Integer)


class LatencyModel(DeclarativeBase):
    __tablename__ = 'split_latencies'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.Text)
    bucket_0 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_1 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_2 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_3 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_4 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_5 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_6 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_7 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_8 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_9 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_10 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_11 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_12 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_13 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_14 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_15 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_16 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_17 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_18 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_19 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_20 = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    bucket_21 = sqlalchemy.Column(sqlalchemy.Integer, default=0)

    @property
    def buckets(self):
        return [
            self.bucket_0,
            self.bucket_1,
            self.bucket_2,
            self.bucket_3,
            self.bucket_4,
            self.bucket_5,
            self.bucket_6,
            self.bucket_7,
            self.bucket_8,
            self.bucket_9,
            self.bucket_10,
            self.bucket_11,
            self.bucket_12,
            self.bucket_13,
            self.bucket_14,
            self.bucket_15,
            self.bucket_16,
            self.bucket_17,
            self.bucket_18,
            self.bucket_19,
            self.bucket_20,
            self.bucket_21,
        ]


class CounterModel(DeclarativeBase):
    __tablename__ = 'split_counters'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.Text)
    value = sqlalchemy.Column(sqlalchemy.Integer, default=0)


class GaugeModel(DeclarativeBase):
    __tablename__ = 'split_gauges'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)  # pylint:disable=invalid-name
    name = sqlalchemy.Column(sqlalchemy.Text)
    value = sqlalchemy.Column(sqlalchemy.Integer)
