import json

import threading

from splitio.models import splits, segments
from splitio.models.events import Event
from splitio.models.impressions import Impression
from splitio.storage import SplitStorage, ImpressionStorage, SegmentStorage, EventStorage, TelemetryStorage
from .adapters.sql import SplitModel, MetadataModel, SegmentKeyModel, SegmentModel, \
    ImpressionModel, EventModel, CounterModel, GaugeModel, LatencyModel, MySegmentModel


class SqlSplitStorage(SplitStorage):
    """Split storage interface implemention backed by a database."""

    def __init__(self, db_client) -> None:
        super().__init__()
        self._db_client = db_client
        self._lock = threading.Lock()

    def _get_split_record(self, split_name):
        return self._db_client.get_one_or_none(SplitModel, SplitModel.name == split_name)

    @staticmethod
    def _get_split_from_record(split_record):
        return splits.from_raw(json.loads(split_record.json_data))

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: str
        """
        record = self._get_split_record(split_name)
        if record is None:
            return None
        return self._get_split_from_record(record)

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_names: list(str)

        :rtype: dict
        """
        records = self._db_client.get_all(SplitModel, SplitModel.name.in_(split_names), group_by=SplitModel.name)
        split_dict = {name: None for name in split_names}
        for record in records:
            split_dict[record.name] = self._get_split_from_record(record)
        return split_dict

    def put(self, split):
        """
        Store a split.

        :param split: Split object to store
        :type split_name: splitio.models.splits.Split
        """
        models_to_merge = []
        with self._lock:
            existing_split_record = self._get_split_record(split.name)
            if existing_split_record is None:
                models_to_merge.append(SplitModel(
                    name=split.name,
                    traffic_type_name=split.traffic_type_name,
                    json_data=json.dumps(split.to_json())
                ))
            else:
                models_to_merge.append(existing_split_record)
                existing_split_record.traffic_type_name = split.traffic_type_name
                existing_split_record.json_data = json.dumps(split.to_json())
            self._db_client.merge_and_commit(*models_to_merge)

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        with self._lock:
            existing_split = self.get(split_name)
            if existing_split is None:
                return False
            self._db_client.delete_all(SplitModel, SplitModel.name == split_name)
            return True

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        record = self._db_client.get_one_or_none(MetadataModel, MetadataModel.name == 'change_number')
        return record.number if record is not None else 0

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        self._db_client.update_or_insert(MetadataModel, dict(name='change_number'), dict(number=new_change_number))

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        records = self._db_client.get_all(SplitModel)
        return list(set(record.name for record in records))

    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        records = self._db_client.get_all(SplitModel)
        return [splits.from_raw(json.loads(record.json_data)) for record in records]

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        record = self._db_client.get_first(SplitModel, SplitModel.traffic_type_name == traffic_type_name)
        return record is not None

    def get_segment_names(self):
        """
        Return a set of all segments referenced by splits in storage.

        :return: Set of all segment names.
        :rtype: set(string)
        """
        return {name for spl in self.get_all_splits() for name in spl.get_segment_names()}

    def kill_locally(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        with self._lock:
            if self.get_change_number() > change_number:
                return
            split = self.get(split_name)
            if not split:
                return
            split.local_kill(default_treatment, change_number)
            self.put(split)


class SqlSegmentStorage(SegmentStorage):
    """DB based segment storage class."""

    def __init__(self, db_client):
        """
        Class constructor.

        :param db_client: DB client or compliant interface.
        :type db_client: splitio.storage.sql.DbClient
        """
        self._db_client = db_client

    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :rtype: str
        """
        record = self._db_client.get_one_or_none(SegmentModel, SegmentModel.name == segment_name)
        if record is not None:
            keys = [key.name for key in record.keys]
            return segments.Segment(segment_name, keys, record.change_number)
        return None

    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        record = self._db_client.get_one_or_none(SegmentModel, SegmentModel.name == segment.name)
        if record is None:
            record = SegmentModel(name=segment.name, change_number=segment.change_number)
        record.keys = [SegmentKeyModel(name=key) for key in segment.keys]
        self._db_client.merge_and_commit(record)

    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Update a split. Create it if it doesn't exist.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: Set of members to add to the segment.
        :type to_add: set
        :param to_remove: List of members to remove from the segment.
        :type to_remove: Set
        """
        record = self._db_client.get_one_or_none(SegmentModel, SegmentModel.name == segment_name)
        if record is None:
            record = SegmentModel(name=segment_name, change_number=change_number)
        record.keys = [key for key in record.keys if key.name not in to_remove]
        for add in to_add:
            record.keys.append(SegmentKeyModel(name=add))
        if change_number is not None:
            record.change_number = change_number
        self._db_client.merge_and_commit(record)

    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        segment = self.get(segment_name)
        if segment is None:
            return None
        return segment.change_number

    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        record = self._db_client.get_one_or_none(SegmentModel, SegmentModel.name == segment_name)
        if record is not None:
            record.change_number = new_change_number
            self._db_client.merge_and_commit(record)

    def segment_contains(self, segment_name, key):
        """
        Check whether a specific key belongs to a segment in storage.

        :param segment_name: Name of the segment to search in.
        :type segment_name: str
        :param key: Key to search for.
        :type key: str

        :return: True if the segment contains the key. False otherwise.
        :rtype: bool
        """
        return self._db_client.get_one_or_none(
            SegmentKeyModel,
            SegmentKeyModel.model.has(name=segment_name),
            SegmentKeyModel.name == key,
        ) is not None


class SqlMySegmentsStorage:
    def __init__(self, db_client):
        self._db_client = db_client

    def get(self, traffic_key):
        return [record.name for record in self._db_client.get_all(MySegmentModel, MySegmentModel.traffic_key == traffic_key)]

    def put(self, traffic_key, segment_names):
        self._db_client.delete_all(MySegmentModel, MySegmentModel.traffic_key == traffic_key)
        self._db_client.merge_and_commit(*[MySegmentModel(traffic_key=traffic_key, segment_name=segment_name) for segment_name in segment_names])

    def segment_contains(self, segment_name, key):
        return self._db_client.get_count(MySegmentModel, MySegmentModel.traffic_key == key, MySegmentModel.segment_name == segment_name) > 0

    def clear(self):
        self._db_client.delete_all(MySegmentModel)


class SqlImpressionStorage(ImpressionStorage):
    """DB implementation of an impressions storage."""

    def __init__(self, db_client, queue_size):
        """
        Construct an instance.

        :param db_client: DB client or compliant interface.
        :type db_client: splitio.storage.sql.DbClient
        :param queue_size: How many impressions to queue before forcing a submission
        """
        self._queue_size = queue_size
        self._db_client = db_client
        self._table_full_hook = None

    def set_table_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._table_full_hook = hook

    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        self._db_client.merge_and_commit(*[ImpressionModel(json_data=json.dumps(impression)) for impression in impressions])
        if self._db_client.get_count(ImpressionModel) > self._queue_size:
            self._table_full_hook()
        return True

    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        records = self._db_client.pop(ImpressionModel, count, order_by=ImpressionModel.created_at)
        return [Impression(*json.loads(record.json_data)) for record in records]

    def clear(self):
        """
        Clear data.
        """
        self._db_client.delete_all(ImpressionModel)


class SqlEventStorage(EventStorage):
    """
    DB storage for events.

    Supports adding and popping events.
    """

    MAX_SIZE_BYTES = 5 * 1024 * 1024

    def __init__(self, db_client, queue_size):
        """
        Construct an instance.

        :param db_client: DB client or compliant interface.
        :type db_client: splitio.storage.sql.DbClient
        :param queue_size: How many impressions to queue before forcing a submission
        """
        self._queue_size = queue_size
        self._db_client = db_client
        self._table_full_hook = None

    def set_table_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._table_full_hook = hook

    def put(self, events):
        """
        Add an event to storage.

        :param event: Event to be added in the storage
        """
        self._db_client.merge_and_commit(*[EventModel(json_data=json.dumps(event.event), size=event.size) for event in events])
        if (self._db_client.get_count(EventModel) > self._queue_size
                or sum(event.size for event in self._db_client.get_all(EventModel)) >= self.MAX_SIZE_BYTES):
            self._table_full_hook()
        return True

    def pop_many(self, count):
        """
        Pop multiple items from the storage.

        :param count: number of items to be retrieved and removed from the queue.
        """
        records = self._db_client.pop(EventModel, count, order_by=EventModel.created_at)
        return [Event(*json.loads(record.json_data)) for record in records]

    def clear(self):
        """
        Clear data.
        """
        self._db_client.delete_all(EventModel)


class SqlTelemetryStorage(TelemetryStorage):
    """DB implementation of telemetry storage interface."""

    def __init__(self, db_client):
        """
        Construct an instance.

        :param db_client: DB client or compliant interface.
        :type db_client: splitio.storage.sql.DbClient
        """
        self._db_client = db_client

    def inc_latency(self, name, bucket):
        """
        Add a latency.

        :param name: Name of the latency metric.
        :type name: str
        :param value: Value of the latency metric.
        :tyoe value: int
        """
        if 0 <= bucket <= 21:
            self._db_client.increment_or_create(
                LatencyModel,
                getattr(LatencyModel, f'bucket_{bucket}'),
                LatencyModel.name == name,
                name=name
            )

    def inc_counter(self, name):
        """
        Increment a counter.

        :param name: Name of the counter metric.
        :type name: str
        """
        self._db_client.increment_or_create(CounterModel, CounterModel.value, CounterModel.name == name, name=name)

    def put_gauge(self, name, value):
        """
        Add a gauge metric.

        :param name: Name of the gauge metric.
        :type name: str
        :param value: Value of the gauge metric.
        :type value: int
        """
        self._db_client.update_or_insert(GaugeModel, dict(name=name), dict(value=value))

    def pop_counters(self):
        """
        Get all the counters.

        :rtype: list
        """
        return {counter.name: counter.value for counter in self._db_client.pop(CounterModel)}

    def pop_gauges(self):
        """
        Get all the gauges.

        :rtype: list

        """
        return {gauge.name: gauge.value for gauge in self._db_client.pop(GaugeModel)}

    def pop_latencies(self):
        """
        Get all latencies.

        :rtype: list
        """
        return {latency.name: latency.buckets for latency in self._db_client.pop(LatencyModel)}

    def clear(self):
        """
        Clear data.
        """
        self._db_client.delete_all(LatencyModel)
        self._db_client.delete_all(CounterModel)
        self._db_client.delete_all(GaugeModel)
