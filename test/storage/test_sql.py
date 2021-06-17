from splitio.models.events import EventWrapper, Event
from splitio.models.impressions import Impression
from splitio.models.segments import Segment
from splitio.models.splits import Split
from split_client_side.storage.sql import SqlSplitStorage, SqlSegmentStorage, SqlImpressionStorage, SqlEventStorage, SqlTelemetryStorage
from split_client_side.storage.adapters.sql import DbClient


# pylint:disable=no-self-use


class SqlSplitStorageTests:
    """SQL split storage test cases."""

    def _create_split_model(
        self,
        name,
        seed=400,
        killed=False,
        default_treatment='',
        traffic_type_name='user',
        status='ACTIVE',
        change_number=1,
    ):
        return Split(
            name=name,
            seed=seed,
            killed=killed,
            default_treatment=default_treatment,
            traffic_type_name=traffic_type_name,
            status=status,
            change_number=change_number,
        )

    def test_get_split(self, mocker):
        """Test retrieving a split works."""
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        storage = SqlSplitStorage(DbClient())
        result = storage.get('some_split')

        assert result is None
        assert from_raw.mock_calls == []

        split = self._create_split_model('some_split')
        storage.put(split)
        result = storage.get('some_split')

        assert result is not None
        assert from_raw.mock_calls == [mocker.call(split.to_json())]

    def test_fetch_many_splits(self):
        storage = SqlSplitStorage(DbClient())
        splits = [self._create_split_model(f'some_split_{n}') for n in range(4)]
        for split in splits:
            storage.put(split)

        results = storage.fetch_many([f'some_split_{n*2}' for n in range(3)])

        assert results['some_split_0'] is not None
        assert results['some_split_2'] is not None
        assert results['some_split_4'] is None

    def test_get_split_names(self):
        storage = SqlSplitStorage(DbClient())
        split_names = [f'some_split_{n}' for n in range(4)]
        splits = [self._create_split_model(name) for name in split_names]
        for split in splits:
            storage.put(split)

        all_split_names = storage.get_split_names()

        assert set(split_names) == set(all_split_names)

        storage.remove('some_split_0')

        all_split_names = storage.get_split_names()

        assert set(split_names) - {'some_split_0'} == set(all_split_names)

    def test_get_all_splits(self):
        storage = SqlSplitStorage(DbClient())
        split_names = [f'some_split_{n}' for n in range(4)]
        splits = [self._create_split_model(name) for name in split_names]
        for split in splits:
            storage.put(split)

        all_splits = storage.get_all_splits()

        assert set(split_names) == {s.name for s in all_splits}

        storage.remove('some_split_0')

        all_splits = storage.get_all_splits()

        assert set(split_names) - {'some_split_0'} == {s.name for s in all_splits}

    def test_is_valid_traffic_type(self):
        """Test that traffic type validation works."""
        storage = SqlSplitStorage(DbClient())

        assert storage.is_valid_traffic_type('user') is False

        user_split_1 = self._create_split_model('some_split_1', traffic_type_name='user')
        user_split_2 = self._create_split_model('some_split_2', traffic_type_name='user')
        storage.put(user_split_1)

        assert storage.is_valid_traffic_type('user') is True

        storage.remove('some_split_1')

        assert storage.is_valid_traffic_type('user') is False

        storage.put(user_split_1)

        assert storage.is_valid_traffic_type('user') is True

        storage.put(user_split_2)

        assert storage.is_valid_traffic_type('user') is True

        enterprise_split = self._create_split_model('some_split_2', traffic_type_name='enterprise')
        storage.put(enterprise_split)

        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('enterprise') is True

        storage.remove('some_split_1')

        assert storage.is_valid_traffic_type('user') is False

        storage.put(user_split_2)

        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('enterprise') is False


class SqlSegmentStorageTests:
    def _assert_segment_equals(self, segment, result):
        assert result.name == segment.name
        assert result.keys == segment.keys
        assert result.change_number == segment.change_number

    def test_segment_storage_retrieval(self):
        """Test storing and retrieving segments."""
        storage = SqlSegmentStorage(DbClient())
        segment = Segment('some_segment', ['key_1', 'key_2', 'key_4'], 1)

        storage.put(segment)
        result = storage.get('some_segment')
        self._assert_segment_equals(segment, result)
        assert storage.get('nonexistant-segment') is None

    def test_change_number(self):
        """Test storing and retrieving segment changeNumber."""
        storage = SqlSegmentStorage(DbClient())
        storage.set_change_number('some_segment', 123)
        # Change number is not updated if segment doesn't exist
        assert storage.get_change_number('some_segment') is None
        assert storage.get_change_number('nonexistant-segment') is None

        # Change number is updated if segment does exist.
        storage = SqlSegmentStorage(DbClient())
        segment = Segment('some_segment', [], 1)
        storage.put(segment)
        storage.set_change_number('some_segment', 123)
        assert storage.get_change_number('some_segment') == 123

    def test_segment_contains(self):
        """Test using storage to determine whether a key belongs to a segment."""
        storage = SqlSegmentStorage(DbClient())
        segment = Segment('some_segment', [], 1)
        storage.put(segment)

        assert not storage.segment_contains('some_segment', 'abc')

        segment.keys.add('def')
        storage.put(segment)

        assert not storage.segment_contains('some_segment', 'abc')

        segment.keys.add('abc')
        storage.put(segment)

        assert storage.segment_contains('some_segment', 'abc')

        segment.keys.remove('abc')
        storage.put(segment)

        assert not storage.segment_contains('some_segment', 'abc')

    def test_segment_update(self):
        """Test updating a segment."""
        storage = SqlSegmentStorage(DbClient())
        segment = Segment('some_segment', ['key1', 'key2', 'key3'], 123)
        storage.put(segment)
        self._assert_segment_equals(storage.get('some_segment'), segment)

        storage.update('some_segment', ['key4', 'key5'], ['key2', 'key3'], 456)
        assert storage.segment_contains('some_segment', 'key1')
        assert storage.segment_contains('some_segment', 'key4')
        assert storage.segment_contains('some_segment', 'key5')
        assert not storage.segment_contains('some_segment', 'key2')
        assert not storage.segment_contains('some_segment', 'key3')
        assert storage.get_change_number('some_segment') == 456


class SqlImpressionStorageTests:
    def test_push_pop_impressions(self):
        """Test pushing and retrieving impressions."""
        storage = SqlImpressionStorage(DbClient(), 100)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

        # Assert inserting multiple impressions at once works and maintains order.
        impressions = [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.put(impressions)

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = SqlImpressionStorage(DbClient(), 100)
        queue_full_hook = mocker.Mock()
        storage.set_table_full_hook(queue_full_hook)
        impressions = [
            Impression('key%d' % i, 'feature1', 'on', 'l1', 123456, 'b1', 321654)
            for i in range(0, 101)
        ]
        storage.put(impressions)
        assert queue_full_hook.mock_calls == mocker.call()

    def test_clear(self):
        """Test clear method."""
        storage = SqlImpressionStorage(DbClient(), 100)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])

        storage.clear()
        assert not storage.pop_many(1)


class SqlEventStorageTests:
    def test_push_pop_events(self):
        """Test pushing and retrieving events."""
        storage = SqlEventStorage(DbClient(), 100)
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

        # Assert inserting multiple impressions at once works and maintains order.
        events = [
            EventWrapper(
                event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ]
        assert storage.put(events)

        # Assert events are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = SqlEventStorage(DbClient(), 100)
        queue_full_hook = mocker.Mock()
        storage.set_table_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 321654, None), size=1024) for i in range(0, 101)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_queue_full_hook_properties(self, mocker):
        """Test queue_full_hook is executed when the queue is full regarding properties."""
        storage = SqlEventStorage(DbClient(), 200)
        queue_full_hook = mocker.Mock()
        storage.set_table_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 1, None), size=32768) for i in range(160)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_clear(self):
        """Test clear method."""
        storage = SqlEventStorage(DbClient(), 100)
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        storage.clear()
        assert not storage.pop_many(1)


class SqlTelemetryStorageTests:
    def test_latencies(self):
        """Test storing and retrieving latencies."""
        storage = SqlTelemetryStorage(DbClient())
        storage.inc_latency('sdk.get_treatment', -1)
        storage.inc_latency('sdk.get_treatment', 0)
        storage.inc_latency('sdk.get_treatment', 1)
        storage.inc_latency('sdk.get_treatment', 5)
        storage.inc_latency('sdk.get_treatment', 5)
        storage.inc_latency('sdk.get_treatment', 22)
        latencies = storage.pop_latencies()
        assert latencies['sdk.get_treatment'][0] == 1
        assert latencies['sdk.get_treatment'][1] == 1
        assert latencies['sdk.get_treatment'][5] == 2
        assert len(latencies['sdk.get_treatment']) == 22
        assert storage.pop_latencies() == {}

    def test_counters(self):
        """Test storing and retrieving counters."""
        storage = SqlTelemetryStorage(DbClient())
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_2')
        counters = storage.pop_counters()
        assert counters['some_counter_1'] == 3
        assert counters['some_counter_2'] == 1
        assert storage.pop_counters() == {}

    def test_gauges(self):
        """Test storing and retrieving gauges."""
        storage = SqlTelemetryStorage(DbClient())
        storage.put_gauge('some_gauge_1', 321)
        storage.put_gauge('some_gauge_2', 654)
        gauges = storage.pop_gauges()
        assert gauges['some_gauge_1'] == 321
        assert gauges['some_gauge_2'] == 654
        assert storage.pop_gauges() == {}

    def test_clear(self):
        """Test clear."""
        storage = SqlTelemetryStorage(DbClient())
        storage.put_gauge('some_gauge_1', 321)
        storage.inc_counter('some_counter_1')
        storage.inc_latency('sdk.get_treatment', 5)

        storage.clear()

        assert not storage.pop_counters()
        assert not storage.pop_gauges()
        assert not storage.pop_latencies()
