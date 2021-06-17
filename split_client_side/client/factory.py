import threading

from splitio.client import input_validator
from splitio.client.factory import SplitFactory, _wrap_impression_listener
from splitio.client import util
from splitio.engine.impressions import Manager as ImpressionsManager

# APIs
from splitio.api.client import HttpClient
from splitio.api.splits import SplitsAPI
from splitio.api.impressions import ImpressionsAPI
from splitio.api.events import EventsAPI
from splitio.api.telemetry import TelemetryAPI
from splitio.api.auth import AuthAPI

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask

# Synchronizer
from splitio.sync.synchronizer import SplitTasks, SplitSynchronizers, Synchronizer
from splitio.sync.manager import Manager
from splitio.sync.split import SplitSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.telemetry import TelemetrySynchronizer

# Recorder
from splitio.recorder.recorder import StandardRecorder

from ..api.segments import MySegmentsAPI
from .client import ClientSideClient
from ..storage.adapters import sql
from ..storage.sql import SqlSplitStorage, SqlMySegmentsStorage, SqlImpressionStorage, \
    SqlEventStorage, SqlTelemetryStorage
from ..sync.segment import MySegmentsSynchronizer


class ClientSideFactory(SplitFactory):
    def __init__(self, *args, **kwargs):
        traffic_key = kwargs.pop('traffic_key')
        super().__init__(*args, **kwargs)
        self._traffic_key = traffic_key

    def client(self):
        """
        Return a new client.

        This client is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return ClientSideClient(self, self._recorder, self._labels_enabled, traffic_key=self._traffic_key)


def build_factory(api_key, cfg, traffic_key, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-locals
                  auth_api_base_url=None, streaming_api_base_url=None):
    """Build and return a split factory tailored to the supplied config."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    http_client = HttpClient(
        sdk_url=sdk_url,
        events_url=events_url,
        auth_url=auth_api_base_url,
        timeout=cfg.get('connectionTimeout')
    )

    sdk_metadata = util.get_metadata(cfg)
    apis = {
        'auth': AuthAPI(http_client, api_key, sdk_metadata),
        'splits': SplitsAPI(http_client, api_key, sdk_metadata),
        'segments': MySegmentsAPI(http_client, api_key, sdk_metadata, traffic_key),
        'impressions': ImpressionsAPI(http_client, api_key, sdk_metadata, cfg['impressionsMode']),
        'events': EventsAPI(http_client, api_key, sdk_metadata),
        'telemetry': TelemetryAPI(http_client, api_key, sdk_metadata)
    }

    db_client = sql.build(cfg)

    storages = {
        'splits': SqlSplitStorage(db_client),
        'segments': SqlMySegmentsStorage(db_client),
        'impressions': SqlImpressionStorage(db_client, cfg['impressionsQueueSize']),
        'events': SqlEventStorage(db_client, cfg['eventsQueueSize']),
        'telemetry': SqlTelemetryStorage(db_client)
    }

    imp_manager = ImpressionsManager(
        cfg['impressionsMode'],
        True,
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata))

    synchronizers = SplitSynchronizers(
        SplitSynchronizer(apis['splits'], storages['splits']),
        MySegmentsSynchronizer(apis['segments'], storages['segments'], traffic_key),
        ImpressionSynchronizer(apis['impressions'], storages['impressions'],
                               cfg['impressionsBulkSize']),
        EventSynchronizer(apis['events'], storages['events'], cfg['eventsBulkSize']),
        TelemetrySynchronizer(apis['telemetry'], storages['telemetry']),
        ImpressionsCountSynchronizer(apis['impressions'], imp_manager),
    )

    tasks = SplitTasks(
        SplitSynchronizationTask(
            synchronizers.split_sync.synchronize_splits,
            cfg['featuresRefreshRate'],
        ),
        SegmentSynchronizationTask(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        ),
        ImpressionsSyncTask(
            synchronizers.impressions_sync.synchronize_impressions,
            cfg['impressionsRefreshRate'],
        ),
        EventsSyncTask(synchronizers.events_sync.synchronize_events, cfg['eventsPushRate']),
        TelemetrySynchronizationTask(
            synchronizers.telemetry_sync.synchronize_telemetry,
            cfg['metricsRefreshRate'],
        ),
        ImpressionsCountSyncTask(synchronizers.impressions_count_sync.synchronize_counters)
    )

    synchronizer = Synchronizer(synchronizers, tasks)

    sdk_ready_flag = threading.Event()
    manager = Manager(sdk_ready_flag, synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, streaming_api_base_url, api_key[-4:])

    storages['events'].set_table_full_hook(tasks.events_task.flush)
    storages['impressions'].set_table_full_hook(tasks.impressions_task.flush)

    recorder = StandardRecorder(
        imp_manager,
        storages['telemetry'],
        storages['events'],
        storages['impressions'],
    )

    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer")
    initialization_thread.setDaemon(True)
    initialization_thread.start()

    return ClientSideFactory(api_key, storages, cfg['labelsEnabled'],
                             recorder, manager, sdk_ready_flag, traffic_key=traffic_key)
