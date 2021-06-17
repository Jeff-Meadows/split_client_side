

class MySegmentsSynchronizer:
    def __init__(self, segment_api, segment_storage, traffic_key):
        """
        Class constructor.

        :param segment_api: API to retrieve segments from backend.
        :type segment_api: MySegmentsApi

        :param segment_storage: Segment storage reference.
        :type segment_storage: splitio.storage.SegmentStorage

        """
        self._api = segment_api
        self._segment_storage = segment_storage
        self._traffic_key = traffic_key

    def synchronize_segments(self):
        """
        Submit all current segments and wait for them to finish, then set the ready flag.

        :return: True if no error occurs. False otherwise.
        :rtype: bool
        """
        segment_names = self._api.fetch_segments(self._traffic_key)
        self._segment_storage.put(self._traffic_key, segment_names)
        return True
