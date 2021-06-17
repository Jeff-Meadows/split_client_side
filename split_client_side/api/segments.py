import json
import logging

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.segments import SegmentsAPI


_LOGGER = logging.getLogger(__name__)


class MySegmentsAPI(SegmentsAPI):

    def __init__(self, http_client, apikey, sdk_metadata, traffic_key):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: client.HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param traffic_key: Traffic key to get segments for
        :type traffic_key: `str`

        """
        super().__init__(http_client, apikey, sdk_metadata)
        self._traffic_key = traffic_key

    def fetch_segments(self, traffic_key):
        """
        Fetch my segments from backend.

        :param traffic_key: Traffic key to fetch segments for.
        :type traffic_key: str

        :return: List of segment names the traffic key belongs to
        :rtype: list
        """
        try:
            response = self._client.get(
                'sdk',
                '/mySegments/{traffic_key}'.format(traffic_key=traffic_key),
                self._apikey,
                extra_headers=self._metadata,
            )

            if 200 <= response.status_code < 300:
                return [s['name'] for s in json.loads(response.body)['mySegments']]
            raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error(
                'Error fetching %s because an exception was raised by the HTTPClient',
                traffic_key
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Segments not fetched properly.') from exc
