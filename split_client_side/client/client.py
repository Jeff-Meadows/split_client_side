from splitio.client.client import Client


class ClientSideClient(Client):
    def __init__(self, *args, **kwargs):
        traffic_key = kwargs.pop('traffic_key')
        super().__init__(*args, **kwargs)
        self._traffic_key = traffic_key

    def get_treatment_with_config(self, feature, attributes=None):
        """
        Get the treatment and config for a feature, with optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: tuple(str, str)
        """
        # pylint:disable=arguments-differ
        return super().get_treatment_with_config(self._traffic_key, feature, attributes)

    def get_treatment(self, feature, attributes=None):
        """
        Get the treatment for a feature, with an optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: str
        """
        # pylint:disable=arguments-differ
        return super().get_treatment(self._traffic_key, feature, attributes)

    def get_treatments_with_config(self, features, attributes=None):
        """
        Evaluate multiple features and return a dict with feature -> (treatment, config).

        Get the treatments for a list of features, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param features: Array of the names of the features for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the features provided
        :rtype: dict
        """
        # pylint:disable=arguments-differ
        return super().get_treatments_with_config(self._traffic_key, features, attributes)

    def get_treatments(self, features, attributes=None):
        """
        Evaluate multiple features and return a dictionary with all the feature/treatments.

        Get the treatments for a list of features, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param features: Array of the names of the features for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the features provided
        :rtype: dict
        """
        # pylint:disable=arguments-differ
        return super().get_treatments(self._traffic_key, features, attributes)
