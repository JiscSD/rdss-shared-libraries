""" Decorators for messages placed in Kinesis streams"""

from datetime import datetime
from dateutil.tz import tzlocal
import json
import socket


class RouterHistoryDecorator(object):

    @property
    def name(self):
        return 'RouterHistoryDecorator'

    def process(self, payload, machine_id):
        """Decorates message's history header with details
        :param payload: JSON formatted payload
        :return: decorated JSON formatted payload
        :rtype: str
        """
        try:
            payload_json = json.loads(payload)
        except json.decoder.JSONDecodeError:
            payload_json = {}

        try:
            header = payload_json['messageHeader']
        except (TypeError, KeyError):
            header = {}

        try:
            history = payload_json['messageHeader']['messageHistory']
        except (TypeError, KeyError):
            history = []

        try:
            # Get the hostname first
            machine_name = socket.gethostname()
        except (socket.gaierror, socket.herror):
            # Not sure what is best to express unknown: should pass validation
            machine_name = 'unknown'

        history_element = {
            'machineId': machine_id,
            'machineAddress': machine_name,
            'timestamp': datetime.now(tzlocal()).isoformat()
        }

        try:
            history.append(history_element)
        except AttributeError:
            history = [history_element]

        try:
            header['messageHistory'] = history
        except TypeError:
            header = {'messageHistory': history}

        try:
            payload_json['messageHeader'] = header
        except TypeError:
            payload_json = {'messageHeader': header}

        return json.dumps(payload_json)
