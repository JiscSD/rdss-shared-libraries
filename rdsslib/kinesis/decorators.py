""" Decorators for messages placed in Kinesis streams"""

from datetime import datetime
from dateutil.tz import tzlocal
import json
import socket


class RouterHistoryDecorator(object):

    def process(self, payload):
        """Decorates message's history header with details"""
        try:
            payload_json = json.loads(payload)
        except ValueError:
            return

        if 'messageHeader' not in payload_json:
            return

        if 'messageHistory' not in payload_json['messageHeader']:
            payload_json['messageHeader']['messageHistory'] = []

        if type(payload_json['messageHeader']['messageHistory']) is not list:
            if type(payload_json['messageHeader']['messageHistory']) is dict:
                current_message_history = payload_json['messageHeader'][
                    'messageHistory']
                payload_json['messageHeader']['messageHistory'] = []
                payload_json['messageHeader']['messageHistory'].append(
                    current_message_history)
            else:
                return

        payload_json['messageHeader']['messageHistory'].append({
            'machineId': 'rdss-institutional-content-based-router',
            'machineAddress': socket.gethostbyname(socket.gethostname()),
            'timestamp': datetime.now(tzlocal()).isoformat()
        })
        return json.dumps(payload_json)