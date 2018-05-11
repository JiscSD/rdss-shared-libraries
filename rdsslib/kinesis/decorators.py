""" Decorators for messages placed in Kinesis streams"""

from datetime import datetime
from dateutil.tz import tzlocal
import json
import socket
import logging


class RouterHistoryDecorator(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @property
    def name(self):
        return 'RouterHistoryDecorator'

    def process(self, payload, machine_id):
        """Decorates message's history header with details
        :param payload: JSON formatted payload
        :return: decorated JSON formatted payload
        :rtype: str
        """
        self.logger.info('Preparing to append history header to [%s] with machine ID [%s]', payload, machine_id)
        try:
            payload_json = json.loads(payload)
        except json.decoder.JSONDecodeError:
            self.logger.exception('An error occurred decoding JSON payload [%s]', payload)
            payload_json = {}

        try:
            header = payload_json['messageHeader']
        except (TypeError, KeyError):
            self.logger.exception(
                'An error occurred accessing [messageHeader] field on JSON payload [%s]',
                payload_json
            )
            header = {}

        try:
            history = payload_json['messageHeader']['messageHistory']
        except (TypeError, KeyError):
            self.logger.exception(
                'An error occurred accessing [messageHeader][messageHistory] field on JSON payload [%s]',
                payload_json
            )
            history = []

        try:
            # Get the hostname first
            machine_name = socket.gethostname()
            self.logger.info('Got machine name [%s]', machine_name)
        except (socket.gaierror, socket.herror):
            # Not sure what is best to express unknown: should pass validation
            self.logger.info('Unable to retrieve machine name, using default [unknown]')
            machine_name = 'unknown'

        history_element = {
            'machineId': machine_id,
            'machineAddress': machine_name,
            'timestamp': datetime.now(tzlocal()).isoformat()
        }

        try:
            self.logger.info('Appending history [%s] to existing message history [%s]', history_element, history)
            history.append(history_element)
        except AttributeError:
            self.logger.exception(
                'Unable to append [%s] to message history [%s], converting to array',
                history_element,
                history
            )
            history = [history_element]

        try:
            self.logger.info('Appending history header [%s] to existing message history [%s]', history_element, history)
            header['messageHistory'] = history
        except TypeError:
            self.logger.exception(
                'Unable to append [%s] to message history [%s], fixing [messageHistory] element in [messageHeader]',
                history_element,
                history
            )
            header = {'messageHistory': history}

        try:
            self.logger.info('Setting [messageHeader] on JSON payload [%s] to [%s]', payload_json, header)
            payload_json['messageHeader'] = header
        except TypeError:
            self.logger.info('Unable to set [messageHeader] to [%s], fixing JSON payload', header)
            payload_json = {'messageHeader': header}

        self.logger.info('Finished appending header, returning JSON payload [%s]', payload_json)
        return json.dumps(payload_json)
