#!/usr/bin/python

import json
import logging
import time

import requests

class HttpResponseNotOkError(RuntimeError):
    pass

logger = logging.getLogger("notificator")

class CalendarProxy(object):
    time_format = "%Y-%m-%dT%H:%M:%S+00:00"

    def __init__(self, backend_url, user_id):
        self.backend_url = backend_url
        self.user_id = user_id

    def create_event(self, calendar_id, name, description, start_time, end_time):
        event = {
            "type": "user",
            "othersCanView": "true",
            "startTs": time.strftime(self.time_format, start_time),
            "endTs": time.strftime(self.time_format, end_time),
            "name": name,
            "description": description,
            "layerId": calendar_id
        }

        logger.debug("Creating event: %s", event)
        response = requests.post(url="http://{0}/create-event".format(self.backend_url),
                                 params={"uid": self.user_id},
                                 data={"data": json.dumps(event)})
        self._check_response(response)

    def create_calendar(self, name, public):
        calendar = {
            "name": name,
            "color": "#ffc760",
            "notifyAboutEventChanges": "false",
            "isClosed": "false" if public else "true",
        }
        response = requests.post(url="http://{0}/create-layer".format(self.backend_url),
                                 params={"uid": self.user_id},
                                 data={"data": json.dumps(calendar)})
        self._check_response(response)
        return response.json()['layerId']

    def delete_calendar(self, calendar_id):
        response = requests.post(url="http://{0}/delete_layer".format(self.backend_url),
                                 params={"uid": self.user_id, "id": calendar_id})
        self._check_response(response)

    def delete_event(self, event_id):
        response = requests.post(url="http://{0}/delete_event".format(self.backend_url),
                                 params={"uid": self.user_id, "id": event_id})
        self._check_response(response)

    def get_all_calendars(self):
        response = requests.get(url="http://{0}/get_user_layers".format(self.backend_url),
                                params={"uid": self.user_id})
        self._check_response(response)
        return [calendar["id"] for calendar in response.json()["layers"]]

    def get_all_events(self, start_time, end_time):
        response = requests.get(url="http://{0}/get-events".format(self.backend_url),
                                params={"uid": self.user_id,
                                        "from": time.strftime(self.time_format, start_time),
                                        "to": time.strftime(self.time_format, end_time)})
        self._check_response(response)
        return [event["id"] for event in response.json()["events"]]

    def _check_response(self, response):
        logger.debug("Response code: %d  reason: %s\n%s", response.status_code, response.reason, response.content)
        if not response.ok:
            raise HttpResponseNotOkError(response.reason)
