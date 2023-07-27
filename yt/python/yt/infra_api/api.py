#!/usr/bin/python

import requests
import time
import typing

from requests import adapters
from urllib3.util import retry


class InfraError(Exception):
    pass


class EVENT(object):
    class TYPE(object):
        MAINTENANCE = 'maintenance'
        ISSUE = 'issue'

    class SEVERITY(object):
        MAJOR = 'major'
        MINOR = 'minor'


class InfraClient(object):
    VERSION = "v1"

    def __init__(self, backend_url, token, max_retries=None, backoff_factor=None):
        self.backend_url = backend_url
        self.token = token

        if max_retries is None:
            max_retries = 3

        if backoff_factor is None:
            backoff_factor = 0.5

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def _build_http_session(self):
        session = requests.Session()
        retry_strategy = retry.Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            method_whitelist=["GET"],
        )
        adapter = adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request(self, method, url, params=None, body=None):
        http_session = self._build_http_session()

        headers = {}
        if self.token is not None:
            headers["Authorization"] = "OAuth {0}".format(self.token)

        response = http_session.request(
            method=method,
            url="https://{0}/{1}/{2}".format(self.backend_url, self.VERSION, url),
            params=params,
            headers=headers,
            json=body,
        )

        if not response.ok:
            raise InfraError("Got response with code {}: {}", response.status_code, response.text)
        return response

    # Events
    # Constructors
    @staticmethod
    def make_event_spec(
        title,  # type: str
        description,  # type: str
        service_id,  # type: int
        environment_id,  # type: int
        event_type,  # type: str
        severity,  # type: str
        mail=True,  # type: bool
        start_time=None,  # type: int
        finish_time=None,  # type: int
        affected_dcs=None,  # type: typing.List[str]
        tickets=None,  # type: str
    ):
        if start_time is None:
            start_time = int(time.time())
        event_spec = {
            'title': title,
            'description': description,
            'environmentId': environment_id,
            'serviceId': service_id,
            'type': event_type,
            'severity': severity,
            'startTime': start_time,
            'sendEmailNotifications': mail,
            "man": False,
            "myt": False,
            "sas": False,
            "vla": False,
            "iva": False,
            "components": [],
        }
        if finish_time is not None:
            event_spec['finishTime'] = finish_time
        if affected_dcs is not None:
            for dc in affected_dcs:
                event_spec[dc] = True
        if tickets is not None:
            event_spec['tickets'] = tickets
        return event_spec

    @staticmethod
    def make_event_resolution_spec(finish_time=None):
        if finish_time is None:
            finish_time = int(time.time())
        return {
            'finishTime': finish_time,
        }

    # API
    def create_event(self, event):
        return self._request("post", "events", body=event).json()

    def update_event(self, event_id, event):
        self._request("put", "events/{0}".format(event_id), body=event)

    def delete_event(self, event_id):
        self._request("delete", "events/{0}".format(event_id))

    def get_event(self, event_id):
        return self._request("get", "events/{0}".format(event_id))

    def get_events(self, filter=None):
        return self._request("get", "events", params=filter).json()

    # Services
    def get_services(self):
        return self._request("get", "services").json()

    def create_service(self, service):
        return self._request("post", "services", body=service).json()

    def update_service(self, service_id, new_attrs):
        return self._request("put", "services/{0}".format(service_id), body=new_attrs).json()

    def delete_service(self, service_id):
        self._request("delete", "services/{0}".format(service_id))

    # Emails
    def get_mails(self, service_id, environment_id=None):
        return self._request("get", "mails", params={
            "serviceId": service_id,
            "environmentId": environment_id,
        }).json()

    def add_mail(self, address, service_id, environment_id, type, severity):
        self._request("post", "mails", body={
            "email": address,
            "serviceId": service_id,
            "environmentId": environment_id,
            "eventType": type,
            "eventSeverity": severity,
        })
