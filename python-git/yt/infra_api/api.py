#!/usr/bin/python

import requests


class InfraError(Exception):
        pass


class InfraClient(object):
    VERSION = "v1"

    def __init__(self, backend_url, token):
        self.backend_url = backend_url
        self.token = token

    def _request(self, method, url, params=None, body=None):
        headers = {}
        if self.token is not None:
            headers["Authorization"] = "OAuth {0}".format(self.token)

        response = requests.request(
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
    def create_event(self, event):
        return self._request("post", "events", body=event).json()

    def update_event(self, event_id, event):
        self._request("put", "events/{0}".format(event_id), body=event)

    def delete_event(self, event_id):
        self._request("delete", "events/{0}".format(event_id))

    def get_events(self, filter=None):
        return self._request("get", "events", params=filter).json()

    # Services
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
