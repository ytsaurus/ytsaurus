from abc import abstractmethod, ABCMeta
from collections import namedtuple

OdinDBRecord = namedtuple("OdinDbRecord",
                          ["cluster", "service", "timestamp", "state", "duration", "messages"])


class Storage(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_record(self, check_id, cluster, service, timestamp, **kwargs):
        pass

    @staticmethod
    def validate_record(record):
        for key in ("check_id", "cluster", "service", "timestamp"):
            assert key in record

    def add_records_bulk(self, records):
        for record in records:
            self.validate_record(record)
        self.add_records_bulk_impl(records)

    def add_records_bulk_impl(self, records):
        for record in records:
            self.add_record(**record)

    @abstractmethod
    def get_records(self, clusters, services, start_timestamp, stop_timestamp):
        pass


class StorageForCluster(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_record(self, check_id, service, timestamp, **kwargs):
        pass

    @staticmethod
    def validate_record(record):
        for key in ("check_id", "service", "timestamp"):
            assert key in record

    def add_records_bulk(self, records):
        for record in records:
            self.validate_record(record)
        self.add_records_bulk_impl(records)

    def add_records_bulk_impl(self, records):
        for record in records:
            self.add_record(**record)

    @abstractmethod
    def get_records(self, services, start_timestamp, stop_timestamp):
        pass
