/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <assert.h>
#include "metadata.h"
#include "error.h"

using std::string;
using std::vector;
using std::unordered_set;

namespace cppkafka {

// PartitionMetadata

PartitionMetadata::PartitionMetadata(const rd_kafka_metadata_partition& partition) 
: id_(partition.id), error_(partition.err), leader_(partition.leader) {
    for (int i = 0; i < partition.replica_cnt; ++i) {
        replicas_.push_back(partition.replicas[i]);
    }
    for (int i = 0; i < partition.isr_cnt; ++i) {
        isrs_.push_back(partition.isrs[i]);
    }
}

uint32_t PartitionMetadata::get_id() const {
    return id_;
}

Error PartitionMetadata::get_error() const {
    return error_;
}

int32_t PartitionMetadata::get_leader() const {
    return leader_;
}

const vector<int32_t>& PartitionMetadata::get_replicas() const {
    return replicas_;
}

const vector<int32_t>& PartitionMetadata::get_in_sync_replica_brokers() const {
    return isrs_;
}

// TopicMetadata 

TopicMetadata::TopicMetadata(const rd_kafka_metadata_topic& topic) 
: name_(topic.topic), error_(topic.err) {
    for (int i = 0; i < topic.partition_cnt; ++i) {
        partitions_.emplace_back(topic.partitions[i]);
    }
}

const string& TopicMetadata::get_name() const {
    return name_;
}

Error TopicMetadata::get_error() const {
    return error_;
}

const vector<PartitionMetadata>& TopicMetadata::get_partitions() const {
    return partitions_;
}

// BrokerMetadata

BrokerMetadata::BrokerMetadata(const rd_kafka_metadata_broker_t& broker) 
: host_(broker.host), id_(broker.id), port_(static_cast<uint16_t>(broker.port)) {

}

const string& BrokerMetadata::get_host() const {
    return host_;
}

int32_t BrokerMetadata::get_id() const {
    return id_;
}

uint16_t BrokerMetadata::get_port() const {
    return port_;
}

// Metadata

void dummy_metadata_destroyer(const rd_kafka_metadata_t*) {

}

Metadata Metadata::make_non_owning(const rd_kafka_metadata_t* handle) {
    return Metadata(handle, NonOwningTag{});
}

Metadata::Metadata()
: handle_(nullptr, nullptr) {

}

Metadata::Metadata(const rd_kafka_metadata_t* handle)
: handle_(handle, &rd_kafka_metadata_destroy) {

}

Metadata::Metadata(const rd_kafka_metadata_t* handle, NonOwningTag)
: handle_(handle, &dummy_metadata_destroyer) {

}

vector<BrokerMetadata> Metadata::get_brokers() const {
    assert(handle_);
    vector<BrokerMetadata> output;
    for (int i = 0; i < handle_->broker_cnt; ++i) {
        const rd_kafka_metadata_broker_t& broker = handle_->brokers[i];
        output.emplace_back(broker);
    }
    return output;
}

vector<TopicMetadata> Metadata::get_topics() const {
    assert(handle_);
    vector<TopicMetadata> output;
    for (int i = 0; i < handle_->topic_cnt; ++i) {
        const rd_kafka_metadata_topic_t& topic = handle_->topics[i];
        output.emplace_back(topic);
    }
    return output;
}

vector<TopicMetadata> Metadata::get_topics(const unordered_set<string>& topics) const {
    assert(handle_);
    vector<TopicMetadata> output;
    for (int i = 0; i < handle_->topic_cnt; ++i) {
        const rd_kafka_metadata_topic_t& topic = handle_->topics[i];
        if (topics.count(topic.topic)) {
            output.emplace_back(topic);
        }
    }
    return output;
}

vector<TopicMetadata> Metadata::get_topics_prefixed(const string& prefix) const {
    assert(handle_);
    vector<TopicMetadata> output;
    for (int i = 0; i < handle_->topic_cnt; ++i) {
        const rd_kafka_metadata_topic_t& topic = handle_->topics[i];
        string topic_name = topic.topic; 
        if (topic_name.find(prefix) == 0) {
            output.emplace_back(topic);
        }
    }
    return output;
}


Metadata::operator bool() const {
    return handle_ != nullptr;
}

const rd_kafka_metadata_t* Metadata::get_handle() const {
    return handle_.get();
}

} // cppkafka
