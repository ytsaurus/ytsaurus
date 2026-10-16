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

#include "kafka_handle_base.h"
#include "metadata.h"
#include "group_information.h"
#include "exceptions.h"
#include "topic.h"
#include "topic_partition_list.h"

using std::string;
using std::vector;
using std::move;
using std::make_tuple;
using std::lock_guard;
using std::mutex;
using std::exception;
using std::chrono::milliseconds;

namespace cppkafka {

const milliseconds KafkaHandleBase::DEFAULT_TIMEOUT{1000};

KafkaHandleBase::KafkaHandleBase(Configuration config) 
: timeout_ms_(DEFAULT_TIMEOUT), config_(move(config)), handle_(nullptr, HandleDeleter(this)), destroy_flags_(0) {
    auto& maybe_config = config_.get_default_topic_configuration();
    if (maybe_config) {
        maybe_config->set_as_opaque();
        auto conf_handle = rd_kafka_topic_conf_dup(maybe_config->get_handle());
        rd_kafka_conf_set_default_topic_conf(config_.get_handle(), conf_handle);
    }
}

void KafkaHandleBase::pause_partitions(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_pause_partitions(get_handle(), 
                                                          topic_list_handle.get());
    check_error(error, topic_list_handle.get());
}

void KafkaHandleBase::pause(const std::string& topic) {
    pause_partitions(convert(topic, get_metadata(get_topic(topic)).get_partitions()));
}

void KafkaHandleBase::resume_partitions(const TopicPartitionList& topic_partitions) {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_resume_partitions(get_handle(), 
                                                           topic_list_handle.get());
    check_error(error, topic_list_handle.get());
}

void KafkaHandleBase::resume(const std::string& topic) {
    resume_partitions(convert(topic, get_metadata(get_topic(topic)).get_partitions()));
}

void KafkaHandleBase::set_timeout(milliseconds timeout) {
    timeout_ms_ = timeout;
}

void KafkaHandleBase::set_log_level(LogLevel level) {
    rd_kafka_set_log_level(handle_.get(), static_cast<int>(level));
}

void KafkaHandleBase::add_brokers(const string& brokers) {
    rd_kafka_brokers_add(handle_.get(), brokers.data());
}

rd_kafka_t* KafkaHandleBase::get_handle() const {
    return handle_.get();
}

Topic KafkaHandleBase::get_topic(const string& name) {
    save_topic_config(name, TopicConfiguration{});
    return get_topic(name, nullptr);
}

Topic KafkaHandleBase::get_topic(const string& name, TopicConfiguration config) {
    auto handle = config.get_handle();
    save_topic_config(name, move(config));
    return get_topic(name, rd_kafka_topic_conf_dup(handle));
}

KafkaHandleBase::OffsetTuple
KafkaHandleBase::query_offsets(const TopicPartition& topic_partition) const {
    return query_offsets(topic_partition, timeout_ms_);
}

KafkaHandleBase::OffsetTuple
KafkaHandleBase::query_offsets(const TopicPartition& topic_partition,
                               milliseconds timeout) const {
    int64_t low;
    int64_t high;
    const string& topic = topic_partition.get_topic();
    const int partition = topic_partition.get_partition();
    const int timeout_ms = static_cast<int>(timeout.count());
    rd_kafka_resp_err_t result = rd_kafka_query_watermark_offsets(handle_.get(), topic.data(),
                                                                  partition, &low, &high,
                                                                  timeout_ms);
    check_error(result);
    return make_tuple(low, high);
}

Metadata KafkaHandleBase::get_metadata(bool all_topics) const {
    return get_metadata(all_topics, nullptr, timeout_ms_);
}

Metadata KafkaHandleBase::get_metadata(bool all_topics,
                                       milliseconds timeout) const {
    return get_metadata(all_topics, nullptr, timeout);
}

TopicMetadata KafkaHandleBase::get_metadata(const Topic& topic) const {
    return get_metadata(topic, timeout_ms_);
}

TopicMetadata KafkaHandleBase::get_metadata(const Topic& topic,
                                            milliseconds timeout) const {
    Metadata md = get_metadata(false, topic.get_handle(), timeout);
    auto topics = md.get_topics();
    if (topics.empty()) {
        throw ElementNotFound("topic metadata", topic.get_name());
    }
    return topics.front();
}

GroupInformation KafkaHandleBase::get_consumer_group(const string& name) {
    return get_consumer_group(name, timeout_ms_);
}

GroupInformation KafkaHandleBase::get_consumer_group(const string& name,
                                                     milliseconds timeout) {
    auto result = fetch_consumer_groups(name.c_str(), timeout);
    if (result.empty()) {
        throw ElementNotFound("consumer group information", name);
    }
    return move(result[0]);
}

vector<GroupInformation> KafkaHandleBase::get_consumer_groups() {
    return get_consumer_groups(timeout_ms_);
}

vector<GroupInformation> KafkaHandleBase::get_consumer_groups(milliseconds timeout) {
    return fetch_consumer_groups(nullptr, timeout);
}

TopicPartitionList
KafkaHandleBase::get_offsets_for_times(const TopicPartitionsTimestampsMap& queries) const {
    return get_offsets_for_times(queries, timeout_ms_);
}

TopicPartitionList
KafkaHandleBase::get_offsets_for_times(const TopicPartitionsTimestampsMap& queries,
                                       milliseconds timeout) const {
    TopicPartitionList topic_partitions;
    for (const auto& query : queries) {
        const TopicPartition& topic_partition = query.first;
        topic_partitions.emplace_back(topic_partition.get_topic(), topic_partition.get_partition(),
                                      query.second.count());
    }
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    const int timeout_ms = static_cast<int>(timeout.count());
    rd_kafka_resp_err_t result = rd_kafka_offsets_for_times(handle_.get(), topic_list_handle.get(),
                                                            timeout_ms);
    check_error(result, topic_list_handle.get());
    return convert(topic_list_handle);
}

string KafkaHandleBase::get_name() const {
    return rd_kafka_name(handle_.get());
}

milliseconds KafkaHandleBase::get_timeout() const {
    return timeout_ms_;
}

const Configuration& KafkaHandleBase::get_configuration() const {
    return config_;
}

int KafkaHandleBase::get_out_queue_length() const {
    return rd_kafka_outq_len(handle_.get());
}

void KafkaHandleBase::yield() const {
    rd_kafka_yield(handle_.get());
}

void KafkaHandleBase::set_handle(rd_kafka_t* handle) {
    handle_ = HandlePtr(handle, HandleDeleter(this));
}

Topic KafkaHandleBase::get_topic(const string& name, rd_kafka_topic_conf_t* conf) {
    rd_kafka_topic_t* topic = rd_kafka_topic_new(get_handle(), name.data(), conf);
    if (!topic) {
        throw HandleException(rd_kafka_last_error());
    }
    return Topic(topic);
}

Metadata KafkaHandleBase::get_metadata(bool all_topics,
                                       rd_kafka_topic_t* topic_ptr,
                                       milliseconds timeout) const {
    const rd_kafka_metadata_t* metadata;
    const int timeout_ms = static_cast<int>(timeout.count());
    rd_kafka_resp_err_t error = rd_kafka_metadata(get_handle(), !!all_topics,
                                                  topic_ptr, &metadata, timeout_ms);
    check_error(error);
    return Metadata(metadata);
}

vector<GroupInformation> KafkaHandleBase::fetch_consumer_groups(const char* name,
                                                                milliseconds timeout) {
    const rd_kafka_group_list* list = nullptr;
    const int timeout_ms = static_cast<int>(timeout.count());
    auto result = rd_kafka_list_groups(get_handle(), name, &list, timeout_ms);
    check_error(result);

    // Wrap this in a unique_ptr so it gets auto deleted
    using GroupHandle = std::unique_ptr<const rd_kafka_group_list,
                                        decltype(&rd_kafka_group_list_destroy)>;
    GroupHandle group_handle(list, &rd_kafka_group_list_destroy);

    vector<GroupInformation> groups;
    for (int i = 0; i < list->group_cnt; ++i) {
        groups.emplace_back(list->groups[i]);
    }
    return groups;
}

void KafkaHandleBase::save_topic_config(const string& topic_name, TopicConfiguration config) {
    lock_guard<mutex> _(topic_configurations_mutex_);
    auto iter = topic_configurations_.emplace(topic_name, move(config)).first;
    iter->second.set_as_opaque();
}

void KafkaHandleBase::check_error(rd_kafka_resp_err_t error) const {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw HandleException(error);
    }
}

void KafkaHandleBase::check_error(rd_kafka_resp_err_t error,
                                  const rd_kafka_topic_partition_list_t* list_ptr) const {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw HandleException(error);
    }
    if (list_ptr) {
        //check if any partition has errors
        for (int i = 0; i < list_ptr->cnt; ++i) {
            if (list_ptr->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw HandleException(list_ptr->elems[i].err);
            }
        }
    }
}

rd_kafka_conf_t* KafkaHandleBase::get_configuration_handle() {
    return config_.get_handle();
}

#if RD_KAFKA_VERSION >= RD_KAFKA_DESTROY_FLAGS_SUPPORT_VERSION

void KafkaHandleBase::set_destroy_flags(int destroy_flags) {
    destroy_flags_ = destroy_flags;
};

int KafkaHandleBase::get_destroy_flags() const {
    return destroy_flags_;
};

#endif


void KafkaHandleBase::HandleDeleter::operator()(rd_kafka_t* handle) {
#if RD_KAFKA_VERSION >= RD_KAFKA_DESTROY_FLAGS_SUPPORT_VERSION
    rd_kafka_destroy_flags(handle, handle_base_ptr_->get_destroy_flags());
#else
    rd_kafka_destroy(handle);
#endif
}

} // cppkafka
