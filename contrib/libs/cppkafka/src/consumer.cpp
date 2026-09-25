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
#include <sstream>
#include <algorithm>
#include <cctype>
#include "macros.h"
#include "consumer.h"
#include "exceptions.h"
#include "logging.h"
#include "configuration.h"
#include "topic_partition_list.h"
#include "detail/callback_invoker.h"

using std::vector;
using std::string;
using std::move;
using std::make_tuple;
using std::ostringstream;
using std::chrono::milliseconds;
using std::toupper;
using std::equal;
using std::allocator;

namespace cppkafka {

void Consumer::rebalance_proxy(rd_kafka_t*, rd_kafka_resp_err_t error,
                               rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    TopicPartitionList list = convert(partitions);
    static_cast<Consumer*>(opaque)->handle_rebalance(error, list);
}

Consumer::Consumer(Configuration config)
: KafkaHandleBase(move(config)) {
    char error_buffer[512];
    rd_kafka_conf_t* config_handle = get_configuration_handle();
    // Set ourselves as the opaque pointer
    rd_kafka_conf_set_opaque(config_handle, this);
    rd_kafka_conf_set_rebalance_cb(config_handle, &Consumer::rebalance_proxy);
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_CONSUMER,
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create consumer handle: " + string(error_buffer));
    }
    rd_kafka_poll_set_consumer(ptr);
    set_handle(ptr);
}

Consumer::~Consumer() {
    try {
        // make sure to destroy the function closures. in case they hold kafka
        // objects, they will need to be destroyed before we destroy the handle
        assignment_callback_ = nullptr;
        revocation_callback_ = nullptr;
        rebalance_error_callback_ = nullptr;
        close();
    }
    catch (const HandleException& ex) {
        ostringstream error_msg;
        error_msg << "Failed to close consumer [" << get_name() << "]: " << ex.what();
        CallbackInvoker<Configuration::ErrorCallback> error_cb("error", get_configuration().get_error_callback(), this);
        CallbackInvoker<Configuration::LogCallback> logger_cb("log", get_configuration().get_log_callback(), nullptr);
        if (error_cb) {
            error_cb(*this, static_cast<int>(ex.get_error().get_error()), error_msg.str());
        }
        else if (logger_cb) {
            logger_cb(*this, static_cast<int>(LogLevel::LogErr), "cppkafka", error_msg.str());
        }
        else {
            rd_kafka_log_print(get_handle(), static_cast<int>(LogLevel::LogErr), "cppkafka", error_msg.str().c_str());
        }
    }
}

void Consumer::set_assignment_callback(AssignmentCallback callback) {
    assignment_callback_ = move(callback);
}

void Consumer::set_revocation_callback(RevocationCallback callback) {
    revocation_callback_ = move(callback);
}

void Consumer::set_rebalance_error_callback(RebalanceErrorCallback callback) {
    rebalance_error_callback_ = move(callback);
}

void Consumer::subscribe(const vector<string>& topics) {
    TopicPartitionList topic_partitions(topics.begin(), topics.end());
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_subscribe(get_handle(), topic_list_handle.get());
    check_error(error);
}

void Consumer::unsubscribe() {
    rd_kafka_resp_err_t error = rd_kafka_unsubscribe(get_handle());
    check_error(error);
}

void Consumer::assign(const TopicPartitionList& topic_partitions) {
    rd_kafka_resp_err_t error;
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    error = rd_kafka_assign(get_handle(), topic_list_handle.get());
    check_error(error, topic_list_handle.get());
}

void Consumer::unassign() {
    rd_kafka_resp_err_t error = rd_kafka_assign(get_handle(), nullptr);
    check_error(error);
}

void Consumer::pause() {
    pause_partitions(get_assignment());
}

void Consumer::resume() {
    resume_partitions(get_assignment());
}

void Consumer::commit() {
    commit(nullptr, false);
}

void Consumer::async_commit() {
    commit(nullptr, true);
}

void Consumer::commit(const Message& msg) {
    commit(msg, false);
}

void Consumer::async_commit(const Message& msg) {
    commit(msg, true);
}

void Consumer::commit(const TopicPartitionList& topic_partitions) {
    commit(&topic_partitions, false);
}

void Consumer::async_commit(const TopicPartitionList& topic_partitions) {
    commit(&topic_partitions, true);
}

KafkaHandleBase::OffsetTuple Consumer::get_offsets(const TopicPartition& topic_partition) const {
    int64_t low;
    int64_t high;
    const string& topic = topic_partition.get_topic();
    const int partition = topic_partition.get_partition();
    rd_kafka_resp_err_t result = rd_kafka_get_watermark_offsets(get_handle(), topic.data(),
                                                                partition, &low, &high);
    check_error(result);
    return make_tuple(low, high);
}

TopicPartitionList
Consumer::get_offsets_committed(const TopicPartitionList& topic_partitions) const {
    return get_offsets_committed(topic_partitions, get_timeout());
}

TopicPartitionList
Consumer::get_offsets_committed(const TopicPartitionList& topic_partitions,
                                milliseconds timeout) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_committed(get_handle(), topic_list_handle.get(),
                                                   static_cast<int>(timeout.count()));
    check_error(error, topic_list_handle.get());
    return convert(topic_list_handle);
}

TopicPartitionList
Consumer::get_offsets_position(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_position(get_handle(), topic_list_handle.get());
    check_error(error, topic_list_handle.get());
    return convert(topic_list_handle);
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION)
void Consumer::store_consumed_offsets() const {
    store_offsets(get_offsets_position(get_assignment()));
}

void Consumer::store_offsets(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_offsets_store(get_handle(), topic_list_handle.get());
    check_error(error, topic_list_handle.get());
}
#endif

void Consumer::store_offset(const Message& msg) const {
    rd_kafka_resp_err_t error = rd_kafka_offset_store(msg.get_handle()->rkt, msg.get_partition(), msg.get_offset());
    check_error(error);
}

vector<string> Consumer::get_subscription() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_subscription(get_handle(), &list);
    check_error(error);

    auto handle = make_handle(list);
    vector<string> output;
    for (const auto& topic_partition : convert(handle)) {
        output.push_back(topic_partition.get_topic());
    }
    return output;
}

TopicPartitionList Consumer::get_assignment() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_assignment(get_handle(), &list);
    check_error(error);
    return convert(make_handle(list));
}

string Consumer::get_member_id() const {
    char* memberid_ptr = rd_kafka_memberid(get_handle());
    string memberid_string = memberid_ptr;
    rd_kafka_mem_free(nullptr, memberid_ptr);
    return memberid_string;
}

const Consumer::AssignmentCallback& Consumer::get_assignment_callback() const {
    return assignment_callback_;
}

const Consumer::RevocationCallback& Consumer::get_revocation_callback() const {
    return revocation_callback_;
}

const Consumer::RebalanceErrorCallback& Consumer::get_rebalance_error_callback() const {
    return rebalance_error_callback_;
}

Message Consumer::poll() {
    return poll(get_timeout());
}

Message Consumer::poll(milliseconds timeout) {
    return rd_kafka_consumer_poll(get_handle(), static_cast<int>(timeout.count()));
}

std::vector<Message> Consumer::poll_batch(size_t max_batch_size) {
    return poll_batch(max_batch_size, get_timeout(), allocator<Message>());
}

std::vector<Message> Consumer::poll_batch(size_t max_batch_size, milliseconds timeout) {
    return poll_batch(max_batch_size, timeout, allocator<Message>());
}

Queue Consumer::get_main_queue() const {
    Queue queue = Queue::make_queue(rd_kafka_queue_get_main(get_handle()));
    queue.disable_queue_forwarding();
    return queue;
}

Queue Consumer::get_consumer_queue() const {
    return Queue::make_queue(rd_kafka_queue_get_consumer(get_handle()));
}

Queue Consumer::get_partition_queue(const TopicPartition& partition) const {
    Queue queue = Queue::make_queue(rd_kafka_queue_get_partition(get_handle(),
                                                                 partition.get_topic().c_str(),
                                                                 partition.get_partition()));
    queue.disable_queue_forwarding();
    return queue;
}

void Consumer::close() {
    rd_kafka_resp_err_t error = rd_kafka_consumer_close(get_handle());
    check_error(error);
}

void Consumer::commit(const Message& msg, bool async) {
    rd_kafka_resp_err_t error;
    error = rd_kafka_commit_message(get_handle(), msg.get_handle(), async ? 1 : 0);
    check_error(error);
}

void Consumer::commit(const TopicPartitionList* topic_partitions, bool async) {
    rd_kafka_resp_err_t error;
    if (topic_partitions == nullptr) {
        error = rd_kafka_commit(get_handle(), nullptr, async ? 1 : 0);
        check_error(error);
    }
    else {
        TopicPartitionsListPtr topic_list_handle = convert(*topic_partitions);
        error = rd_kafka_commit(get_handle(), topic_list_handle.get(), async ? 1 : 0);
        check_error(error, topic_list_handle.get());
    }
}

void Consumer::handle_rebalance(rd_kafka_resp_err_t error,
                                TopicPartitionList& topic_partitions) {
    if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        CallbackInvoker<AssignmentCallback>("assignment", assignment_callback_, this)(topic_partitions);
        assign(topic_partitions);
    }
    else if (error == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        CallbackInvoker<RevocationCallback>("revocation", revocation_callback_, this)(topic_partitions);
        unassign();
    }
    else {
        CallbackInvoker<RebalanceErrorCallback>("rebalance error", rebalance_error_callback_, this)(error);
        unassign();
    }
}

} // cppkafka
