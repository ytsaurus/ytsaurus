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

#include <errno.h>
#include <memory>
#include "producer.h"
#include "exceptions.h"
#include "message_internal.h"

using std::move;
using std::string;
using std::chrono::milliseconds;
using std::unique_ptr;
using std::get;

namespace cppkafka {

Producer::Producer(Configuration config)
: KafkaHandleBase(move(config)), message_payload_policy_(PayloadPolicy::COPY_PAYLOAD) {
    char error_buffer[512];
    auto config_handle = get_configuration().get_handle();
    rd_kafka_conf_set_opaque(config_handle, this);
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_PRODUCER,
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create producer handle: " + string(error_buffer));
    }
    set_handle(ptr);
}

void Producer::set_payload_policy(PayloadPolicy policy) {
    message_payload_policy_ = policy;
}

Producer::PayloadPolicy Producer::get_payload_policy() const {
    return message_payload_policy_;
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

void Producer::produce(const MessageBuilder& builder) {
    do_produce(builder, MessageBuilder::HeaderListType(builder.header_list())); //copy headers
}

void Producer::produce(MessageBuilder&& builder) {
    do_produce(builder, std::move(builder.header_list())); //move headers
}

void Producer::produce(const Message& message) {
    do_produce(message, HeaderList<Message::HeaderType>(message.get_header_list())); //copy headers
}

void Producer::produce(Message&& message) {
    do_produce(message, message.detach_header_list<Message::HeaderType>()); //move headers
}

#else

void Producer::produce(const MessageBuilder& builder) {
    do_produce(builder);
}

void Producer::produce(MessageBuilder&& builder) {
    do_produce(builder);
}

void Producer::produce(const Message& message) {
    do_produce(message);
}

void Producer::produce(Message&& message) {
    do_produce(message);
}

#endif

int Producer::poll() {
    return poll(get_timeout());
}

int Producer::poll(milliseconds timeout) {
    return rd_kafka_poll(get_handle(), static_cast<int>(timeout.count()));
}

void Producer::flush() {
    flush(get_timeout());
}

void Producer::flush(milliseconds timeout) {
    auto result = rd_kafka_flush(get_handle(), static_cast<int>(timeout.count()));
    check_error(result);
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

void Producer::do_produce(const MessageBuilder& builder,
                          MessageBuilder::HeaderListType&& headers) {
    const Buffer& payload = builder.payload();
    const Buffer& key = builder.key();
    const int policy = static_cast<int>(message_payload_policy_);
    auto result = rd_kafka_producev(get_handle(),
                                    RD_KAFKA_V_TOPIC(builder.topic().data()),
                                    RD_KAFKA_V_PARTITION(builder.partition()),
                                    RD_KAFKA_V_MSGFLAGS(policy),
                                    RD_KAFKA_V_TIMESTAMP(builder.timestamp().count()),
                                    RD_KAFKA_V_KEY((void*)key.get_data(), key.get_size()),
                                    RD_KAFKA_V_HEADERS(headers.get_handle()), //pass ownership to rdkafka
                                    RD_KAFKA_V_VALUE((void*)payload.get_data(), payload.get_size()),
                                    RD_KAFKA_V_OPAQUE(builder.user_data()),
                                    RD_KAFKA_V_END);
    // We only want to release the handle on the headers data
    // if the rd_kafka_producev function returned no error, otherwise
    // the function doesn't take ownership of the headers data.    
    if(result == RD_KAFKA_RESP_ERR_NO_ERROR) {
        headers.release_handle();
    }
    check_error(result);
}

void Producer::do_produce(const Message& message,
                          MessageBuilder::HeaderListType&& headers) {
    const Buffer& payload = message.get_payload();
    const Buffer& key = message.get_key();
    const int policy = static_cast<int>(message_payload_policy_);
    int64_t duration = message.get_timestamp() ? message.get_timestamp().get().get_timestamp().count() : 0;
    auto result = rd_kafka_producev(get_handle(),
                                    RD_KAFKA_V_TOPIC(message.get_topic().data()),
                                    RD_KAFKA_V_PARTITION(message.get_partition()),
                                    RD_KAFKA_V_MSGFLAGS(policy),
                                    RD_KAFKA_V_TIMESTAMP(duration),
                                    RD_KAFKA_V_KEY((void*)key.get_data(), key.get_size()),
                                    RD_KAFKA_V_HEADERS(headers.get_handle()), //pass ownership to rdkafka
                                    RD_KAFKA_V_VALUE((void*)payload.get_data(), payload.get_size()),
                                    RD_KAFKA_V_OPAQUE(message.get_user_data()),
                                    RD_KAFKA_V_END);
    // We only want to release the handle on the headers data
    // if the rd_kafka_producev function returned no error, otherwise
    // the function doesn't take ownership of the headers data.    
    if(result == RD_KAFKA_RESP_ERR_NO_ERROR) {
        headers.release_handle();
    }
    check_error(result);
}

#else

void Producer::do_produce(const MessageBuilder& builder) {
    const Buffer& payload = builder.payload();
    const Buffer& key = builder.key();
    const int policy = static_cast<int>(message_payload_policy_);
    auto result = rd_kafka_producev(get_handle(),
                                    RD_KAFKA_V_TOPIC(builder.topic().data()),
                                    RD_KAFKA_V_PARTITION(builder.partition()),
                                    RD_KAFKA_V_MSGFLAGS(policy),
                                    RD_KAFKA_V_TIMESTAMP(builder.timestamp().count()),
                                    RD_KAFKA_V_KEY((void*)key.get_data(), key.get_size()),
                                    RD_KAFKA_V_VALUE((void*)payload.get_data(), payload.get_size()),
                                    RD_KAFKA_V_OPAQUE(builder.user_data()),
                                    RD_KAFKA_V_END);
    check_error(result);
}

void Producer::do_produce(const Message& message) {
    const Buffer& payload = message.get_payload();
    const Buffer& key = message.get_key();
    const int policy = static_cast<int>(message_payload_policy_);
    int64_t duration = message.get_timestamp() ? message.get_timestamp().get().get_timestamp().count() : 0;
    auto result = rd_kafka_producev(get_handle(),
                                    RD_KAFKA_V_TOPIC(message.get_topic().data()),
                                    RD_KAFKA_V_PARTITION(message.get_partition()),
                                    RD_KAFKA_V_MSGFLAGS(policy),
                                    RD_KAFKA_V_TIMESTAMP(duration),
                                    RD_KAFKA_V_KEY((void*)key.get_data(), key.get_size()),
                                    RD_KAFKA_V_VALUE((void*)payload.get_data(), payload.get_size()),
                                    RD_KAFKA_V_OPAQUE(message.get_user_data()),
                                    RD_KAFKA_V_END);
    check_error(result);
}

#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION

} // cppkafka
