/*
 * Copyright (c) 2018, Matias Fontanini
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

#include "event.h"

using std::allocator;
using std::string;
using std::unique_ptr;
using std::vector;

namespace cppkafka {

Event::Event(rd_kafka_event_t* handle)
: handle_(handle, &rd_kafka_event_destroy) {

}

string Event::get_name() const {
    return rd_kafka_event_name(handle_.get());
}

rd_kafka_event_type_t Event::get_type() const {
    return rd_kafka_event_type(handle_.get());
}

Message Event::get_next_message() const {
    // Note: the constness in rd_kafka_event_message_next's return value is not needed and it
    // breaks Message's interface. This is dirty but it looks like it should have no side effects.
    const auto message =
        const_cast<rd_kafka_message_t*>(rd_kafka_event_message_next(handle_.get()));
    return Message::make_non_owning(message);
}

vector<Message> Event::get_messages() {
    return get_messages(allocator<Message>());
}

size_t Event::get_message_count() const {
    return rd_kafka_event_message_count(handle_.get());
}

Error Event::get_error() const {
    return rd_kafka_event_error(handle_.get());
}

void* Event::get_opaque() const {
    return rd_kafka_event_opaque(handle_.get());
}

TopicPartition Event::get_topic_partition() const {
    using TopparHandle = unique_ptr<rd_kafka_topic_partition_t,
                                    decltype(&rd_kafka_topic_partition_destroy)>;
    TopparHandle toppar_handle{rd_kafka_event_topic_partition(handle_.get()),
                               &rd_kafka_topic_partition_destroy};
    return TopicPartition(toppar_handle->topic, toppar_handle->partition, toppar_handle->offset);
}

TopicPartitionList Event::get_topic_partition_list() const {
    auto toppars_handle = rd_kafka_event_topic_partition_list(handle_.get());
    return convert(toppars_handle);
}

Event::operator bool() const {
    return !!handle_;
}

} // cppkafka
