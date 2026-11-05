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

#ifndef CPPKAFKA_EVENT_H
#define CPPKAFKA_EVENT_H

#include <memory>
#include <string>
#include <vector>
#include "error.h"
#include "message.h"
#include "topic_partition.h"
#include "topic_partition_list.h"

namespace cppkafka {

class Event {
public:
    /**
     * Construct an Event from a rdkafka event handle and take ownership of it
     *
     * /param handle The handle to construct this event from
     */
    Event(rd_kafka_event_t* handle);

    /**
     * Returns the name of this event
     */
    std::string get_name() const;

    /**
     * Returns the type of this event
     */
    rd_kafka_event_type_t get_type() const;

    /**
     * \brief Gets the next message contained in this event.
     *
     * This call is only valid if the event type is one of:
     * * RD_KAFKA_EVENT_FETCH
     * * RD_KAFKA_EVENT_DR
     *
     * \note The returned message's lifetime *is tied to this Event*. That is, if the event
     * is free'd so will the contents of the message.
     */
    Message get_next_message() const;

    /**
     * \brief Gets all messages in this event (if any)
     *
     * This call is only valid if the event type is one of:
     * * RD_KAFKA_EVENT_FETCH
     * * RD_KAFKA_EVENT_DR
     *
     * \note The returned messages' lifetime *is tied to this Event*. That is, if the event
     * is free'd so will the contents of the messages.
     *
     * \return A vector containing 0 or more messages
     */
    std::vector<Message> get_messages();

    /**
     * \brief Gets all messages in this event (if any)
     *
     * This call is only valid if the event type is one of:
     * * RD_KAFKA_EVENT_FETCH
     * * RD_KAFKA_EVENT_DR
     *
     * \param allocator The allocator to use on the output vector
     *
     * \note The returned messages' lifetime *is tied to this Event*. That is, if the event
     * is free'd so will the contents of the messages.
     *
     * \return A vector containing 0 or more messages
     */
    template <typename Allocator>
    std::vector<Message, Allocator> get_messages(const Allocator allocator);

    /**
     * \brief Gets the number of messages contained in this event
     *
     * This call is only valid if the event type is one of:
     * * RD_KAFKA_EVENT_FETCH
     * * RD_KAFKA_EVENT_DR
     */
    size_t get_message_count() const;

    /**
     * \brief Returns the error in this event
     */
    Error get_error() const;

    /**
     * Gets the opaque pointer in this event
     */
    void* get_opaque() const;

#if RD_KAFKA_VERSION >= RD_KAFKA_EVENT_STATS_SUPPORT_VERSION
    /**
     * \brief Gets the stats in this event
     *
     * This call is only valid if the event type is RD_KAFKA_EVENT_STATS
     */
    std::string get_stats() const {
        return rd_kafka_event_stats(handle_.get());
    }
#endif

    /**
     * \brief Gets the topic/partition for this event
     *
     * This call is only valid if the event type is RD_KAFKA_EVENT_ERROR
     */
    TopicPartition get_topic_partition() const;

    /**
     * \brief Gets the list of topic/partitions in this event
     *
     * This call is only valid if the event type is one of:
     * * RD_KAFKA_EVENT_REBALANCE
     * * RD_KAFKA_EVENT_OFFSET_COMMIT
     */
    TopicPartitionList get_topic_partition_list() const;

    /**
     * Check whether this event is valid
     *
     * /return true iff this event has a valid (non-null) handle inside
     */
    operator bool() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_event_t, decltype(&rd_kafka_event_destroy)>;

    HandlePtr handle_;
};

template <typename Allocator>
std::vector<Message, Allocator> Event::get_messages(const Allocator allocator) {
    const size_t total_messages = get_message_count();
    std::vector<const rd_kafka_message_t*> raw_messages(total_messages);
    const auto messages_read = rd_kafka_event_message_array(handle_.get(),
                                                            raw_messages.data(),
                                                            total_messages);
    std::vector<Message, Allocator> output(allocator);
    output.reserve(messages_read);
    for (auto message : raw_messages) {
        output.emplace_back(Message::make_non_owning(const_cast<rd_kafka_message_t*>(message)));
    }
    return output;
}

} // cppkafka

#endif // CPPKAFKA_EVENT_H
