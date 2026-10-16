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

#ifndef CPPKAFKA_TOPIC_H
#define CPPKAFKA_TOPIC_H

#include <string>
#include <memory>
#include <librdkafka/rdkafka.h>
#include "macros.h"

namespace cppkafka {

/**
 * \brief Represents a rdkafka topic
 *
 * This is a simple wrapper over a rd_kafka_topic_t*
 */
class CPPKAFKA_API Topic {
public:
    /**
     * \brief Creates a Topic object that doesn't take ownership of the handle
     *
     * \param handle The handle to be used
     */
    static Topic make_non_owning(rd_kafka_topic_t* handle);

    /**
     * \brief Constructs an empty topic
     *
     * Note that using any methods except Topic::get_handle on an empty topic is undefined 
     * behavior
     */
    Topic();

    /**
     * \brief Constructs a topic using a handle
     *
     * This will take ownership of the handle
     *
     * \param handle The handle to be used
     */
    Topic(rd_kafka_topic_t* handle);

    /**
     * Returns the topic name
     */
    std::string get_name() const;

    /** 
     * \brief Check if the partition is available
     *
     * This translates into a call to rd_kafka_topic_partition_available
     *
     * \param partition The partition to check
     */
    bool is_partition_available(int partition) const;
    
    /**
     * Indicates whether this topic is valid (not null)
     */
    explicit operator bool() const {
        return handle_ != nullptr;
    }

    /**
     * Returns the rdkakfa handle
     */
    rd_kafka_topic_t* get_handle() const;
private:
    using HandlePtr = std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)>;

    struct NonOwningTag { };

    Topic(rd_kafka_topic_t* handle, NonOwningTag);

    HandlePtr handle_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_H
