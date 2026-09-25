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

#ifndef CPPKAFKA_MESSAGE_TIMESTAMP_H
#define CPPKAFKA_MESSAGE_TIMESTAMP_H

#include <chrono>
#include <librdkafka/rdkafka.h>
#include "macros.h"

namespace cppkafka {

/**
 * Represents a message's timestamp
 */
class CPPKAFKA_API MessageTimestamp {
    friend class Message;
public:
    /**
     * The timestamp type
     */
    enum TimestampType {
        CREATE_TIME = RD_KAFKA_TIMESTAMP_CREATE_TIME,
        LOG_APPEND_TIME = RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME
    };
    
    /**
     * Gets the timestamp value. If the timestamp was created with a 'time_point',
     * the duration represents the number of milliseconds since epoch.
     */
    std::chrono::milliseconds get_timestamp() const;

    /**
     * Gets the timestamp type
     */
    TimestampType get_type() const;
private:
    MessageTimestamp(std::chrono::milliseconds timestamp, TimestampType type);
    
    std::chrono::milliseconds timestamp_;
    TimestampType type_;
};

} // cppkafka

#endif //CPPKAFKA_MESSAGE_TIMESTAMP_H
