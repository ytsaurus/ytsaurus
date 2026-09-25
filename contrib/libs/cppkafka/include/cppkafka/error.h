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

#ifndef CPPKAFKA_ERROR_H
#define CPPKAFKA_ERROR_H

#include <string>
#include <iosfwd>
#include <librdkafka/rdkafka.h>
#include "macros.h"

namespace cppkafka {

/**
 * Abstraction for an rdkafka error
 */
class CPPKAFKA_API Error {
public:
    /**
     * @brief Constructs an error object with RD_KAFKA_RESP_ERR_NO_ERROR
     */
    Error() = default;
    /**
     * Constructs an error object
     */
    Error(rd_kafka_resp_err_t error);

    /**
     * Gets the error value
     */
    rd_kafka_resp_err_t get_error() const;

    /**
     * Gets the error string
     */
    std::string to_string() const;

    /**
     * Checks whether this error contains an actual error (and not RD_KAFKA_RESP_ERR_NO_ERROR)
     */
    explicit operator bool() const;

    /**
     * Compares this error for equality
     */
    bool operator==(const Error& rhs) const;

    /**
     * Compares this error for inequality
     */
    bool operator!=(const Error& rhs) const;

    /**
     * Writes this error's string representation into a stream
     */
    CPPKAFKA_API friend std::ostream& operator<<(std::ostream& output, const Error& rhs);
private:
    rd_kafka_resp_err_t error_{RD_KAFKA_RESP_ERR_NO_ERROR};
};

} // cppkafka

#endif // CPPKAFKA_ERROR_H
