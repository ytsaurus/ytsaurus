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

#ifndef CPPKAFKA_EXCEPTIONS_H
#define CPPKAFKA_EXCEPTIONS_H

#include <stdexcept>
#include <string>
#include <librdkafka/rdkafka.h>
#include "macros.h"
#include "error.h"

namespace cppkafka {

/**
 * Base class for all cppkafka exceptions
 */
class CPPKAFKA_API Exception : public std::exception {
public:
    Exception(std::string message);

    const char* what() const noexcept;
private:
    std::string message_;
};

/**
 * A configuration related error
 */
class CPPKAFKA_API ConfigException : public Exception {
public:
    ConfigException(const std::string& config_name, const std::string& error);
};

/** 
 * Indicates a configuration option was not set
 */
class CPPKAFKA_API ConfigOptionNotFound : public Exception {
public:
    ConfigOptionNotFound(const std::string& config_name);
};

/** 
 * Indicates a configuration option value could not be converted to a specified type
 */
class CPPKAFKA_API InvalidConfigOptionType : public Exception {
public:
    InvalidConfigOptionType(const std::string& config_name, const std::string& type);
};

/** 
 * Indicates something that was being looked up failed to be found
 */
class CPPKAFKA_API ElementNotFound : public Exception {
public:
    ElementNotFound(const std::string& element_type, const std::string& name);
};

/** 
 * Indicates something that was incorrectly parsed
 */
class CPPKAFKA_API ParseException : public Exception {
public:
    ParseException(const std::string& message);
};

/** 
 * Indicates something had an unexpected versiom
 */
class CPPKAFKA_API UnexpectedVersion : public Exception {
public:
    UnexpectedVersion(uint32_t version);
};

/**
 * A generic rdkafka handle error
 */
class CPPKAFKA_API HandleException : public Exception {
public:
    HandleException(Error error);

    Error get_error() const;
private:
    Error error_;
};

/**
 * Consumer exception
 */
class CPPKAFKA_API ConsumerException : public Exception {
public:
    ConsumerException(Error error);

    Error get_error() const;
private:
    Error error_;
};

/**
 * Queue exception for rd_kafka_queue_t errors
 */
class CPPKAFKA_API QueueException : public Exception {
public:
    QueueException(Error error);

    Error get_error() const;
private:
    Error error_;
};

/**
 * Backoff performer has no more retries left for a specific action.
 */
class CPPKAFKA_API ActionTerminatedException : public Exception {
public:
    ActionTerminatedException(const std::string& error);
};

} // cppkafka

#endif // CPPKAFKA_EXCEPTIONS_H
