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

#include "exceptions.h"

using std::string;
using std::to_string;

namespace cppkafka {

// Exception

Exception::Exception(string message) 
: message_(move(message)) {

}

const char* Exception::what() const noexcept {
    return message_.data();
}

// ConfigException

ConfigException::ConfigException(const string& config_name, const string& error)
: Exception("Failed to set " + config_name + ": " + error) {

}

// ConfigOptionNotFound

ConfigOptionNotFound::ConfigOptionNotFound(const string& config_name) 
: Exception(config_name + " not found") {

}

// InvalidConfigOptionType

InvalidConfigOptionType::InvalidConfigOptionType(const string& config_name, const string& type) 
: Exception(config_name + " could not be converted to " + type) {

}

// ElementNotFound

ElementNotFound::ElementNotFound(const string& element_type, const string& name)
: Exception("Could not find " + element_type + " for " + name) {

}

// ParseException

ParseException::ParseException(const string& message)
: Exception(message) {

}

// UnexpectedVersion

UnexpectedVersion::UnexpectedVersion(uint32_t version)
: Exception("Unexpected version " + to_string(version)) {
}

// HandleException

HandleException::HandleException(Error error) 
: Exception(error.to_string()), error_(error) {

}

Error HandleException::get_error() const {
    return error_;
}

// ConsumerException

ConsumerException::ConsumerException(Error error) 
: Exception(error.to_string()), error_(error) {

}

Error ConsumerException::get_error() const {
    return error_;
}

// QueueException

QueueException::QueueException(Error error)
: Exception(error.to_string()), error_(error) {

}

Error QueueException::get_error() const {
    return error_;
}

// ActionTerminatedException

ActionTerminatedException::ActionTerminatedException(const string& error) 
: Exception(error) {

}
    
} // cppkafka
