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

#ifndef CPPKAFKA_CONFIGURATION_OPTION_H
#define CPPKAFKA_CONFIGURATION_OPTION_H

#include <string>
#include <type_traits>
#include "macros.h"

namespace cppkafka {

/**
 * Wrapper over a configuration (key, value) pair
 */
class CPPKAFKA_API ConfigurationOption {
public:
    /**
     * Construct using a std::string value
     */
    ConfigurationOption(const std::string& key, const std::string& value);
    
    /**
     * Construct using a const char* value
     */
    ConfigurationOption(const std::string& key, const char* value);
    
    /**
     * Construct using a bool value
     */
    ConfigurationOption(const std::string& key, bool value);

    /**
     * Construct using any integral value
     */
    template <typename T,
              typename = typename std::enable_if<std::is_integral<T>::value>::type>
    ConfigurationOption(const std::string& key, T value)
    : ConfigurationOption(key, std::to_string(value)) {

    }

    /**
     * Gets the key
     */
    const std::string& get_key() const;

    /**
     * Gets the value
     */
    const std::string& get_value() const;
private:
    std::string key_;
    std::string value_;
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_OPTION_H
