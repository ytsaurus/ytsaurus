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
 
#ifndef CPPKAFKA_HEADER_H
#define CPPKAFKA_HEADER_H

#include "macros.h"
#include "buffer.h"
#include <string>
#include <assert.h>

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

namespace cppkafka {

/**
 * \brief Class representing a rdkafka header.
 *
 * The template parameter 'BufferType' can represent a cppkafka::Buffer, std::string, std::vector, etc.
 * A valid header may contain an empty name as well as null data.
 */
template <typename BufferType>
class Header {
public:
    using ValueType = BufferType;
    
    /**
     * \brief Build an empty header with no data
     */
    Header() = default;
    
    /**
     * \brief Build a header instance
     * \param name The header name
     * \param value The non-modifiable header data
     */
    Header(std::string name,
           const BufferType& value);
    
    /**
     * \brief Build a header instance
     * \param name The header name
     * \param value The header data to be moved
     */
    Header(std::string name,
           BufferType&& value);
    
    /**
     * \brief Get the header name
     * \return A reference to the name
     */
    const std::string& get_name() const;
    
    /**
     * \brief Get the header value
     * \return A const reference to the underlying buffer
     */
    const BufferType& get_value() const;
    
    /**
     * \brief Get the header value
     * \return A non-const reference to the underlying buffer
     */
    BufferType& get_value();
    
    /**
     * \brief Check if this header is empty
     * \return True if the header contains valid data, false otherwise.
     */
    operator bool() const;
    
private:
    template <typename T>
    T make_value(const T& other);
    
    Buffer make_value(const Buffer& other);
    
    std::string name_;
    BufferType value_;
};

// Comparison operators for Header type
template <typename BufferType>
bool operator==(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return std::tie(lhs.get_name(), lhs.get_value()) == std::tie(rhs.get_name(), rhs.get_value());
}

template <typename BufferType>
bool operator!=(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return !(lhs == rhs);
}

template <typename BufferType>
bool operator<(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return std::tie(lhs.get_name(), lhs.get_value()) < std::tie(rhs.get_name(), rhs.get_value());
}

template <typename BufferType>
bool operator>(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return std::tie(lhs.get_name(), lhs.get_value()) > std::tie(rhs.get_name(), rhs.get_value());
}

template <typename BufferType>
bool operator<=(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return !(lhs > rhs);
}

template <typename BufferType>
bool operator>=(const Header<BufferType>& lhs, const Header<BufferType>& rhs) {
    return !(lhs < rhs);
}

// Implementation
template <typename BufferType>
Header<BufferType>::Header(std::string name,
                           const BufferType& value)
: name_(std::move(name)),
  value_(make_value(value)) {
}

template <typename BufferType>
Header<BufferType>::Header(std::string name,
                           BufferType&& value)
: name_(std::move(name)),
  value_(std::move(value)) {
}

template <typename BufferType>
const std::string& Header<BufferType>::get_name() const {
    return name_;
}

template <typename BufferType>
const BufferType& Header<BufferType>::get_value() const {
    return value_;
}

template <typename BufferType>
BufferType& Header<BufferType>::get_value() {
    return value_;
}

template <typename BufferType>
Header<BufferType>::operator bool() const {
    return !value_.empty();
}

template <>
inline
Header<Buffer>::operator bool() const {
    return value_.get_size() > 0;
}

template <typename BufferType>
template <typename T>
T Header<BufferType>::make_value(const T& other) {
    return other;
}

template <typename BufferType>
Buffer Header<BufferType>::make_value(const Buffer& other) {
    return Buffer(other.get_data(), other.get_size());
}

} //namespace cppkafka

#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION

#endif //CPPKAFKA_HEADER_H
