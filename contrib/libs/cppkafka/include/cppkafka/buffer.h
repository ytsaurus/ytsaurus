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

#ifndef CPPKAFKA_BUFFER_H
#define CPPKAFKA_BUFFER_H

#include <cstddef>
#include <array>
#include <vector>
#include <iosfwd>
#include <algorithm>
#include "macros.h"
#include "exceptions.h"

namespace cppkafka {

/**
 * \brief Represents a view of a buffer.
 * 
 * This is only a view, hence you should convert the contents of a buffer into
 * some other container if you want to store it somewhere. 
 *
 * If you're using this to produce a message, you *need* to guarantee that the
 * pointer that this buffer points to will still until the call to Producer::produce
 * returns.
 */
class CPPKAFKA_API Buffer {
public:
    /**
     * The type of data this buffer points to
     */
    using DataType = unsigned char;

    /**
     * The const iterator type
     */
    using const_iterator = const DataType*;

    /**
     * Constructs an empty buffer
     */
    Buffer();

    /**
     * Constructs a buffer from a pointer and a size
     *
     * \param data A pointer to some type of size 1
     * \param size The size of the buffer
     */
    template <typename T>
    Buffer(const T* data, size_t size) 
    : data_(reinterpret_cast<const DataType*>(data)), size_(size) {
        static_assert(sizeof(T) == sizeof(DataType), "sizeof(T) != sizeof(DataType)");
        if ((data_ == nullptr) && (size_ > 0)) {
            throw Exception("Invalid buffer configuration");
        }
    }
    
    /**
     * Constructs a buffer from two iterators in the range [first,last)
     *
     * \param first An iterator to the start of data
     * \param last An iterator to the end of data (not included)
     */
    template <typename Iter>
    Buffer(const Iter first, const Iter last)
    : Buffer(&*first, std::distance(first, last)) {
    }

    /**
     * Constructs a buffer from a vector
     *
     * \param data The vector to be used as input
     */
    template <typename T>
    Buffer(const std::vector<T>& data)
    : data_(data.data()), size_(data.size()) {
        static_assert(sizeof(T) == sizeof(DataType), "sizeof(T) != sizeof(DataType)");
    }

    /**
     * Don't allow construction from temporary vectors
     */
    template <typename T>
    Buffer(std::vector<T>&& data) = delete;

    /**
     * Constructs a buffer from an array
     *
     * \param data The the array to be used as input
     */
    template <typename T, size_t N>
    Buffer(const std::array<T, N>& data)
    : data_(reinterpret_cast<const DataType*>(data.data())), size_(data.size()) {
        static_assert(sizeof(T) == sizeof(DataType), "sizeof(T) != sizeof(DataType)");
    }

    /**
     * Don't allow construction from temporary arrays
     */
    template <typename T, size_t N>
    Buffer(std::array<T, N>&& data) = delete;
    
    /**
     * Constructs a buffer from a raw array
     *
     * \param data The the array to be used as input
     */
    template <typename T, size_t N>
    Buffer(const T(&data)[N])
    : Buffer(data, N) {
    }
    
     // Don't allow construction from temporary raw arrays
    template <typename T, size_t N>
    Buffer(T(&&data)[N]) = delete;

    /**
     * \brief Construct a buffer from a const string ref
     *
     * Note that you *can't use temporaries* here as they would be destructed after
     * the constructor finishes.
     */
    Buffer(const std::string& data); 

    /**
     * Don't allow construction from temporary strings
     */
    Buffer(std::string&&) = delete;
    
    Buffer(const Buffer&) = delete;
    Buffer(Buffer&&) = default;
    Buffer& operator=(const Buffer&) = delete;
    Buffer& operator=(Buffer&&) = default;

    /**
     * Getter for the data pointer
     */
    const DataType* get_data() const;

    /**
     * Getter for the size of the buffer
     */
    size_t get_size() const;

    /**
     * Gets an iterator to the beginning of this buffer
     */
    const_iterator begin() const;

    /**
     * Gets an iterator to the end of this buffer
     */
    const_iterator end() const;

    /**
     * Checks whether this is a non empty buffer
     */
    explicit operator bool() const;

    /**
     * Converts the contents of the buffer into a string
     */
    operator std::string() const;

    /**
     * \brief Converts the contents of the buffer into a vector.
     *
     * The vector must contain some type of size 1 (e.g. uint8_t, char, etc).
     */
    template <typename T>
    operator std::vector<T>() const {
        static_assert(sizeof(T) == sizeof(DataType), "sizeof(T) != sizeof(DataType)");
        return std::vector<T>(data_, data_ + size_);
    }

    /**
     * Output operator
     */
    CPPKAFKA_API friend std::ostream& operator<<(std::ostream& output, const Buffer& rhs);
private:
    const DataType* data_;
    size_t size_;
};

/**
 * Compares Buffer objects for equality 
 */
CPPKAFKA_API bool operator==(const Buffer& lhs, const Buffer& rhs);

/**
 * Compares Buffer objects for inequality 
 */
CPPKAFKA_API bool operator!=(const Buffer& lhs, const Buffer& rhs);

/**
 * Compares Buffer objects lexicographically
 */
CPPKAFKA_API bool operator<(const Buffer& lhs, const Buffer& rhs);
CPPKAFKA_API bool operator<=(const Buffer& lhs, const Buffer& rhs);
CPPKAFKA_API bool operator>(const Buffer& lhs, const Buffer& rhs);
CPPKAFKA_API bool operator>=(const Buffer& lhs, const Buffer& rhs);

} // cppkafka

#endif // CPPKAFKA_BUFFER_H
