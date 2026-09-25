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
 
#ifndef CPPKAFKA_HEADER_LIST_ITERATOR_H
#define CPPKAFKA_HEADER_LIST_ITERATOR_H

#include <cstddef>
#include <utility>
#include <iterator>
#include "header.h"

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

namespace cppkafka {

template <typename HeaderType>
class HeaderList;

template <typename HeaderType>
class HeaderIterator;

template <typename HeaderType>
bool operator==(const HeaderIterator<HeaderType>& lhs, const HeaderIterator<HeaderType>& rhs);

/**
 * \brief Iterator over a HeaderList object.
 * \tparam HeaderType The type of header this iterator points to.
 */
template <typename HeaderType>
class HeaderIterator {
public:
    friend HeaderList<HeaderType>;
    using HeaderListType = HeaderList<HeaderType>;
    using BufferType = typename HeaderType::ValueType;
    //std::iterator_traits
    using difference_type = std::ptrdiff_t;
    using value_type = HeaderType;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::bidirectional_iterator_tag;
    friend bool operator==<HeaderType>(const HeaderIterator<HeaderType>& lhs,
                                       const HeaderIterator<HeaderType>& rhs);
    
    HeaderIterator(const HeaderIterator& other)
    : header_list_(other.header_list_),
      header_(make_header(other.header_)),
      index_(other.index_) {
      
    }
    HeaderIterator& operator=(const HeaderIterator& other) {
        if (this == &other) return *this;
        header_list_ = other.header_list_;
        header_ = make_header(other.header_);
        index_ = other.index_;
        return *this;
    }
    HeaderIterator(HeaderIterator&&) = default;
    HeaderIterator& operator=(HeaderIterator&&) = default;
    
    /**
     * \brief Prefix increment of the iterator.
     * \return Itself after being incremented.
     */
    HeaderIterator& operator++() {
        assert(index_ < header_list_.size());
        ++index_;
        return *this;
    }
    
    /**
     * \brief Postfix increment of the iterator.
     * \return Itself before being incremented.
     */
    HeaderIterator operator++(int) {
        HeaderIterator tmp(*this);
        operator++();
        return tmp;
    }
    
    /**
     * \brief Prefix decrement of the iterator.
     * \return Itself after being decremented.
     */
    HeaderIterator& operator--() {
        assert(index_ > 0);
        --index_;
        return *this;
    }
    
    /**
     * \brief Postfix decrement of the iterator.
     * \return Itself before being decremented.
     */
    HeaderIterator operator--(int) {
        HeaderIterator tmp(*this);
        operator--();
        return tmp;
    }
    
    /**
     * \brief Dereferences this iterator.
     * \return A reference to the header the iterator points to.
     * \warning Throws if invalid or if *this == end().
     */
    const HeaderType& operator*() const {
        header_ = header_list_.at(index_);
        return header_;
    }
    HeaderType& operator*() {
        header_ = header_list_.at(index_);
        return header_;
    }
    
    /**
     * \brief Dereferences this iterator.
     * \return The address to the header the iterator points to.
     * \warning Throws if invalid or if *this == end().
     */
    const HeaderType* operator->() const {
        header_ = header_list_.at(index_);
        return &header_;
    }
    HeaderType* operator->() {
        header_ = header_list_.at(index_);
        return &header_;
    }
    
private:
    HeaderIterator(const HeaderListType& headers,
                   size_t index)
    : header_list_(headers),
      index_(index) {
    }
    
    template <typename T>
    T make_header(const T& other) {
        return other;
    }
    
    Header<Buffer> make_header(const Header<Buffer>& other) {
        return Header<Buffer>(other.get_name(),
                              Buffer(other.get_value().get_data(),
                                     other.get_value().get_size()));
    }
    
    const HeaderListType& header_list_;
    HeaderType header_;
    size_t index_;
};

// Equality comparison operators
template <typename HeaderType>
bool operator==(const HeaderIterator<HeaderType>& lhs, const HeaderIterator<HeaderType>& rhs) {
    return (lhs.header_list_.get_handle() == rhs.header_list_.get_handle()) && (lhs.index_ == rhs.index_);
}

template <typename HeaderType>
bool operator!=(const HeaderIterator<HeaderType>& lhs, const HeaderIterator<HeaderType>& rhs) {
    return !(lhs == rhs);
}

} //namespace cppkafka

#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION

#endif //CPPKAFKA_HEADER_LIST_ITERATOR_H

