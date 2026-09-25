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
 
#ifndef CPPKAFKA_HEADER_LIST_H
#define CPPKAFKA_HEADER_LIST_H

#include <librdkafka/rdkafka.h>
#include "clonable_ptr.h"
#include "header.h"
#include "header_list_iterator.h"
#include "exceptions.h"

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)

namespace cppkafka {

/**
 * \brief Thin wrapper over a rd_kafka_headers_t handle which optionally controls its lifetime.
 * \tparam HeaderType The header type
 *
 * This is a copyable and movable class that wraps a rd_kafka_header_t*. When copying this class,
 * all associated headers are also copied via rd_kafka_headers_copy(). If this list owns the underlying handle,
 * its destructor will call rd_kafka_headers_destroy().
 */
template <typename HeaderType>
class HeaderList {
public:
    template <typename OtherHeaderType>
    friend class HeaderList;
    
    using BufferType = typename HeaderType::ValueType;
    using Iterator = HeaderIterator<HeaderType>;
    /**
     * Constructs a message that won't take ownership of the given pointer.
     */
    static HeaderList<HeaderType> make_non_owning(rd_kafka_headers_t* handle);
    
    /**
     * \brief Create an empty header list with no handle.
     */
    HeaderList();
    
    /**
     * \brief Create an empty header list. This call translates to rd_kafka_headers_new().
     * \param reserve The number of headers to reserve space for.
     */
    explicit HeaderList(size_t reserve);
    
    /**
     * \brief Create a header list and assume ownership of the handle.
     * \param handle The header list handle.
     */
    explicit HeaderList(rd_kafka_headers_t* handle);
    
    /**
     * \brief Create a header list from another header list type
     * \param other The other list
     */
    template <typename OtherHeaderType>
    HeaderList(const HeaderList<OtherHeaderType>& other);

    template <typename OtherHeaderType>
    HeaderList(HeaderList<OtherHeaderType>&& other);
    
    /**
     * \brief Add a header to the list. This translates to rd_kafka_header_add().
     * \param header The header.
     * \return An Error indicating if the operation was successful or not.
     * \warning This operation shall invalidate all iterators.
     */
    Error add(const HeaderType& header);
    
    /**
     * \brief Remove all headers with 'name'. This translates to rd_kafka_header_remove().
     * \param name The name of the header(s) to remove.
     * \return An Error indicating if the operation was successful or not.
     * \warning This operation shall invalidate all iterators.
     */
    Error remove(const std::string& name);
    
    /**
     * \brief Return the header present at position 'index'. Throws on error.
     *        This translates to rd_kafka_header_get(index)
     * \param index The header index in the list (0-based).
     * \return The header at that position.
     */
    HeaderType at(size_t index) const; //throws
    
    /**
     * \brief Return the first header in the list. Throws if the list is empty.
     *        This translates to rd_kafka_header_get(0).
     * \return The first header.
     */
    HeaderType front() const; //throws
    
    /**
     * \brief Return the first header in the list. Throws if the list is empty.
     *        This translates to rd_kafka_header_get(size-1).
     * \return The last header.
     */
    HeaderType back() const; //throws
    
    /**
     * \brief Returns the number of headers in the list. This translates to rd_kafka_header_cnt().
     * \return The number of headers.
     */
    size_t size() const;
    
    /**
     * \brief Indicates if this list is empty.
     * \return True if empty, false otherwise.
     */
    bool empty() const;
    
    /**
     * \brief Returns a HeaderIterator pointing to the first position if the list is not empty
     *        or pointing to end() otherwise.
     * \return An iterator.
     * \warning This iterator will be invalid if add() or remove() is called.
     */
    Iterator begin() const;
    
    /**
     * \brief Returns a HeaderIterator pointing to one element past the end of the list.
     * \return An iterator.
     * \remark This iterator cannot be de-referenced.
     */
    Iterator end() const;
    
    /**
     * \brief Get the underlying header list handle.
     * \return The handle.
     */
    rd_kafka_headers_t* get_handle() const;
    
    /**
     * \brief Get the underlying header list handle and release its ownership.
     * \return The handle.
     * \warning After this call, the HeaderList becomes invalid.
     */
    rd_kafka_headers_t* release_handle();
    
    /**
     * \brief Indicates if this list is valid (contains a non-null handle) or not.
     * \return True if valid, false otherwise.
     */
    explicit operator bool() const;
    
private:
    struct NonOwningTag { };
    static void dummy_deleter(rd_kafka_headers_t*) {}
    
    using HandlePtr = ClonablePtr<rd_kafka_headers_t, decltype(&rd_kafka_headers_destroy),
                                  decltype(&rd_kafka_headers_copy)>;
    
    HeaderList(rd_kafka_headers_t* handle, NonOwningTag);
    
    HandlePtr handle_;
};

template <typename HeaderType>
bool operator==(const HeaderList<HeaderType>& lhs, const HeaderList<HeaderType> rhs) {
    if (!lhs && !rhs) {
        return true;
    }
    if (!lhs || !rhs) {
        return false;
    }
    if (lhs.size() != rhs.size()) {
        return false;
    }
    return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

template <typename HeaderType>
bool operator!=(const HeaderList<HeaderType>& lhs, const HeaderList<HeaderType> rhs) {
    return !(lhs == rhs);
}

template <typename HeaderType>
HeaderList<HeaderType> HeaderList<HeaderType>::make_non_owning(rd_kafka_headers_t* handle) {
    return HeaderList(handle, NonOwningTag());
}

template <typename HeaderType>
HeaderList<HeaderType>::HeaderList()
: handle_(nullptr, nullptr, nullptr) {

}

template <typename HeaderType>
HeaderList<HeaderType>::HeaderList(size_t reserve)
: handle_(rd_kafka_headers_new(reserve), &rd_kafka_headers_destroy, &rd_kafka_headers_copy) {
    assert(reserve);
}

template <typename HeaderType>
HeaderList<HeaderType>::HeaderList(rd_kafka_headers_t* handle)
: handle_(handle, &rd_kafka_headers_destroy, &rd_kafka_headers_copy) { //if we own the header list, we clone it on copy
    assert(handle);
}

template <typename HeaderType>
HeaderList<HeaderType>::HeaderList(rd_kafka_headers_t* handle, NonOwningTag)
: handle_(handle, &dummy_deleter, nullptr) { //if we don't own the header list, we forward the handle on copy.
    assert(handle);
}

template <typename HeaderType>
template <typename OtherHeaderType>
HeaderList<HeaderType>::HeaderList(const HeaderList<OtherHeaderType>& other)
: handle_(other.handle_) {

}

template <typename HeaderType>
template <typename OtherHeaderType>
HeaderList<HeaderType>::HeaderList(HeaderList<OtherHeaderType>&& other)
: handle_(std::move(other.handle_)) {

}

// Methods
template <typename HeaderType>
Error HeaderList<HeaderType>::add(const HeaderType& header) {
    assert(handle_);
    return rd_kafka_header_add(handle_.get(),
                               header.get_name().data(),  header.get_name().size(),
                               header.get_value().data(), header.get_value().size());
    
}

template <>
inline
Error HeaderList<Header<Buffer>>::add(const Header<Buffer>& header) {
    assert(handle_);
    return rd_kafka_header_add(handle_.get(),
                               header.get_name().data(), header.get_name().size(),
                               header.get_value().get_data(), header.get_value().get_size());
}

template <typename HeaderType>
Error HeaderList<HeaderType>::remove(const std::string& name) {
    assert(handle_);
    return rd_kafka_header_remove(handle_.get(), name.data());
}

template <typename HeaderType>
HeaderType HeaderList<HeaderType>::at(size_t index) const {
    assert(handle_);
    const char *name, *value;
    size_t size;
    Error error = rd_kafka_header_get_all(handle_.get(), index, &name, reinterpret_cast<const void**>(&value), &size);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw Exception(error.to_string());
    }
    return HeaderType(name, BufferType(value, value + size));
}

template <typename HeaderType>
HeaderType HeaderList<HeaderType>::front() const {
    return at(0);
}

template <typename HeaderType>
HeaderType HeaderList<HeaderType>::back() const {
    return at(size()-1);
}

template <typename HeaderType>
size_t HeaderList<HeaderType>::size() const {
    return handle_ ? rd_kafka_header_cnt(handle_.get()) : 0;
}

template <typename HeaderType>
bool HeaderList<HeaderType>::empty() const {
    return size() == 0;
}

template <typename HeaderType>
typename HeaderList<HeaderType>::Iterator
HeaderList<HeaderType>::begin() const {
    return Iterator(*this, 0);
}

template <typename HeaderType>
typename HeaderList<HeaderType>::Iterator
HeaderList<HeaderType>::end() const {
    return Iterator(*this, size());
}

template <typename HeaderType>
rd_kafka_headers_t* HeaderList<HeaderType>::get_handle() const {
    return handle_.get();
}

template <typename HeaderType>
rd_kafka_headers_t* HeaderList<HeaderType>::release_handle() {
    return handle_.release();
}

template <typename HeaderType>
HeaderList<HeaderType>::operator bool() const {
    return static_cast<bool>(handle_);
}

} //namespace cppkafka

#endif //RD_KAFKA_HEADERS_SUPPORT_VERSION

#endif //CPPKAFKA_HEADER_LIST_H
