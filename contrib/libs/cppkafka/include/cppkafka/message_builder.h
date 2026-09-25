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

#ifndef CPPKAFKA_MESSAGE_BUILDER_H
#define CPPKAFKA_MESSAGE_BUILDER_H

#include <chrono>
#include "buffer.h"
#include "topic.h"
#include "macros.h"
#include "message.h"
#include "header_list.h"

namespace cppkafka {

/**
 * \brief Base template class for message construction
 */
template <typename BufferType, typename Concrete>
class BasicMessageBuilder {
public:
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    using HeaderType = Header<BufferType>;
    using HeaderListType = HeaderList<HeaderType>;
#endif
    /**
     * Construct a BasicMessageBuilder
     *
     * \param topic The topic into which this message would be produced
     */
    BasicMessageBuilder(std::string topic);
    
    /**
     * Construct a BasicMessageBuilder from a Message object
     */
    BasicMessageBuilder(const Message& message);

    /**
     * \brief Construct a message builder from another one that uses a different buffer type
     *
     * Note that this can only be used if BufferType can be constructed from an OtherBufferType
     *
     * \param rhs The message builder to be constructed from
     */
    template <typename OtherBufferType, typename OtherConcrete>
    BasicMessageBuilder(const BasicMessageBuilder<OtherBufferType, OtherConcrete>& rhs);
    template <typename OtherBufferType, typename OtherConcrete>
    BasicMessageBuilder(BasicMessageBuilder<OtherBufferType, OtherConcrete>&& rhs);

    /**
     * Default copy and move constructors and assignment operators
     */
    BasicMessageBuilder(BasicMessageBuilder&&) = default;
    BasicMessageBuilder(const BasicMessageBuilder&) = default;
    BasicMessageBuilder& operator=(BasicMessageBuilder&&) = default;
    BasicMessageBuilder& operator=(const BasicMessageBuilder&) = default;

    /**
     * Sets the topic in which this message will be produced
     *
     * \param value The topic to be used
     */
    Concrete& topic(std::string value);

    /**
     * Sets the partition into which this message will be produced
     *
     * \param value The partition to be used  
     */
    Concrete& partition(int value);

    /**
     * Sets the message's key
     *
     * \param value The key to be used
     */
    Concrete& key(const BufferType& value);

    /**
     * Sets the message's key
     *
     * \param value The key to be used
     */
    Concrete& key(BufferType&& value);

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    /**
     *  Add a header(s) to the message
     *
     * \param header The header to be used
     */
    Concrete& header(const HeaderType& header);
    Concrete& headers(const HeaderListType& headers);
    Concrete& headers(HeaderListType&& headers);
#endif
    
    /**
     * Sets the message's payload
     *
     * \param value The payload to be used
     */
    Concrete& payload(const BufferType& value);

    /**
     * Sets the message's payload
     *
     * \param value The payload to be used
     */
    Concrete& payload(BufferType&& value);

    /**
     * Sets the message's timestamp with a 'duration'
     *
     * \param value The timestamp to be used
     */
    Concrete& timestamp(std::chrono::milliseconds value);
    
    /**
     * Sets the message's timestamp with a 'time_point'.
     *
     * \param value The timestamp to be used
     */
    template <typename Clock, typename Duration = typename Clock::duration>
    Concrete& timestamp(std::chrono::time_point<Clock, Duration> value);

    /**
     * Sets the message's user data pointer
     *
     * \param value Pointer to the user data to be used on the produce call
     */
    Concrete& user_data(void* value);

    /**
     * Gets the topic this message will be produced into
     */
    const std::string& topic() const;

    /**
     * Gets the partition this message will be produced into
     */
    int partition() const;

    /**
     * Gets the message's key
     */
    const BufferType& key() const;

    /**
     * Gets the message's key
     */
    BufferType& key();
    
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    /**
     * Gets the list of headers
     */
    const HeaderListType& header_list() const;
    
    /**
     * Gets the list of headers
     */
    HeaderListType& header_list();
#endif
    
    /**
     * Gets the message's payload
     */
    const BufferType& payload() const;

    /**
     * Gets the message's payload
     */
    BufferType& payload();

    /**
     * Gets the message's timestamp as a duration. If the timestamp was created with a 'time_point',
     * the duration represents the number of milliseconds since epoch.
     */
    std::chrono::milliseconds timestamp() const;

    /**
     * Gets the message's user data pointer
     */
    void* user_data() const;
    
    /**
     * Private data accessor (internal use only)
     */
    Message::InternalPtr internal() const;
    Concrete& internal(Message::InternalPtr internal);
    
protected:
    void construct_buffer(BufferType& lhs, const BufferType& rhs);

private:
    Concrete& get_concrete();
    
    std::string topic_;
    int partition_{-1};
    BufferType key_;
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    HeaderListType header_list_;
#endif
    BufferType payload_;
    std::chrono::milliseconds timestamp_{0};
    void* user_data_;
    Message::InternalPtr internal_;
};

template <typename T, typename C>
BasicMessageBuilder<T, C>::BasicMessageBuilder(std::string topic)
: topic_(std::move(topic)),
  user_data_(nullptr) {
}

template <typename T, typename C>
BasicMessageBuilder<T, C>::BasicMessageBuilder(const Message& message)
: topic_(message.get_topic()),
  key_(Buffer(message.get_key().get_data(), message.get_key().get_size())),
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
  //Here we must copy explicitly the Message headers since they are non-owning and this class
  //assumes full ownership. Otherwise we will be holding an invalid handle when Message goes
  //out of scope and rdkafka frees its resource.
  header_list_(message.get_header_list() ?
               HeaderListType(rd_kafka_headers_copy(message.get_header_list().get_handle())) : HeaderListType()), //copy headers
#endif
  payload_(Buffer(message.get_payload().get_data(), message.get_payload().get_size())),
  timestamp_(message.get_timestamp() ? message.get_timestamp().get().get_timestamp() :
                                       std::chrono::milliseconds(0)),
  user_data_(message.get_user_data()),
  internal_(message.internal()) {
  
}

template <typename T, typename C>
template <typename U, typename V>
BasicMessageBuilder<T, C>::BasicMessageBuilder(const BasicMessageBuilder<U, V>& rhs)
: topic_(rhs.topic()),
  partition_(rhs.partition()),
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
  header_list_(rhs.header_list()), //copy headers
#endif
  timestamp_(rhs.timestamp()),
  user_data_(rhs.user_data()),
  internal_(rhs.internal()) {
    get_concrete().construct_buffer(key_, rhs.key());
    get_concrete().construct_buffer(payload_, rhs.payload());
}

template <typename T, typename C>
template <typename U, typename V>
BasicMessageBuilder<T, C>::BasicMessageBuilder(BasicMessageBuilder<U, V>&& rhs)
: topic_(rhs.topic()),
  partition_(rhs.partition()),
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
  header_list_(std::move(header_list())), //assume header ownership
#endif
  timestamp_(rhs.timestamp()),
  user_data_(rhs.user_data()),
  internal_(rhs.internal()) {
    get_concrete().construct_buffer(key_, std::move(rhs.key()));
    get_concrete().construct_buffer(payload_, std::move(rhs.payload()));
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::topic(std::string value) {
    topic_ = std::move(value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::partition(int value) {
    partition_ = value;
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(const T& value) {
    get_concrete().construct_buffer(key_, value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::key(T&& value) {
    key_ = std::move(value);
    return get_concrete();
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
template <typename T, typename C>
C& BasicMessageBuilder<T, C>::header(const HeaderType& header) {
    if (!header_list_) {
        header_list_ = HeaderListType(5);
    }
    header_list_.add(header);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::headers(const HeaderListType& headers) {
    header_list_ = headers;
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::headers(HeaderListType&& headers) {
    header_list_ = std::move(headers);
    return get_concrete();
}
#endif

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(const T& value) {
    get_concrete().construct_buffer(payload_, value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::payload(T&& value) {
    payload_ = std::move(value);
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::timestamp(std::chrono::milliseconds value) {
    timestamp_ = value;
    return get_concrete();
}

template <typename T, typename C>
template <typename Clock, typename Duration>
C& BasicMessageBuilder<T, C>::timestamp(std::chrono::time_point<Clock, Duration> value)
{
    timestamp_ = std::chrono::duration_cast<std::chrono::milliseconds>(value.time_since_epoch());
    return get_concrete();
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::user_data(void* value) {
    user_data_ = value;
    return get_concrete();
}

template <typename T, typename C>
const std::string& BasicMessageBuilder<T, C>::topic() const {
    return topic_;
}

template <typename T, typename C>
int BasicMessageBuilder<T, C>::partition() const {
    return partition_;
}

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::key() const {
    return key_;
}

template <typename T, typename C>
T& BasicMessageBuilder<T, C>::key() {
    return key_;
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
template <typename T, typename C>
const typename BasicMessageBuilder<T, C>::HeaderListType&
BasicMessageBuilder<T, C>::header_list() const {
    return header_list_;
}

template <typename T, typename C>
typename BasicMessageBuilder<T, C>::HeaderListType&
BasicMessageBuilder<T, C>::header_list() {
    return header_list_;
}
#endif

template <typename T, typename C>
const T& BasicMessageBuilder<T, C>::payload() const {
    return payload_;
}

template <typename T, typename C>
T& BasicMessageBuilder<T, C>::payload() {
    return payload_;
}

template <typename T, typename C>
std::chrono::milliseconds BasicMessageBuilder<T, C>::timestamp() const {
    return timestamp_;
}

template <typename T, typename C>
void* BasicMessageBuilder<T, C>::user_data() const {
    return user_data_;
}

template <typename T, typename C>
Message::InternalPtr BasicMessageBuilder<T, C>::internal() const {
    return internal_;
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::internal(Message::InternalPtr internal) {
    internal_ = internal;
    return get_concrete();
}

template <typename T, typename C>
void BasicMessageBuilder<T, C>::construct_buffer(T& lhs, const T& rhs) {
    lhs = rhs;
}

template <typename T, typename C>
C& BasicMessageBuilder<T, C>::get_concrete() {
    return static_cast<C&>(*this);
}

/**
 * \brief Message builder class
 * 
 * Allows building a message including topic, partition, key, payload, etc.
 *
 * Example:
 *
 * \code
 * Producer producer(...);
 * 
 * string payload = "hello world";
 * producer.produce(MessageBuilder("test").partition(5).payload(payload));
 * \endcode
 */
class MessageBuilder : public BasicMessageBuilder<Buffer, MessageBuilder> {
public:
    using Base = BasicMessageBuilder<Buffer, MessageBuilder>;
    using BasicMessageBuilder<Buffer, MessageBuilder>::BasicMessageBuilder;
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    using HeaderType = Base::HeaderType;
    using HeaderListType = Base::HeaderListType;
#endif

    void construct_buffer(Buffer& lhs, const Buffer& rhs) {
        lhs = Buffer(rhs.get_data(), rhs.get_size());
    }

    template <typename T>
    void construct_buffer(Buffer& lhs, T&& rhs) {
        lhs = Buffer(std::forward<T>(rhs));
    }
    

    MessageBuilder clone() const {
        MessageBuilder builder(topic());
        builder.key(Buffer(key().get_data(), key().get_size())).
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
            headers(header_list()).
#endif
            payload(Buffer(payload().get_data(), payload().get_size())).
            timestamp(timestamp()).
            user_data(user_data()).
            internal(internal());
        return builder;
    }
};

/**
 * \brief Message builder class for a specific data type
 */
template <typename T>
class ConcreteMessageBuilder : public BasicMessageBuilder<T, ConcreteMessageBuilder<T>> {
public:
    using Base = BasicMessageBuilder<T, ConcreteMessageBuilder<T>>;
    using BasicMessageBuilder<T, ConcreteMessageBuilder<T>>::BasicMessageBuilder;
#if (RD_KAFKA_VERSION >= RD_KAFKA_HEADERS_SUPPORT_VERSION)
    using HeaderType = typename Base::HeaderType;
    using HeaderListType = typename Base::HeaderListType;
#endif
};

} // cppkafka

#endif // CPPKAFKA_MESSAGE_BUILDER_H
