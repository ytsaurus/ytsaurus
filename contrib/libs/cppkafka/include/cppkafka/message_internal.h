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

#ifndef CPPKAFKA_MESSAGE_INTERNAL_H
#define CPPKAFKA_MESSAGE_INTERNAL_H

#include <memory>
#include "macros.h"

namespace cppkafka {

class Message;

class Internal {
public:
    virtual ~Internal() = default;
};
using InternalPtr = std::shared_ptr<Internal>;

/**
 * \brief Private message data structure
 */
class CPPKAFKA_API MessageInternal {
public:
    MessageInternal(void* user_data, std::shared_ptr<Internal> internal);
    static std::unique_ptr<MessageInternal> load(Message& message);
    void* get_user_data() const;
    InternalPtr get_internal() const;
private:
    void*          user_data_;
    InternalPtr    internal_;
};

template <typename BuilderType>
class MessageInternalGuard {
public:
    MessageInternalGuard(BuilderType& builder)
    : builder_(builder),
      user_data_(builder.user_data()) {
        if (builder_.internal()) {
            // Swap contents with user_data
            ptr_.reset(new MessageInternal(user_data_, builder_.internal()));
            builder_.user_data(ptr_.get()); //overwrite user data
        }
    }
    ~MessageInternalGuard() {
        //Restore user data
        builder_.user_data(user_data_);
    }
    void release() {
        ptr_.release();
    }
private:
    BuilderType& builder_;
    std::unique_ptr<MessageInternal> ptr_;
    void* user_data_;
};

}

#endif //CPPKAFKA_MESSAGE_INTERNAL_H
