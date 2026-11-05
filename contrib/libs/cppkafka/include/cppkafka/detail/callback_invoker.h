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

#ifndef CPPKAFKA_CALLBACK_INVOKER_H
#define CPPKAFKA_CALLBACK_INVOKER_H

#include <sstream>
#include <assert.h>
#include "../logging.h"
#include "../kafka_handle_base.h"

namespace cppkafka {

// Error values
template <typename T>
T error_value() { return T{}; }

template<> inline
void error_value<void>() {};

template<> inline
bool error_value<bool>() { return false; }

template<> inline
int error_value<int>() { return -1; }

/**
 * \brief Wraps an std::function object and runs it while preventing all exceptions from escaping
 * \tparam Func An std::function object
 */
template <typename Func>
class CallbackInvoker
{
public:
    using RetType = typename Func::result_type;
    using LogCallback = std::function<void(KafkaHandleBase& handle,
                                           int level,
                                           const std::string& facility,
                                           const std::string& message)>;
    CallbackInvoker(const char* callback_name,
                    const Func& callback,
                    KafkaHandleBase* handle)
    : callback_name_(callback_name),
      callback_(callback),
      handle_(handle) {
    }
    
    explicit operator bool() const {
        return (bool)callback_;
    }
    
    template <typename ...Args>
    RetType operator()(Args&&... args) const {
        static const char* library_name = "cppkafka";
        std::ostringstream error_msg;
        try {
            if (callback_) {
                return callback_(std::forward<Args>(args)...);
            }
            return error_value<RetType>();
        }
        catch (const std::exception& ex) {
            if (handle_) {
                error_msg << "Caught exception in " << callback_name_ << " callback: " << ex.what();
            }
        }
        catch (...) {
            if (handle_) {
                error_msg << "Caught unknown exception in " << callback_name_ << " callback";
            }
        }
        // Log error
        if (handle_) {
            if (handle_->get_configuration().get_log_callback()) {
                try {
                    // Log it
                    handle_->get_configuration().get_log_callback()(*handle_,
                                                                    static_cast<int>(LogLevel::LogErr),
                                                                    library_name,
                                                                    error_msg.str());
                }
                catch (...) {} // sink everything
            }
            else {
                rd_kafka_log_print(handle_->get_handle(),
                                   static_cast<int>(LogLevel::LogErr),
                                   library_name,
                                   error_msg.str().c_str());
            }
        }
        return error_value<RetType>();
    }
private:
    const char* callback_name_;
    const Func& callback_;
    KafkaHandleBase* handle_;
};

}

#endif
