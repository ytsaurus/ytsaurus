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

#ifndef CPPKAFKA_BACKOFF_COMMITTER_H
#define CPPKAFKA_BACKOFF_COMMITTER_H

#include <chrono>
#include <functional>
#include <thread>
#include <string>
#include "../consumer.h"
#include "backoff_performer.h"
#include "../detail/callback_invoker.h"
#include "../macros.h"

namespace cppkafka {

/**
 * \brief Allows performing synchronous commits that will backoff until successful
 *
 * This class serves as a simple wrapper around Consumer::commit, allowing to commit
 * messages and topic/partition/offset tuples handling errors and performing a backoff
 * whenever the commit fails.
 *
 * Both linear and exponential backoff policies are supported, the former one being
 * the default.
 *
 * Example code on how to use this:
 *
 * \code
 * // Create a consumer
 * Consumer consumer(...);
 *
 * // Create a committer using this consumer
 * BackoffCommitter committer(consumer);
 *
 * // Set an error callback. This is optional and allows having some feedback 
 * // when commits fail. If the callback returns false, then this message won't
 * // be committer again.
 * committer.set_error_callback([](Error error) {
 *     cout << "Error committing: " << error << endl;
 *     return true; 
 * });
 *
 * // Now commit. If there's an error, this will retry forever
 * committer.commit(some_message);
 * \endcode
 */
class CPPKAFKA_API BackoffCommitter : public BackoffPerformer {
public:
    /**
     * \brief The error callback.
     * 
     * Whenever an error occurs committing an offset, this callback will be executed using
     * the generated error. While the function returns true, then this is offset will be
     * committed again until it either succeeds or the function returns false.
     */
    using ErrorCallback = std::function<bool(Error)>;

    /**
     * \brief Constructs an instance using default values
     *
     * By default, the linear backoff policy is used
     *
     * \param consumer The consumer to use for committing offsets
     */
    BackoffCommitter(Consumer& consumer);

    /**
     * \brief Sets the error callback
     *
     * \sa ErrorCallback
     * \param callback The callback to be set
     */
    void set_error_callback(ErrorCallback callback);
    
    /**
     * \brief Commits the current partition assignment synchronously
     *
     * This will call Consumer::commit() until either the message is successfully
     * committed or the error callback returns false (if any is set).
     */
    void commit();
    
    /**
     * \brief Commits the given message synchronously
     *
     * This will call Consumer::commit(msg) until either the message is successfully
     * committed or the error callback returns false (if any is set).
     *
     * \param msg The message to be committed
     */
    void commit(const Message& msg);

    /**
     * \brief Commits the offsets on the given topic/partitions synchronously
     *
     * This will call Consumer::commit(topic_partitions) until either the offsets are successfully
     * committed or the error callback returns false (if any is set).
     *
     * \param topic_partitions The topic/partition list to be committed
     */
    void commit(const TopicPartitionList& topic_partitions);
    
    /**
     * \brief Get the internal Consumer object
     *
     * \return A reference to the Consumer
     */
    Consumer& get_consumer();
private:
    // If the ReturnType contains 'true', we abort committing. Otherwise we continue.
    // The second member of the ReturnType contains the RdKafka error if any.
    template <typename...Args>
    bool do_commit(Args&&...args) {
        try {
            consumer_.commit(std::forward<Args>(args)...);
            // If the commit succeeds, we're done.
            return true;
        }
        catch (const HandleException& ex) {
            Error error = ex.get_error();
            // If there were actually no offsets to commit, return. Retrying won't solve
            // anything here.
            if (error == RD_KAFKA_RESP_ERR__NO_OFFSET) {
                return true; //not considered an error.
            }
            // If there's a callback and it returns false for this message, abort.
            // Otherwise keep committing.
            CallbackInvoker<ErrorCallback> callback("backoff committer", callback_, &consumer_);
            if (callback && !callback(error)) {
                throw ex; //abort
            }
        }
        return false; //continue
    }

    Consumer& consumer_;
    ErrorCallback callback_;
};

} // cppkafka

#endif // CPPKAFKA_BACKOFF_COMMITTER_H
