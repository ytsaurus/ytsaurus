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

#ifndef CPPKAFKA_BACKOFF_PERFORMER_H
#define CPPKAFKA_BACKOFF_PERFORMER_H

#include <chrono>
#include <functional>
#include <thread>
#include "../consumer.h"
#include "../exceptions.h"

namespace cppkafka {

/**
 * 
 */
class CPPKAFKA_API BackoffPerformer {
public:
    using TimeUnit = std::chrono::milliseconds;
    static const TimeUnit DEFAULT_INITIAL_BACKOFF;
    static const TimeUnit DEFAULT_BACKOFF_STEP;
    static const TimeUnit DEFAULT_MAXIMUM_BACKOFF;
    static const size_t DEFAULT_MAXIMUM_RETRIES;

    /**
     * The backoff policy to use
     */
    enum class BackoffPolicy {
        LINEAR,
        EXPONENTIAL
    };

    /**
     * Constructs an instance of backoff performer
     * 
     * By default, the linear backoff policy is used
     */
    BackoffPerformer();

    /**
     * \brief Sets the backoff policy
     *
     * \param policy The backoff policy to be used
     */
    void set_backoff_policy(BackoffPolicy policy);

    /**
     * \brief Sets the initial backoff
     *
     * The first time a commit fails, this will be the delay between the request is sent
     * and we re-try doing so 
     *
     * \param value The value to be used
     */
    void set_initial_backoff(TimeUnit value);

    /**
     * \brief Sets the backoff step
     *
     * When using the linear backoff policy, this will be the delay between sending a request
     * that fails and re-trying it
     *
     * \param value The value to be used
     */
    void set_backoff_step(TimeUnit value);

    /**
     * \brief Sets the maximum backoff
     *
     * The backoff used will never be larger than this number
     *
     * \param value The value to be used
     */
    void set_maximum_backoff(TimeUnit value);
    
    /**
     * \brief Sets the maximum number of retries for the commit operation
     *
     * \param value The number of retries before giving up
     *
     * \remark Setting value to 0 is equivalent to 1, i.e. it will try at least once
     */
    void set_maximum_retries(size_t value);

    /**
     * \brief Executes an action and backs off if it fails
     *
     * This will call the functor and will retry in case it returns false
     *
     * \param callback The action to be executed
     */
    template <typename Functor>
    void perform(const Functor& callback) {
        TimeUnit backoff = initial_backoff_;
        size_t retries = maximum_retries_;
        while (retries--) {
            auto start = std::chrono::steady_clock::now();
            // If the callback returns true, we're done
            if (callback()) {
                return; //success
            }
            auto end = std::chrono::steady_clock::now();
            auto time_elapsed = end - start;
            // If we still have time left, then sleep
            if (time_elapsed < backoff) {
                std::this_thread::sleep_for(backoff - time_elapsed);
            }
            // Increase out backoff depending on the policy being used
            backoff = increase_backoff(backoff);
        }
        // No more retries left or we have a terminal error.
        throw ActionTerminatedException("Commit failed: no more retries.");
    }
private:
    TimeUnit increase_backoff(TimeUnit backoff);

    TimeUnit initial_backoff_;
    TimeUnit backoff_step_;
    TimeUnit maximum_backoff_;
    BackoffPolicy policy_;
    size_t maximum_retries_;
};

} // cppkafka

#endif // CPPKAFKA_BACKOFF_PERFORMER_H
