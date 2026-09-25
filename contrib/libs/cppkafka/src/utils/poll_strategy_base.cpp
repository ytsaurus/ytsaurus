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

#include "utils/poll_strategy_base.h"
#include "consumer.h"

using std::chrono::milliseconds;

namespace cppkafka {

PollStrategyBase::PollStrategyBase(Consumer& consumer)
: consumer_(consumer),
  consumer_queue_(QueueData{consumer.get_consumer_queue(), boost::any()}) {
    // get all currently active partition assignments
    TopicPartitionList assignment = consumer_.get_assignment();
    on_assignment(assignment);
    
    // take over the assignment callback
    assignment_callback_ = consumer.get_assignment_callback();
    consumer_.set_assignment_callback([this](TopicPartitionList& partitions) {
        on_assignment(partitions);
    });
    // take over the revocation callback
    revocation_callback_ = consumer.get_revocation_callback();
    consumer_.set_revocation_callback([this](const TopicPartitionList& partitions) {
        on_revocation(partitions);
    });
    // take over the rebalance error callback
    rebalance_error_callback_ = consumer.get_rebalance_error_callback();
    consumer_.set_rebalance_error_callback([this](Error error) {
        on_rebalance_error(error);
    });
}

PollStrategyBase::~PollStrategyBase() {
    //reset the original callbacks
    consumer_.set_assignment_callback(assignment_callback_);
    consumer_.set_revocation_callback(revocation_callback_);
    consumer_.set_rebalance_error_callback(rebalance_error_callback_);
}

void PollStrategyBase::set_timeout(milliseconds timeout) {
    consumer_.set_timeout(timeout);
}

milliseconds PollStrategyBase::get_timeout() {
    return consumer_.get_timeout();
}

Consumer& PollStrategyBase::get_consumer() {
    return consumer_;
}

QueueData& PollStrategyBase::get_consumer_queue() {
    return consumer_queue_;
}

PollStrategyBase::QueueMap& PollStrategyBase::get_partition_queues() {
    return partition_queues_;
}

void PollStrategyBase::reset_state() {

}

void PollStrategyBase::assign(TopicPartitionList& partitions) {
    // populate partition queues
    for (const auto& partition : partitions) {
        // get the queue associated with this partition
        partition_queues_.emplace(partition, QueueData{consumer_.get_partition_queue(partition), boost::any()});
    }
    reset_state();
}

void PollStrategyBase::revoke(const TopicPartitionList& partitions) {
    for (const auto &partition : partitions) {
        partition_queues_.erase(partition);
    }
    reset_state();
}

void PollStrategyBase::revoke() {
    partition_queues_.clear();
    reset_state();
}

void PollStrategyBase::on_assignment(TopicPartitionList& partitions) {
    assign(partitions);
    // call original consumer callback if any
    if (assignment_callback_) {
        assignment_callback_(partitions);
    }
}

void PollStrategyBase::on_revocation(const TopicPartitionList& partitions) {
    revoke(partitions);
    // call original consumer callback if any
    if (revocation_callback_) {
        revocation_callback_(partitions);
    }
}

void PollStrategyBase::on_rebalance_error(Error error) {
    reset_state();
    // call original consumer callback if any
    if (rebalance_error_callback_) {
        rebalance_error_callback_(error);
    }
}

} //cppkafka
