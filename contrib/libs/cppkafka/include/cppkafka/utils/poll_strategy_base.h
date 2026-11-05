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
 
#ifndef CPPKAFKA_POLL_STRATEGY_BASE_H
#define CPPKAFKA_POLL_STRATEGY_BASE_H

#include <map>
#include <boost/any.hpp>
#include "../queue.h"
#include "../topic_partition_list.h"
#include "poll_interface.h"
#include "../macros.h"

namespace cppkafka {

/**
 * \brief Contains a partition queue and generic metadata which can be used to store
 *        related (user-specific) information.
 */
struct QueueData {
    Queue       queue;
    boost::any  metadata;
};

/**
 * \class PollStrategyBase
 *
 * \brief Base implementation of  the PollInterface
 */
class CPPKAFKA_API PollStrategyBase : public PollInterface {
public:
    using QueueMap = std::map<TopicPartition, QueueData>;
    
    /**
     * \brief Constructor
     *
     * \param consumer A reference to the polled consumer instance
     */
    explicit PollStrategyBase(Consumer& consumer);
    
    /**
     * \brief Destructor
     */
    ~PollStrategyBase();
    
    /**
     * \sa PollInterface::set_timeout
     */
    void set_timeout(std::chrono::milliseconds timeout) override;
    
    /**
     * \sa PollInterface::get_timeout
     */
    std::chrono::milliseconds get_timeout() override;
    
    /**
     * \sa PollInterface::get_consumer
     */
    Consumer& get_consumer() final;
    
    /**
     * \brief Creates partitions queues associated with the supplied partitions.
     *
     * This method contains a default implementation. It adds all the new queues belonging
     * to the provided partition list and calls reset_state().
     * To be used with static consumers.
     *
     * \param partitions Assigned topic partitions.
     */
    virtual void assign(TopicPartitionList& partitions);
    
    /**
     * \brief Removes partitions queues associated with the supplied partitions.
     *
     * This method contains a default implementation. It removes all the queues
     * belonging to the provided partition list and calls reset_state().
     * To be used with static consumers.
     *
     * \param partitions Revoked topic partitions.
     */
    virtual void revoke(const TopicPartitionList& partitions);
    
    /**
     * \brief Removes all partitions queues associated with the supplied partitions.
     *
     * This method contains a default implementation. It removes all the queues
     * currently assigned and calls reset_state(). To be used with static consumers.
     */
    virtual void revoke();
    
protected:
    /**
     * \brief Get the queues from all assigned partitions
     *
     * \return A map of queues indexed by partition
     */
    QueueMap& get_partition_queues();
    
    /**
     * \brief Get the main consumer queue which services the underlying Consumer object
     *
     * \return The consumer queue
     */
    QueueData& get_consumer_queue();
    
    /**
     * \brief Reset the internal state of the queues.
     *
     * Use this function to reset the state of any polling strategy or algorithm.
     *
     * \remark This function gets called by on_assignement(), on_revocation() and on_rebalance_error()
     */
    virtual void reset_state();
    
    /**
     * \brief Function to be called when a new partition assignment takes place
     *
     * This method contains a default implementation. It calls assign()
     * and invokes the user assignment callback.
     *
     * \param partitions Assigned topic partitions
     */
    virtual void on_assignment(TopicPartitionList& partitions);
    
    /**
     * \brief Function to be called when an old partition assignment gets revoked
     *
     * This method contains a default implementation. It calls revoke()
     * and invokes the user revocation callback.
     *
     * \param partitions Revoked topic partitions
     */
    virtual void on_revocation(const TopicPartitionList& partitions);
    
    /**
     * \brief Function to be called when a topic rebalance error happens
     *
     * This method contains a default implementation. Calls reset_state().
     *
     * \param error The rebalance error
     */
    virtual void on_rebalance_error(Error error);
    
private:
    Consumer&                           consumer_;
    QueueData                           consumer_queue_;
    QueueMap                            partition_queues_;
    Consumer::AssignmentCallback        assignment_callback_;
    Consumer::RevocationCallback        revocation_callback_;
    Consumer::RebalanceErrorCallback    rebalance_error_callback_;
};

} //cppkafka

#endif //CPPKAFKA_POLL_STRATEGY_BASE_H
