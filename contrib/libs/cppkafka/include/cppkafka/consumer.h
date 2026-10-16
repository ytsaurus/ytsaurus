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

#ifndef CPP_KAFKA_CONSUMER_H
#define CPP_KAFKA_CONSUMER_H

#include <vector>
#include <string>
#include <chrono>
#include <functional>
#include "kafka_handle_base.h"
#include "queue.h"
#include "macros.h"
#include "error.h"
#include "detail/callback_invoker.h"

namespace cppkafka {

class TopicConfiguration;

/**
 * \brief High level kafka consumer class
 *
 * Wrapper for the high level consumer API provided by rdkafka. Most methods are just
 * a one to one mapping to rdkafka functions.
 *
 * This class allows hooking up to assignments/revocations via callbacks.
 *
 * Semi-simple code showing how to use this class
 *
 * \code
 * // Create a configuration and set the group.id and broker list fields
 * Configuration config = {
 *     { "metadata.broker.list", "127.0.0.1:9092" },
 *     { "group.id", "foo" }
 * };
 *
 * // Create a consumer
 * Consumer consumer(config);
 *
 * // Set the assignment callback
 * consumer.set_assignment_callback([&](TopicPartitionList& topic_partitions) {
 *     // Here you could fetch offsets and do something, altering the offsets on the
 *     // topic_partitions vector if needed
 *     cout << "Got assigned " << topic_partitions.size() << " partitions!" << endl;
 * });
 *
 * // Set the revocation callback
 * consumer.set_revocation_callback([&](const TopicPartitionList& topic_partitions) {
 *     cout << topic_partitions.size() << " partitions revoked!" << endl;
 * });
 *
 * // Subscribe
 * consumer.subscribe({ "my_topic" });
 * while (true) {
 *     // Poll. This will optionally return a message. It's necessary to check if it's a valid
 *     // one before using it
 *     Message msg = consumer.poll();
 *     if (msg) {
 *         if (!msg.get_error()) {
 *             // It's an actual message. Get the payload and print it to stdout
 *             cout << msg.get_payload().as_string() << endl;
 *         }
 *         else if (!msg.is_eof()) {
 *             // Is it an error notification, handle it.
 *             // This is explicitly skipping EOF notifications as they're not actually errors,
 *             // but that's how rdkafka provides them
 *         }
 *     }
 * }
 * \endcode
 */
class CPPKAFKA_API Consumer : public KafkaHandleBase {
public:
    using AssignmentCallback = std::function<void(TopicPartitionList&)>;
    using RevocationCallback = std::function<void(const TopicPartitionList&)>;
    using RebalanceErrorCallback = std::function<void(Error)>;
    using KafkaHandleBase::pause;
    using KafkaHandleBase::resume;

    /**
     * \brief Creates an instance of a consumer.
     *
     * Note that the configuration *must contain* the group.id attribute set or this
     * will throw.
     *
     * \param config The configuration to be used
     */
    Consumer(Configuration config);
    Consumer(const Consumer&) = delete;
    Consumer(Consumer&&) = delete;
    Consumer& operator=(const Consumer&) = delete;
    Consumer& operator=(Consumer&&) = delete;

    /**
     * \brief Closes and destroys the rdkafka handle
     *
     * This will call Consumer::close before destroying the handle
     */
    ~Consumer();

    /**
     * \brief Sets the topic/partition assignment callback
     *
     * The Consumer class will use rd_kafka_conf_set_rebalance_cb and will handle the
     * rebalance, converting from rdkafka topic partition list handles into TopicPartitionList
     * and executing the assignment/revocation/rebalance_error callbacks.
     *
     * \note You *do not need* to call Consumer::assign with the provided topic parttitions. This
     * will be handled automatically by cppkafka.
     *
     * \param callback The topic/partition assignment callback
     */
    void set_assignment_callback(AssignmentCallback callback);

    /**
     * \brief Sets the topic/partition revocation callback
     *
     * The Consumer class will use rd_kafka_conf_set_rebalance_cb and will handle the
     * rebalance, converting from rdkafka topic partition list handles into TopicPartitionList
     * and executing the assignment/revocation/rebalance_error callbacks.
     *
     * \note You *do not need* to call Consumer::assign with an empty topic partition list or
     * anything like that. That's handled automatically by cppkafka. This is just a notifitation
     * so your application code can react to revocations
     *
     * \param callback The topic/partition revocation callback
     */
    void set_revocation_callback(RevocationCallback callback);

    /**
     * \brief Sets the rebalance error callback
     *
     * The Consumer class will use rd_kafka_conf_set_rebalance_cb and will handle the
     * rebalance, converting from rdkafka topic partition list handles into TopicPartitionList
     * and executing the assignment/revocation/rebalance_error callbacks.
     *
     * \param callback The rebalance error callback
     */
    void set_rebalance_error_callback(RebalanceErrorCallback callback);

    /**
     * \brief Subscribes to the given list of topics
     *
     * This translates to a call to rd_kafka_subscribe
     *
     * \param topics The topics to subscribe to
     */
    void subscribe(const std::vector<std::string>& topics);

    /**
     * \brief Unsubscribes to the current subscription list
     *
     * This translates to a call to rd_kafka_unsubscribe
     */
    void unsubscribe();

    /**
     * \brief Sets the current topic/partition assignment
     *
     * This translates into a call to rd_kafka_assign
     */
    void assign(const TopicPartitionList& topic_partitions);

    /**
     * \brief Unassigns the current topic/partition assignment
     *
     * This translates into a call to rd_kafka_assign using a null as the topic partition list
     * parameter
     */
    void unassign();
    
    /**
     * \brief Pauses all consumption
     */
    void pause();
    
    /**
     * \brief Resumes all consumption
     */
    void resume();
    
    /**
     * \brief Commits the current partition assignment
     *
     * This translates into a call to rd_kafka_commit with a null partition list.
     *
     * \remark This function is equivalent to calling commit(get_assignment())
     */
    void commit();
    
    /**
     * \brief Commits the current partition assignment asynchronously
     *
     * This translates into a call to rd_kafka_commit with a null partition list.
     *
     * \remark This function is equivalent to calling async_commit(get_assignment())
     */
    void async_commit();

    /**
     * \brief Commits the given message synchronously
     *
     * This translates into a call to rd_kafka_commit_message
     *
     * \param msg The message to be committed
     */
    void commit(const Message& msg);

    /**
     * \brief Commits the given message asynchronously
     *
     * This translates into a call to rd_kafka_commit_message
     *
     * \param msg The message to be committed
     */
    void async_commit(const Message& msg);

    /**
     * \brief Commits the offsets on the given topic/partitions synchronously
     *
     * This translates into a call to rd_kafka_commit
     *
     * \param topic_partitions The topic/partition list to be committed
     */
    void commit(const TopicPartitionList& topic_partitions);

    /**
     * \brief Commits the offsets on the given topic/partitions asynchronously
     *
     * This translates into a call to rd_kafka_commit
     *
     * \param topic_partitions The topic/partition list to be committed
     */
    void async_commit(const TopicPartitionList& topic_partitions);

    /**
     * \brief Gets the minimum and maximum offsets for the given topic/partition
     *
     * This translates into a call to rd_kafka_get_watermark_offsets
     *
     * \param topic_partition The topic/partition to get the offsets from
     *
     * \return A pair of offsets {low, high}
     */
    OffsetTuple get_offsets(const TopicPartition& topic_partition) const;

    /**
     * \brief Gets the offsets committed for the given topic/partition list
     *
     * This translates into a call to rd_kafka_committed
     *
     * \param topic_partitions The topic/partition list to be queried
     *
     * \return The topic partition list
     */
    TopicPartitionList get_offsets_committed(const TopicPartitionList& topic_partitions) const;
    
    /**
     * \brief Gets the offsets committed for the given topic/partition list with a timeout
     *
     * This translates into a call to rd_kafka_committed
     *
     * \param topic_partitions The topic/partition list to be queried
     *
     * \param timeout The timeout for this operation. Supersedes the default consumer timeout.
     *
     * \return The topic partition list
     */
    TopicPartitionList get_offsets_committed(const TopicPartitionList& topic_partitions,
                                             std::chrono::milliseconds timeout) const;

    /**
     * \brief Gets the offset positions for the given topic/partition list
     *
     * This translates into a call to rd_kafka_position
     *
     * \param topic_partitions The topic/partition list to be queried
     *
     * \return The topic partition list
     */
    TopicPartitionList get_offsets_position(const TopicPartitionList& topic_partitions) const;
#if (RD_KAFKA_VERSION >= RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION)
    /**
     * \brief Stores the offsets on the currently assigned topic/partitions (legacy).
     *
     * This translates into a call to rd_kafka_offsets_store with the offsets prior to the current assignment positions.
     * It is equivalent to calling rd_kafka_offsets_store(get_offsets_position(get_assignment())).
     *
     * \note When using this API it's recommended to set enable.auto.offset.store=false and enable.auto.commit=true.
     */
    void store_consumed_offsets() const;
    
    /**
     * \brief Stores the offsets on the given topic/partitions (legacy).
     *
     * This translates into a call to rd_kafka_offsets_store.
     *
     * \param topic_partitions The topic/partition list to be stored.
     *
     * \note When using this API it's recommended to set enable.auto.offset.store=false and enable.auto.commit=true.
     */
    void store_offsets(const TopicPartitionList& topic_partitions) const;
#endif
    /**
     * \brief Stores the offset for this message (legacy).
     *
     * This translates into a call to rd_kafka_offset_store.
     *
     * \param msg The message whose offset will be stored.
     *
     * \note When using this API it's recommended to set enable.auto.offset.store=false and enable.auto.commit=true.
     */
    void store_offset(const Message& msg) const;

    /**
     * \brief Gets the current topic subscription
     *
     * This translates to a call to rd_kafka_subscription
     */
    std::vector<std::string> get_subscription() const;

    /**
     * \brief Gets the current topic/partition list assignment
     *
     * This translates to a call to rd_kafka_assignment
     *
     * \return The topic partition list
     */
    TopicPartitionList get_assignment() const;

    /**
     * \brief Gets the group member id
     *
     * This translates to a call to rd_kafka_memberid
     *
     * \return The id
     */
    std::string get_member_id() const;

    /**
     * \brief Gets the partition assignment callback.
     *
     * \return The callback reference
     */
    const AssignmentCallback& get_assignment_callback() const;

    /**
     * \brief Gets the partition revocation callback.
     *
     * \return The callback reference
     */
    const RevocationCallback& get_revocation_callback() const;

    /**
     * \brief Gets the rebalance error callback.
     *
     * \return The callback reference
     */
    const RebalanceErrorCallback& get_rebalance_error_callback() const;

    /**
     * \brief Polls for new messages
     *
     * This will call rd_kafka_consumer_poll.
     *
     * Note that you need to call poll periodically as a keep alive mechanism, otherwise the broker
     * will think this consumer is down and will trigger a rebalance (if using dynamic
     * subscription).
     *
     * The timeout used on this call will be the one configured via Consumer::set_timeout.
     *
     * \return A message. The returned message *might* be empty. It's necessary to check
     * that it's valid before using it:
     *
     * \code
     * Message msg = consumer.poll();
     * if (msg) {
     *     // It's a valid message!
     * }
     * \endcode
     */
    Message poll();

    /**
     * \brief Polls for new messages
     *
     * Same as the other overload of Consumer::poll but the provided timeout will be used
     * instead of the one configured on this Consumer.
     *
     * \param timeout The timeout to be used on this call
     *
     * \return A message
     */
    Message poll(std::chrono::milliseconds timeout);

    /**
     * \brief Polls for a batch of messages
     *
     * This can return zero or more messages
     *
     * \param max_batch_size The maximum amount of messages expected
     * \param alloc The optionally supplied allocator for allocating messages
     *
     * \return A list of messages
     */
    template <typename Allocator>
    std::vector<Message, Allocator> poll_batch(size_t max_batch_size,
                                               const Allocator& alloc);

    /**
     * \brief Polls for a batch of messages
     *
     * This can return zero or more messages
     *
     * \param max_batch_size The maximum amount of messages expected
     *
     * \return A list of messages
     */
    std::vector<Message> poll_batch(size_t max_batch_size);

    /**
     * \brief Polls for a batch of messages
     *
     * This can return zero or more messages
     *
     * \param max_batch_size The maximum amount of messages expected
     * \param timeout The timeout for this operation
     * \param alloc The optionally supplied allocator for allocating messages
     *
     * \return A list of messages
     */
    template <typename Allocator>
    std::vector<Message, Allocator> poll_batch(size_t max_batch_size,
                                               std::chrono::milliseconds timeout,
                                               const Allocator& alloc);

    /**
     * \brief Polls for a batch of messages
     *
     * This can return one or more messages
     *
     * \param max_batch_size The maximum amount of messages expected
     * \param timeout The timeout for this operation
     *
     * \return A list of messages
     */
    std::vector<Message> poll_batch(size_t max_batch_size,
                                    std::chrono::milliseconds timeout);
    
    /**
     * \brief Get the global event queue servicing this consumer corresponding to
     *        rd_kafka_queue_get_main and which is polled via rd_kafka_poll
     *
     * \return A Queue object
     *
     * \remark Note that this call will disable forwarding to the consumer_queue.
     *         To restore forwarding if desired, call Queue::forward_to_queue(consumer_queue)
     */
    Queue get_main_queue() const;
    
    /**
     * \brief Get the consumer group queue servicing corresponding to
     *        rd_kafka_queue_get_consumer and which is polled via rd_kafka_consumer_poll
     *
     * \return A Queue object
     */
    Queue get_consumer_queue() const;
    
    /**
     * \brief Get the queue belonging to this partition. If the consumer is not assigned to this
     *        partition, an empty queue will be returned
     *
     * \param partition The partition object
     *
     * \return A Queue object
     *
     * \remark Note that this call will disable forwarding to the consumer_queue.
     *         To restore forwarding if desired, call Queue::forward_to_queue(consumer_queue)
     */
    Queue get_partition_queue(const TopicPartition& partition) const;
private:
    static void rebalance_proxy(rd_kafka_t *handle, rd_kafka_resp_err_t error,
                                rd_kafka_topic_partition_list_t *partitions, void *opaque);
    void close();
    void commit(const Message& msg, bool async);
    void commit(const TopicPartitionList* topic_partitions, bool async);
    void handle_rebalance(rd_kafka_resp_err_t err, TopicPartitionList& topic_partitions);

    AssignmentCallback assignment_callback_;
    RevocationCallback revocation_callback_;
    RebalanceErrorCallback rebalance_error_callback_;
};

// Implementations
template <typename Allocator>
std::vector<Message, Allocator> Consumer::poll_batch(size_t max_batch_size,
                                                     const Allocator& alloc) {
    return poll_batch(max_batch_size, get_timeout(), alloc);
}

template <typename Allocator>
std::vector<Message, Allocator> Consumer::poll_batch(size_t max_batch_size,
                                                     std::chrono::milliseconds timeout,
                                                     const Allocator& alloc) {
    std::vector<rd_kafka_message_t*> raw_messages(max_batch_size);
    // Note that this will leak the queue when using rdkafka < 0.11.5 (see get_queue comment)
    Queue queue = Queue::make_queue(rd_kafka_queue_get_consumer(get_handle()));
    ssize_t result = rd_kafka_consume_batch_queue(queue.get_handle(),
                                                  timeout.count(),
                                                  raw_messages.data(),
                                                  raw_messages.size());
    if (result == -1) {
        check_error(rd_kafka_last_error());
        // on the off-chance that check_error() does not throw an error
        return std::vector<Message, Allocator>(alloc);
    }
    return std::vector<Message, Allocator>(raw_messages.begin(),
                                           raw_messages.begin() + result,
                                           alloc);
}

} // cppkafka

#endif // CPP_KAFKA_CONSUMER_H
