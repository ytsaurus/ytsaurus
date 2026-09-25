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

#ifndef CPPKAFKA_TOPIC_PARTITION_H
#define CPPKAFKA_TOPIC_PARTITION_H

#include <string>
#include <iosfwd>
#include <cstdint>
#include "macros.h"

namespace cppkafka {

/**
 * Represents a topic/partition
 */
class CPPKAFKA_API TopicPartition {
public:
    /**
     * Special offsets enum
     */
    enum Offset {
        OFFSET_BEGINNING = -2,
        OFFSET_END = -1,
        OFFSET_STORED = -1000,
        OFFSET_INVALID = -1001
    };

    /**
     * Default constructs a topic/partition
     */
    TopicPartition();

    /**
     * \brief Constructs a topic/partition
     * 
     * The partition value will be RD_KAFKA_OFFSET_INVALID
     *
     * \param topic The topic name
     */
    TopicPartition(const char* topic);
    
    /**
     * \brief Constructs a topic/partition
     * 
     * The partition value will be RD_KAFKA_OFFSET_INVALID
     *
     * \param topic The topic name
     */
    TopicPartition(std::string topic);
    
    /**
     * Constructs a topic/partition
     *
     * \param topic The topic name
     * \param partition The partition to be used
     */
    TopicPartition(std::string topic, int partition);
    
    /**
     * Constructs a topic/partition
     *
     * \param topic The topic name
     * \param partition The partition to be used
     * \param offset The offset to be used
     */
    TopicPartition(std::string topic, int partition, int64_t offset);

    /**
     * Gets the topic name
     */
    const std::string& get_topic() const;

    /**
     * Gets the partition
     */
    int get_partition() const;

    /**
     * Gets the offset
     */
    int64_t get_offset() const;
    
    /**
     * @brief Sets the partition
     */
    void set_partition(int partition);

    /**
     * Sets the offset
     */
    void set_offset(int64_t offset);

    /**
     * Compare the (topic, partition) for less-than equality
     */
    bool operator<(const TopicPartition& rhs) const;

    /**
     * Compare the (topic, partition) for equality
     */
    bool operator==(const TopicPartition& rhs) const;

    /**
     * Compare the (topic, partition) for in-equality
     */
    bool operator!=(const TopicPartition& rhs) const;

    /**
     * Print to a stream
     */
    CPPKAFKA_API friend std::ostream& operator<<(std::ostream& output, const TopicPartition& rhs);
private:
    std::string topic_;
    int partition_;
    int64_t offset_;
};

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_H
