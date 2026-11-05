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

#ifndef CPPKAFKA_TOPIC_PARTITION_LIST_H
#define CPPKAFKA_TOPIC_PARTITION_LIST_H

#include <memory>
#include <iosfwd>
#include <algorithm>
#include <vector>
#include <set>
#include <librdkafka/rdkafka.h>
#include "macros.h"

namespace cppkafka {

class TopicPartition;
class PartitionMetadata;

using TopicPartitionsListPtr = std::unique_ptr<rd_kafka_topic_partition_list_t, 
                                               decltype(&rd_kafka_topic_partition_list_destroy)>;
/**
 * A topic partition list
 */
using TopicPartitionList = std::vector<TopicPartition>;

// Conversions between rdkafka handles and TopicPartitionList
CPPKAFKA_API TopicPartitionsListPtr convert(const TopicPartitionList& topic_partitions);
CPPKAFKA_API TopicPartitionList convert(const TopicPartitionsListPtr& topic_partitions);
CPPKAFKA_API TopicPartitionList convert(rd_kafka_topic_partition_list_t* topic_partitions);
CPPKAFKA_API TopicPartitionList convert(const std::string& topic,
                                        const std::vector<PartitionMetadata>& partition_metadata);
CPPKAFKA_API TopicPartitionsListPtr make_handle(rd_kafka_topic_partition_list_t* handle);

// Extracts a partition list subset belonging to the provided topics (case-insensitive)
CPPKAFKA_API TopicPartitionList find_matches(const TopicPartitionList& partitions,
                                             const std::set<std::string>& topics);

// Extracts a partition list subset belonging to the provided partition ids
// Note: this assumes that all topic partitions in the original list belong to the same topic
//       otherwise the partition ids may not be unique
CPPKAFKA_API TopicPartitionList find_matches(const TopicPartitionList& partitions,
                                             const std::set<int>& ids);

CPPKAFKA_API std::ostream& operator<<(std::ostream& output, const TopicPartitionList& rhs);

} // cppkafka

#endif // CPPKAFKA_TOPIC_PARTITION_LIST_H
