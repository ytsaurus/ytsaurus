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

#ifndef CPPKAFKA_CONFIGURATION_H
#define CPPKAFKA_CONFIGURATION_H

#include <memory>
#include <string>
#include <functional>
#include <initializer_list>
#include <chrono>
#include <boost/optional.hpp>
#include <librdkafka/rdkafka.h>
#include "topic_partition_list.h"
#include "topic_configuration.h"
#include "clonable_ptr.h"
#include "configuration_base.h"
#include "macros.h"
#include "event.h"

namespace cppkafka {

class Message;
class Error;
class Producer;
class Consumer;
class KafkaHandleBase;

/**
 * \brief Represents a global configuration (rd_kafka_conf_t).
 *
 * This wraps an rdkafka configuration handle. It can safely be copied (will use 
 * rd_kafka_conf_dup under the hood) and moved.
 *
 * Some other overloads for Configuration::set are given via ConfigurationBase.
 */
class CPPKAFKA_API Configuration : public ConfigurationBase<Configuration> {
public:
    using DeliveryReportCallback = std::function<void(Producer& producer, const Message&)>;
    using OffsetCommitCallback = std::function<void(Consumer& consumer,
                                                    Error error,
                                                    const TopicPartitionList& topic_partitions)>;
    using ErrorCallback = std::function<void(KafkaHandleBase& handle,
                                             int error,
                                             const std::string& reason)>;
    using ThrottleCallback = std::function<void(KafkaHandleBase& handle,
                                                const std::string& broker_name,
                                                int32_t broker_id,
                                                std::chrono::milliseconds throttle_time)>;
    using LogCallback = std::function<void(KafkaHandleBase& handle,
                                           int level,
                                           const std::string& facility,
                                           const std::string& message)>;
    using StatsCallback = std::function<void(KafkaHandleBase& handle, const std::string& json)>;
    using SocketCallback = std::function<int(int domain, int type, int protocol)>;
    using BackgroundEventCallback = std::function<void(KafkaHandleBase& handle, Event)>;

    using ConfigurationBase<Configuration>::set;
    using ConfigurationBase<Configuration>::get;

    /**
     * Default constructs a Configuration object
     */
    Configuration();

    /**
     * Constructs a Configuration object using a list of options
     */
    Configuration(const std::vector<ConfigurationOption>& options);

    /**
     * Constructs a Configuration object using a list of options
     */
    Configuration(const std::initializer_list<ConfigurationOption>& options);

    /**
     * \brief Sets an attribute.
     *
     * This will call rd_kafka_conf_set under the hood.
     *
     * \param name The name of the attribute
     * \param value The value of the attribute
     */
    Configuration& set(const std::string& name, const std::string& value);

    /**
     * Sets the delivery report callback (invokes rd_kafka_conf_set_dr_msg_cb)
     */
    Configuration& set_delivery_report_callback(DeliveryReportCallback callback);

    /**
     * Sets the offset commit callback (invokes rd_kafka_conf_set_offset_commit_cb)
     */
    Configuration& set_offset_commit_callback(OffsetCommitCallback callback);

    /** 
     * Sets the error callback (invokes rd_kafka_conf_set_error_cb)
     */
    Configuration& set_error_callback(ErrorCallback callback);

    /** 
     * Sets the throttle callback (invokes rd_kafka_conf_set_throttle_cb)
     */
    Configuration& set_throttle_callback(ThrottleCallback callback);

    /** 
     * Sets the log callback (invokes rd_kafka_conf_set_log_cb)
     */
    Configuration& set_log_callback(LogCallback callback);

    /** 
     * Sets the stats callback (invokes rd_kafka_conf_set_stats_cb)
     */
    Configuration& set_stats_callback(StatsCallback callback);

    /** 
     * Sets the socket callback (invokes rd_kafka_conf_set_socket_cb)
     */
    Configuration& set_socket_callback(SocketCallback callback);

#if RD_KAFKA_VERSION >= RD_KAFKA_ADMIN_API_SUPPORT_VERSION
    /**
     * Sets the background event callback (invokes rd_kafka_conf_set_background_event_cb)
     */
    Configuration& set_background_event_callback(BackgroundEventCallback callback);
    
    /**
     * Sets the event mask (invokes rd_kafka_conf_set_events)
     */
    Configuration& set_events(int events);
#endif

    /** 
     * Sets the default topic configuration
     */
    Configuration& set_default_topic_configuration(TopicConfiguration config);

    /**
     * Returns true if the given property name has been set
     */
    bool has_property(const std::string& name) const;

    /** 
     * Gets the rdkafka configuration handle
     */
    rd_kafka_conf_t* get_handle() const;

    /**
     * Gets an option value
     *
     * \throws ConfigOptionNotFound if the option is not present
     */
    std::string get(const std::string& name) const;

    /**
     * Gets all options, including default values which are set by rdkafka
     */
    std::map<std::string, std::string> get_all() const;

    /**
     * Gets the delivery report callback
     */
    const DeliveryReportCallback& get_delivery_report_callback() const;

    /**
     * Gets the offset commit callback
     */
    const OffsetCommitCallback& get_offset_commit_callback() const;

    /**
     * Gets the error callback
     */
    const ErrorCallback& get_error_callback() const;

    /**
     * Gets the throttle callback
     */
    const ThrottleCallback& get_throttle_callback() const;

    /**
     * Gets the log callback
     */
    const LogCallback& get_log_callback() const;

    /**
     * Gets the stats callback
     */
    const StatsCallback& get_stats_callback() const;

    /**
     * Gets the socket callback
     */
    const SocketCallback& get_socket_callback() const;

    /**
     * Gets the background event callback
     */
    const BackgroundEventCallback& get_background_event_callback() const;

    /**
     * Gets the default topic configuration
     */
    const boost::optional<TopicConfiguration>& get_default_topic_configuration() const;

    /**
     * Gets the default topic configuration
     */
    boost::optional<TopicConfiguration>& get_default_topic_configuration();
private:
    using HandlePtr = ClonablePtr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy),
                                  decltype(&rd_kafka_conf_dup)>;

    Configuration(rd_kafka_conf_t* ptr);
    static HandlePtr make_handle(rd_kafka_conf_t* ptr);

    HandlePtr handle_;
    boost::optional<TopicConfiguration> default_topic_config_;
    DeliveryReportCallback delivery_report_callback_;
    OffsetCommitCallback offset_commit_callback_;
    ErrorCallback error_callback_;
    ThrottleCallback throttle_callback_;
    LogCallback log_callback_;
    StatsCallback stats_callback_;
    SocketCallback socket_callback_;
    BackgroundEventCallback background_event_callback_;
};

} // cppkafka

#endif // CPPKAFKA_CONFIGURATION_H
