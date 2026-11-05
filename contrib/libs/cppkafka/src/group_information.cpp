#include "group_information.h"
#include <cstring>
#include <algorithm>
#include "topic_partition.h"
#include "exceptions.h"
#include "detail/endianness.h"

using std::string;
using std::vector;
using std::memcpy;
using std::distance;

namespace cppkafka {

// MemberAssignmentInformation
MemberAssignmentInformation::MemberAssignmentInformation(const vector<uint8_t>& data) {
    const char* error_msg = "Message is malformed";
    // Version + topic list size
    if (data.size() < sizeof(uint16_t) + sizeof(uint32_t)) {
        throw ParseException(error_msg);
    }
    const uint8_t* ptr = data.data();
    const uint8_t* end = ptr + data.size();
    memcpy(&version_, ptr, sizeof(version_));
    version_ = be16toh(version_);
    ptr += sizeof(version_);

    uint32_t total_topics;
    memcpy(&total_topics, ptr, sizeof(total_topics));
    total_topics = be32toh(total_topics);
    ptr += sizeof(total_topics);

    for (uint32_t i = 0; i != total_topics; ++i) {
        if (ptr + sizeof(uint16_t) > end) {
            throw ParseException(error_msg);
        }
        uint16_t topic_length;
        memcpy(&topic_length, ptr, sizeof(topic_length));
        topic_length = be16toh(topic_length);
        ptr += sizeof(topic_length);

        // Check for string length + size of partitions list
        if (topic_length > distance(ptr, end) + sizeof(uint32_t)) {
            throw ParseException(error_msg);
        }
        string topic_name(ptr, ptr + topic_length);
        ptr += topic_length;

        uint32_t total_partitions;
        memcpy(&total_partitions, ptr, sizeof(total_partitions));
        total_partitions = be32toh(total_partitions);
        ptr += sizeof(total_partitions);

        if (ptr + total_partitions * sizeof(uint32_t) > end) {
            throw ParseException(error_msg);
        }
        for (uint32_t j = 0; j < total_partitions; ++j) {
            uint32_t partition;
            memcpy(&partition, ptr, sizeof(partition));
            partition = be32toh(partition);
            ptr += sizeof(partition);

            topic_partitions_.emplace_back(topic_name, partition);
        }
    }
}

uint16_t MemberAssignmentInformation::get_version() const {
    return version_;
}

const TopicPartitionList& MemberAssignmentInformation::get_topic_partitions() const {
    return topic_partitions_;
}

// GroupMemberInformation

GroupMemberInformation::GroupMemberInformation(const rd_kafka_group_member_info& info)
: member_id_(info.member_id), client_id_(info.client_id), client_host_(info.client_host),
  member_metadata_((uint8_t*)info.member_metadata,
                   (uint8_t*)info.member_metadata + info.member_metadata_size),
  member_assignment_((uint8_t*)info.member_assignment,
                     (uint8_t*)info.member_assignment + info.member_assignment_size) {

}

const string& GroupMemberInformation::get_member_id() const {
    return member_id_;
}

const string& GroupMemberInformation::get_client_id() const {
    return client_id_;
}

const string& GroupMemberInformation::get_client_host() const {
    return client_host_;
}

const vector<uint8_t>& GroupMemberInformation::get_member_metadata() const {
    return member_metadata_;
}

const vector<uint8_t>& GroupMemberInformation::get_member_assignment() const {
    return member_assignment_;
}

// GroupInformation

GroupInformation::GroupInformation(const rd_kafka_group_info& info)
: broker_(info.broker), name_(info.group), error_(info.err), state_(info.state),
  protocol_type_(info.protocol_type), protocol_(info.protocol) {
    for (int i = 0; i < info.member_cnt; ++i) {
        members_.emplace_back(info.members[i]);
    }
}

const BrokerMetadata& GroupInformation::get_broker() const {
    return broker_;
}

const string& GroupInformation::get_name() const {
    return name_;
}

Error GroupInformation::get_error() const {
    return error_;
}

const string& GroupInformation::get_state() const {
    return state_;
}

const string& GroupInformation::get_protocol_type() const {
    return protocol_type_;
}

const string& GroupInformation::get_protocol() const {
    return protocol_;
}

const vector<GroupMemberInformation>& GroupInformation::get_members() const {
    return members_;
}

} // cppkafka
