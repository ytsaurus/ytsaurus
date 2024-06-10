#pragma once

#include "parse_config.h"

#include <mapreduce/yt/interface/common.h>

#include <google/protobuf/message.h>

NYT::TTableSchema CreateTableSchemaFromProto(
        const ::google::protobuf::Message& proto,
        const TParseConfig& config = {});

template<class TProtoType>
inline NYT::TTableSchema CreateTableSchemaFromProto(const TParseConfig& config = {}) {
    static_assert(std::is_base_of<::google::protobuf::Message, TProtoType>::value, "Should be based of google::protobuf::Message");
    TProtoType proto;
    return CreateTableSchemaFromProto(proto, config);
}
