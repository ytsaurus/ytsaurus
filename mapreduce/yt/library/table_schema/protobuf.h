#pragma once

#include <mapreduce/yt/interface/common.h>

#include <contrib/libs/protobuf/message.h>

namespace NYT {
    TTableSchema CreateTableSchema(const ::google::protobuf::Descriptor& proto, const TKeyColumns& keyColumns = TKeyColumns());

    template<class TProtoType>
    inline TTableSchema CreateTableSchema(const TKeyColumns& keyColumns = TKeyColumns()) {
        static_assert(std::is_base_of<::google::protobuf::Message, TProtoType>::value, "Should be base of google::protobuf::Message");
        return CreateTableSchema(*TProtoType::descriptor(), keyColumns);
    }
}
