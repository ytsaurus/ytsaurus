#pragma once

#include <mapreduce/yt/interface/common.h>

#include <contrib/libs/protobuf/message.h>

namespace NYT {
    TTableSchema CreateTableSchema(
        const ::google::protobuf::Descriptor& proto,
        const TKeyColumns& keyColumns = TKeyColumns(),
        const bool keepFieldsWithoutExtension = true);

    template<class TProtoType>
    inline TTableSchema CreateTableSchema(const TKeyColumns& keyColumns = TKeyColumns(),
                                          const bool keepFieldsWithoutExtension = true) {
        static_assert(std::is_base_of<::google::protobuf::Message, TProtoType>::value, "Should be base of google::protobuf::Message");
        return CreateTableSchema(*TProtoType::descriptor(), keyColumns, keepFieldsWithoutExtension);
    }

    template <class T>
    TRichYPath WithSchema(const TRichYPath& path, const TKeyColumns& sortBy = TKeyColumns()) {
        TRichYPath schemedPath(path);
        if constexpr(std::is_base_of<::google::protobuf::Message, T>::value) {
            if (!schemedPath.Schema_) {
                schemedPath.Schema(CreateTableSchema<T>(sortBy));
            }
        }
        return schemedPath;
    }
}
