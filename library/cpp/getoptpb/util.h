#pragma once

#include "getoptpb.h"

#include <library/cpp/getoptpb/proto/confoption.pb.h>

#include <util/generic/maybe.h>

#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/generated_enum_util.h>
#include <type_traits>

namespace NGetoptPb {
    template <typename TProtoEnum, typename = std::enable_if_t< ::google::protobuf::is_proto_enum<TProtoEnum>::value>>
    TMaybe<TString> GetEnumValue(TProtoEnum v) {
        auto desc = ::google::protobuf::GetEnumDescriptor<TProtoEnum>();
        auto vdesc = desc->FindValueByNumber(v);
        Y_ENSURE(vdesc);
        auto const& opts = vdesc->options();
        if (opts.HasExtension(Val)) {
            return opts.GetExtension(Val);
        }
        return {};
    }
}
