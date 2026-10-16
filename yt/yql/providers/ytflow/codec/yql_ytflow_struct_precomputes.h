#pragma once

#include "yql_ytflow_member_descriptor.h"

#include <yt/yt/client/table_client/public.h>

#include <util/generic/hash.h>

#include <utility>


namespace NKikimr::NMiniKQL {

class TType;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec::NPrivate {

struct TStructPrecomputes {
public:
    // mapping (structType, memberIndex) -> member descriptor
    THashMap<
        std::pair<const NKikimr::NMiniKQL::TType*, ui32>,
        TMemberDescriptor
    > MemberDescriptors;

    // mapping (ytStructType, memberIndex) -> member descriptor
    THashMap<
        std::pair<const NYT::NTableClient::TLogicalType*, ui32>,
        TMemberDescriptor
    > YtMemberDescriptors;

    // field indices which are present in yql struct type but absent from yt struct type
    THashMap<const NKikimr::NMiniKQL::TType*, TVector<ui32>> ExtraMembers;
};

} // namespace NYql::NYtflow::NCodec::NPrivate
