#pragma once

#include <contrib/ydb/core/scheme_types/scheme_type_info.h>
#include <contrib/ydb/library/mkql_proto/protos/minikql.pb.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NScheme {

void ProtoMiniKQLTypeFromTypeInfo(NKikimrMiniKQL::TType* type, const TTypeInfo typeInfo);
TTypeInfo TypeInfoFromProtoMiniKQLType(const NKikimrMiniKQL::TType& type);

TTypeInfo TypeInfoFromMiniKQLType(const NMiniKQL::TType* type);

} // namespace NKikimr::NScheme
