#pragma once

#include <yt/yt/client/table_client/public.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>


namespace NKikimr::NMiniKQL {

class TType;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec::NPrivate {

struct TMemberDescriptor {
    TStringBuf Name;

    const NKikimr::NMiniKQL::TType* Type;
    // index of field in TUnboxedValuePod
    TMaybe<ui32> Index;

    const NYT::NTableClient::TLogicalType* YtType;
    // index of value in TUnversionedRow or nested struct
    TMaybe<ui32> YtIndex;
};

} // namespace NYql::NYtflow::NCodec::NPrivate
