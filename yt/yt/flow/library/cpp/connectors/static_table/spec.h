#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/connectors/static_table_v2/spec.h>

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/sink.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

struct TArrivalOrderTableSinkParameters
    : public ISink::TParameters
{
    NYPath::TRichYPath OutputDirectory;
    TDuration TablePeriod;
    TDuration TableTtl;
    std::string TableNameFormat;
    std::optional<std::string> DataWeightColumn;

    REGISTER_YSON_STRUCT(TArrivalOrderTableSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArrivalOrderTableSinkParameters);

struct TDynamicArrivalOrderTableSinkParameters
    : public ISink::TDynamicParameters
{
    i64 MaxRowCount{};
    i64 MaxDataWeight{};
    TDuration TransactionTimeout;
    TDuration RetryBackoff;

    REGISTER_YSON_STRUCT(TDynamicArrivalOrderTableSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicArrivalOrderTableSinkParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
