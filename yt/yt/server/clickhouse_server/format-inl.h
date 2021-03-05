#pragma once
#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "format.h"
#endif

#include <yt/yt/core/misc/format.h>

#include <yt/yt/core/yson/consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TAST>
struct TValueFormatter<TAST, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const TAST& ast, TStringBuf /* format */)
    {
        builder->AppendString(NClickHouseServer::SerializeAndMaybeTruncateSubquery(ast));
    }
};

template <class TAST>
struct TValueFormatter<TAST*, typename std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const TAST* ast, TStringBuf /* format */)
    {
        if (ast) {
            builder->AppendString(NClickHouseServer::SerializeAndMaybeTruncateSubquery(*ast));
        } else {
            builder->AppendChar('#');
        }
    }
};

template <class TAST>
struct TValueFormatter<std::shared_ptr<TAST>, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const std::shared_ptr<TAST>& ast, TStringBuf /* format */)
    {
        if (ast) {
            builder->AppendString(NClickHouseServer::SerializeAndMaybeTruncateSubquery(*ast));
        } else {
            builder->AppendChar('#');
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NYson {

////////////////////////////////////////////////////////////////////////////////

template <class TAST>
void Serialize(const TAST& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>*)
{
    consumer->OnStringScalar(NYT::NClickHouseServer::SerializeAndMaybeTruncateSubquery(ast));
}

template <class TAST>
void Serialize(const TAST* ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(NYT::NClickHouseServer::SerializeAndMaybeTruncateSubquery(*ast));
    } else {
        consumer->OnEntity();
    }
}

template <class TAST>
void Serialize(const std::shared_ptr<TAST>& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(NYT::NClickHouseServer::SerializeAndMaybeTruncateSubquery(*ast));
    } else {
        consumer->OnEntity();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
