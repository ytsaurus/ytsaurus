#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "format.h"
#endif

#include <yt/yt/core/yson/consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TAst>
    requires (std::is_convertible<TAst*, DB::IAST*>::value)
void FormatValue(TStringBuilderBase* builder, const TAst& ast, TStringBuf /*spec*/)
{
    builder->AppendString(ast.formatForLogging());
}

template <class TAst>
    requires (std::is_convertible<TAst*, DB::IAST*>::value)
void FormatValue(TStringBuilderBase* builder, const TAst*& ast, TStringBuf /*spec*/)
{
    if (ast) {
        builder->AppendString(ast->formatForLogging());
    } else {
        builder->AppendChar('#');
    }
}

template <class TAst>
    requires (std::is_convertible<TAst*, DB::IAST*>::value)
void FormatValue(TStringBuilderBase* builder, const std::shared_ptr<TAst>& ast, TStringBuf /*spec*/)
{
    if (ast) {
        builder->AppendString(ast->formatForLogging());
    } else {
        builder->AppendChar('#');
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NYson {

////////////////////////////////////////////////////////////////////////////////

template <class TAst>
void Serialize(const TAst& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    consumer->OnStringScalar(ast.formatForLogging());
}

template <class TAst>
void Serialize(const TAst* ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(ast->formatForLogging());
    } else {
        consumer->OnEntity();
    }
}

template <class TAst>
void Serialize(const std::shared_ptr<TAst>& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(ast->formatForLogging());
    } else {
        consumer->OnEntity();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
