#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "format.h"
#endif

#include <yt/yt/core/yson/consumer.h>

#include <Parsers/formatAST.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TAst>
struct TValueFormatter<TAst, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const TAst& ast, TStringBuf /*format*/)
    {
        builder->AppendString(DB::serializeAST(ast));
    }
};

template <class TAst>
struct TValueFormatter<TAst*, typename std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const TAst* ast, TStringBuf /*format*/)
    {
        if (ast) {
            builder->AppendString(DB::serializeAST(*ast));
        } else {
            builder->AppendChar('#');
        }
    }
};

template <class TAst>
struct TValueFormatter<std::shared_ptr<TAst>, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>>
{
    static void Do(TStringBuilderBase* builder, const std::shared_ptr<TAst>& ast, TStringBuf /*format*/)
    {
        if (ast) {
            builder->AppendString(DB::serializeAST(*ast));
        } else {
            builder->AppendChar('#');
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace NYson {

////////////////////////////////////////////////////////////////////////////////

template <class TAst>
void Serialize(const TAst& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    consumer->OnStringScalar(DB::serializeAST(ast));
}

template <class TAst>
void Serialize(const TAst* ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(DB::serializeAST(*ast));
    } else {
        consumer->OnEntity();
    }
}

template <class TAst>
void Serialize(const std::shared_ptr<TAst>& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAst*, DB::IAST*>::value>*)
{
    if (ast) {
        consumer->OnStringScalar(DB::serializeAST(*ast));
    } else {
        consumer->OnEntity();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
