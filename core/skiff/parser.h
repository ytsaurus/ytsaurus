#pragma once

#include "skiff_schema.h"

#include <yt/core/concurrency/coroutine.h>

#include <util/generic/buffer.h>

namespace NYT {
namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
class TSkiffMultiTableParser
{
public:
    TSkiffMultiTableParser(
        TConsumer* consumer,
        TSkiffSchemaList schemaList,
        const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName);

    ~TSkiffMultiTableParser();

    void Read(TStringBuf data);
    void Finish();

private:
    using TParserCoroutine = NConcurrency::TCoroutine<void(TStringBuf)>;

    TParserCoroutine ParserCoroutine_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT

#define PARSER_INL_H_
#include "parser-inl.h"
#undef PARSER_INL_H_
