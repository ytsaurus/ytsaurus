#pragma once

#include "skiff_schema.h"

#include <yt/core/concurrency/coroutine.h>

#include <util/generic/buffer.h>

namespace NYT::NSkiff {

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

    ui64 GetReadBytesCount();

private:
    using TParserCoroutine = NConcurrency::TCoroutine<void(TStringBuf)>;

    class TImpl;
    std::unique_ptr<TImpl> ParserImpl_;

    TParserCoroutine ParserCoroutine_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiff

#define PARSER_INL_H_
#include "parser-inl.h"
#undef PARSER_INL_H_
