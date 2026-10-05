#pragma once

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <library/cpp/skiff/skiff_schema.h>

#include <yt/yt/core/concurrency/coroutine.h>

#include <yt/yt/core/misc/coro_pipe.h>

#include <util/generic/buffer.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
class TSkiffMultiTableParser
{
public:
    TSkiffMultiTableParser(
        TConsumer* consumer,
        NSkiff::TSkiffSchemaList schemaList,
        const std::vector<NSkiffExt::TSkiffTableColumnIds>& tablesColumnIds,
        const std::string& rangeIndexColumnName,
        const std::string& rowIndexColumnName);

    ~TSkiffMultiTableParser();

    void Read(TStringBuf data);
    void Finish();

    ui64 GetReadBytesCount();

private:
    class TImpl;
    std::unique_ptr<TImpl> ParserImpl_;

    TCoroPipe ParserCoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

#define MULTI_TABLE_PARSER_INL_H_
#include "multi_table_parser-inl.h"
#undef MULTI_TABLE_PARSER_INL_H_
