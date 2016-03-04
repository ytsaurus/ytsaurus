#pragma once

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
struct TIOOptionsTraits;

template <>
struct TIOOptionsTraits<TFileReaderOptions>
{
    static constexpr const char* const ConfigName = "file_reader";
};
template <>
struct TIOOptionsTraits<TFileWriterOptions>
{
    static constexpr const char* const ConfigName = "file_writer";
};
template <>
struct TIOOptionsTraits<TTableReaderOptions>
{
    static constexpr const char* const ConfigName = "table_reader";
};
template <>
struct TIOOptionsTraits<TTableWriterOptions>
{
    static constexpr const char* const ConfigName = "table_writer";
};

template <class TOptions>
Stroka FormIORequestParameters(
    const TRichYPath& path,
    const TOptions& options)
{
    if (!options.Config_) {
        return YPathToYsonString(path);
    }

    auto params = NodeFromYsonString(YPathToYsonString(path));
    params[TIOOptionsTraits<TOptions>::ConfigName] = *options.Config_;
    return NodeToYsonString(params);
}

////////////////////////////////////////////////////////////////////////////////

}
