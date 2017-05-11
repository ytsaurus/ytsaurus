#pragma once

#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/config.h>

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
TString FormIORequestParameters(
    const TRichYPath& path,
    const TOptions& options)
{
    auto params = NodeFromYPath(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TOptions>::ConfigName] = *options.Config_;
    }
    return NodeToYsonString(params);
}

template <>
inline TString FormIORequestParameters(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    auto params = NodeFromYPath(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TTableReaderOptions>::ConfigName] = *options.Config_;
    }
    if (options.Offset_) {
        params["offset"] = *options.Offset_;
    }
    if (options.Length_) {
        params["length"] = *options.Length_;
    }
    return NodeToYsonString(params);
}

template <>
inline TString FormIORequestParameters<TTableWriterOptions>(
    const TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto params = NodeFromYPath(path);
    auto tableWriter = TConfig::Get()->TableWriter;
    if (options.Config_) {
        MergeNodes(tableWriter, *options.Config_);
    }
    if (!tableWriter.Empty()) {
        params[TIOOptionsTraits<TTableWriterOptions>::ConfigName] = std::move(tableWriter);
    }
    return NodeToYsonString(params);
}

////////////////////////////////////////////////////////////////////////////////

}
