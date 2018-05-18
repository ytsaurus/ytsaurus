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
TNode FormIORequestParameters(
    const TRichYPath& path,
    const TOptions& options)
{
    auto params = PathToParamNode(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TOptions>::ConfigName] = *options.Config_;
    }
    return params;
}

template <>
inline TNode FormIORequestParameters(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    auto params = PathToParamNode(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TTableReaderOptions>::ConfigName] = *options.Config_;
    }
    if (options.Offset_) {
        params["offset"] = *options.Offset_;
    }
    if (options.Length_) {
        params["length"] = *options.Length_;
    }
    return params;
}

template <>
inline TNode FormIORequestParameters(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto params = PathToParamNode(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TFileWriterOptions>::ConfigName] = *options.Config_;
    }
    if (options.ComputeMD5_) {
        params["compute_md5"] = *options.ComputeMD5_;
    }
    return params;
}

template <>
inline TNode FormIORequestParameters<TTableWriterOptions>(
    const TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto params = PathToParamNode(path);
    auto tableWriter = TConfig::Get()->TableWriter;
    if (options.Config_) {
        MergeNodes(tableWriter, *options.Config_);
    }
    if (!tableWriter.Empty()) {
        params[TIOOptionsTraits<TTableWriterOptions>::ConfigName] = std::move(tableWriter);
    }
    return params;
}

////////////////////////////////////////////////////////////////////////////////

}
