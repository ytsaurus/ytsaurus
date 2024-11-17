#pragma once

/// @file yt.h
///
/// Main header for YT executor for the Roren library.
///
/// Contains functions to create YT pipeline and transforms to read/write YT tables.

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/yt/proto/config.pb.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include "transforms.h"
#include "yt_io_private.h"
#include "yt_proto_io.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Create pipeline that executes on YT.
///
/// @param cluster is YT cluster to run pipeline.
/// @param workingDir is a directory for storing temporary data.
///    `//tmp` could be used, though it is not recommended for production processes
///    ([documentation](https://yt.yandex-team.ru/docs/user-guide/best-practice/howtorunproduction#zakazhite-neobhodimye-resursy))

TPipeline MakeYtPipeline(const TString& cluster, const TString& workingDir);

///
/// @brief Create pipeline that executes on YT.
///
/// Similar to @ref NRoren::MakeYtPipeline(const TString&, const TString&) but provides additional options.
///
/// @param config proto config for running YT pipeline with additional options
TPipeline MakeYtPipeline(TYtPipelineConfig config);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Read YT table
///
/// @param path path to YT table, all RichYPath attributes that can be used on operations (i.e. columns, row ranges)
///     can be used here
template <typename T>
TReadTransform<T> YtRead(NYT::TRichYPath path)
{
    NPrivate::IRawYtReadPtr inner;
    if constexpr (std::is_same_v<T, NYT::TNode>) {
        inner = NPrivate::MakeYtNodeInput(std::move(path));
    } else if constexpr (std::derived_from<T, ::google::protobuf::Message>) {
        inner = NPrivate::MakeYtProtoRead<T>(std::move(path));
    } else {
        static_assert(TDependentFalse<T>, "Only NYT::TNode or protobuf messages can be used as elements");
    }
    return TReadTransform<T>{inner};
}

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Write YT table
///
/// @param path path to YT table, all RichYPath attributes can can be used on operation output tables can be used here, except schema
///
/// @param schema schema of output table
TYtWriteTransform YtWrite(const NYT::TRichYPath& path, const NYT::TTableSchema& schema);

///
/// @brief Write YT table
///
/// Table schema is derived automatically from input type. Input type must be a protobuf message.
TYtAutoSchemaWriteTransform YtWrite(const NYT::TRichYPath& path);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Write sorted YT table
///
/// PCollection will be saved to specified table and sort operation invoked if required.
TYtSortedWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& schema,
    const NYT::TSortColumns& columnsToSort);

///
/// @brief Write sorted YT table
///
/// Similar to @ref NRoren::YtSortedWrite(const NYT::TRichYPath&, const NYT::TTableSchema&, const NYT::TSortColumns&)
/// but columns to sort are derived from table schema.
TYtSortedWriteTransform YtSortedWrite(
    const NYT::TRichYPath& path,
    const NYT::TTableSchema& sortedSchema);

///
/// @brief Write sorted YT table
///
/// Schema is derived automatically from input type. Input type must be a protobuf message.
TYtAutoSchemaSortedWriteTransform YtSortedWrite(const NYT::TRichYPath& path, const NYT::TSortColumns& sortColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
