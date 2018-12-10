#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/compression/public.h>

#include <yt/core/erasure/public.h>

#include <yt/client/table_client/public.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/transaction_client/public.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

//! YPath string plus attributes.
class TRichYPath
{
public:
    TRichYPath();
    TRichYPath(const TRichYPath& other);
    TRichYPath(TRichYPath&& other);
    TRichYPath(const char* path);
    TRichYPath(const TYPath& path);
    TRichYPath(const TYPath& path, const NYTree::IAttributeDictionary& attributes);
    TRichYPath& operator = (const TRichYPath& other);

    static TRichYPath Parse(const TString& str);
    TRichYPath Normalize() const;

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    // Attribute accessors.
    // "append"
    bool GetAppend() const;
    void SetAppend(bool value);

    // "teleport"
    bool GetTeleport() const;

    // "primary"
    bool GetPrimary() const;

    // "foreign"
    bool GetForeign() const;
    void SetForeign(bool value);

    // "columns"
    std::optional<std::vector<TString>> GetColumns() const;
    void SetColumns(const std::vector<TString>& columns);

    // "rename_columns"
    std::optional<NTableClient::TColumnRenameDescriptors> GetColumnRenameDescriptors() const;

    // "ranges"
    // COMPAT(ignat): also "lower_limit" and "upper_limit"
    std::vector<NChunkClient::TReadRange> GetRanges() const;
    void SetRanges(const std::vector<NChunkClient::TReadRange>& value);
    bool HasNontrivialRanges() const;

    // "file_name"
    std::optional<TString> GetFileName() const;

    // "executable"
    std::optional<bool> GetExecutable() const;

    // "format"
    NYson::TYsonString GetFormat() const;

    // "schema"
    std::optional<NTableClient::TTableSchema> GetSchema() const;

    // "sorted_by"
    NTableClient::TKeyColumns GetSortedBy() const;
    void SetSortedBy(const NTableClient::TKeyColumns& value);

    // "row_count_limit"
    std::optional<i64> GetRowCountLimit() const;

    // "timestamp"
    std::optional<NTransactionClient::TTimestamp> GetTimestamp() const;

    // "optimize_for"
    std::optional<NTableClient::EOptimizeFor> GetOptimizeFor() const;

    // "compression_codec"
    std::optional<NCompression::ECodec> GetCompressionCodec() const;

    // "erasure_codec"
    std::optional<NErasure::ECodec> GetErasureCodec() const;

    // "auto_merge"
    bool GetAutoMerge() const;

    // "transaction_id"
    std::optional<NObjectClient::TTransactionId> GetTransactionId() const;

private:
    TYPath Path_;
    std::unique_ptr<NYTree::IAttributeDictionary> Attributes_;
};

bool operator== (const TRichYPath& lhs, const TRichYPath& rhs);

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRichYPath& path);

std::vector<TRichYPath> Normalize(const std::vector<TRichYPath>& paths);

void Serialize(const TRichYPath& richPath, NYson::IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
