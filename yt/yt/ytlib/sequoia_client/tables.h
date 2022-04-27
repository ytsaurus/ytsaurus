#pragma once

#include "public.h"

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ITableDescriptor
    : public TRefCounted
{
public:
    virtual ESequoiaTable GetType() const = 0;
    virtual const TString& GetName() const = 0;

    virtual const NTableClient::TTableSchemaPtr& GetSchema() const = 0;
    virtual const NTableClient::TNameTablePtr& GetNameTable() const = 0;
    virtual const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TChunkMetaExtensionsTableDescriptor
    : public ITableDescriptor
{
public:
    TChunkMetaExtensionsTableDescriptor();

    static const TChunkMetaExtensionsTableDescriptorPtr& Get();

    ESequoiaTable GetType() const override;
    const TString& GetName() const override;

    const NTableClient::TTableSchemaPtr& GetSchema() const override;
    const NTableClient::TNameTablePtr& GetNameTable() const override;
    const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const override;

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& nameTable);

        const int Id;
        const int MiscExt;
        const int HunkChunkRefsExt;
        const int HunkChunkMiscExt;
        const int BoundaryKeysExt;
        const int HeavyColumnStatisticsExt;
    };
    const TIndex& GetIndex() const;

    struct TChunkMetaExtensionsRow
    {
        using TTable = TChunkMetaExtensionsTableDescriptor;

        // Key.
        TString Id;

        // Values.
        TString MiscExt;
        TString HunkChunkRefsExt;
        TString HunkChunkMiscExt;
        TString BoundaryKeysExt;
        TString HeavyColumnStatisticsExt;
    };
    NTableClient::TLegacyKey ToKey(
        const TChunkMetaExtensionsRow& chunkMetaExtensions,
        const NTableClient::TRowBufferPtr& rowBuffer);
    NTableClient::TUnversionedRow ToUnversionedRow(
        const TChunkMetaExtensionsRow& chunkMetaExtensions,
        const NTableClient::TRowBufferPtr& rowBuffer);
    TChunkMetaExtensionsRow FromUnversionedRow(NTableClient::TUnversionedRow row);

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TNameTablePtr NameTable_;
    const NQueryClient::TColumnEvaluatorPtr ColumnEvaluator_;

    const TIndex Index_;

    static NTableClient::TTableSchemaPtr InitSchema();
};

DEFINE_REFCOUNTED_TYPE(TChunkMetaExtensionsTableDescriptor)

////////////////////////////////////////////////////////////////////////////////

ITableDescriptorPtr GetTableDescriptor(ESequoiaTable table);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
