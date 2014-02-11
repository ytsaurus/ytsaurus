#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "versioned_writer.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/data_statistics.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkWriter
    : public IVersionedWriter
{
    // Required by TMultiChunkSequenceWriter.
    virtual IVersionedWriter* GetFacade() = 0;

    virtual i64 GetMetaSize() const = 0;
    virtual i64 GetDataSize() const = 0;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const = 0;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const = 0;

    // Required by provider.
    virtual NProto::TBoundaryKeysExt GetBoundaryKeys() const = 0;
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IVersionedChunkWriter)

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkWriterProvider
    : public virtual TRefCounted
{
    DEFINE_BYREF_RO_PROPERTY(NProto::TBoundaryKeysExt, BoundaryKeysExt);

public:
    typedef IVersionedChunkWriter TChunkWriter;
    typedef IVersionedWriter TFacade;

    TVersionedChunkWriterProvider(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns);

    IVersionedChunkWriterPtr CreateChunkWriter(NChunkClient::IAsyncWriterPtr asyncWriter);
    void OnChunkFinished();
    void OnChunkClosed(IVersionedChunkWriterPtr writer);

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const;

private:
    TChunkWriterConfigPtr Config_;
    TChunkWriterOptionsPtr Options_;

    const TTableSchema Schema_;
    const TKeyColumns KeyColumns_;

    IVersionedChunkWriterPtr CurrentWriter_;

    int CreatedWriterCount_;
    int FinishedWriterCount_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;

    TSpinLock SpinLock_;

    yhash_set<IVersionedChunkWriterPtr> ActiveWriters_;

};

DEFINE_REFCOUNTED_TYPE(TVersionedChunkWriterProvider)

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    NChunkClient::IAsyncWriterPtr asyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
