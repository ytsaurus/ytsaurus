#pragma once

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterBase
    : public virtual TRefCounted
{
protected:
    TChunkWriterConfigPtr Config_;

    TEncodingChunkWriterPtr EncodingChunkWriter_;

    TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_;

    i64 RowCount_;

};

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkWriterBase
    : public TChunkWriterBase
{
public:


protected:
    TKeyColumns KeyColumns_;

    TBoundaryKeysExt BoundaryKeysExt_;

    TOwningKey LastKey_;

    TBlockIndexExt BlockIndexExt_;
    i64 BlockIndexExtSize_;

    TSamplesExt SamplesExt_;
    i64 SamplesExtSize_;
    double AverageSampleSize_;


};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
