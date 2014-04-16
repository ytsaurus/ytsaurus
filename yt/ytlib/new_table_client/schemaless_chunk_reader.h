


/*
struct IPartitionChunkReader
{
    bool Read(std::vector<TUnversionedValue>* keyValues, std::vector<void*>* rowPointers);
};
*/

struct ISchemalessChunkReader
    : public virtual NChunkClient::IChunkReaderBase
    , public ISchemalessReader
{ };

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReader
    : public ISchemalessChunkReader
    , public TChunkReaderBase
{
public:
    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

private:

    TNameTablePtr NameTable_;
    TNameTablePtr ChunkNameTable_;

    TColumnFilter ColumnFilter_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    std::vector<std::unique_ptr<THorizontalSchemalessBlockReader>> BlockReader_;

    int CurrentBlockIndex_;
    i64 CurrentRowIndex_;
    i64 RowCount_;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

};


////////////////////////////////////////////////////////////////////////////////

TSchemalessChunkReader::TSchemalessChunkReader()
{

}

void TSchemalessChunkReader::DownloadChunkMeta(std::vector<int> extensionTags, int partitionTag)
{
    extensionTags.push_back(ProtoExtensionTag<TBlockMetaExt>::Value);
    extensionTags.push_back(ProtoExtensionTag<TNameTableExt>::Value);

    auto errorOrMeta = WaitFor(UnderlyingReader_->GetChunkMeta(extensionTags));
    THROW_ERROR_EXCEPTION_IF_FAILED(errorOrMeta);

    ChunkMeta_ = errorOrMeta.Value();
    BlockMetaExt_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());
    // ChunkNameTable from proto.


}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequence() 
{
    // Create mapping btw old name table and new name table using index
    bool readSorted = LowerLimit_.HasKey() || UpperLimit_.HasKey() || KeyColumns_;
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceSorted() 
{
    THROW_ERROR_EXCEPTION_IF(!Misc_.sorted(), "Requested sorted read for unsorted chunk");

    std::vector<int> extensionTags = {
        ProtoExtensionTag<TBlockIndexExt>::Value,
        ProtoExtensionTag<TKeyColumnsExt>::Value,
    };

    DownloadChunkMeta(extensionTags);

    // ToDo(psushin): validate key columns if any.

    auto blockIndexExt = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());

    int beginIndex = std::max(GetBeginBlockIndex(BlockMetaExt_), GetBeginBlockIndex(blockIndexExt_));
    int endIndex = std::min(GetEndBlockIndex(BlockMetaExt_), GetEndBlockIndex(blockIndexExt_));

    return CreateBlockSequence(beginIndex, endIndex);
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequencePartition() 
{


}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceBasic() 
{
    // Create mapping btw old name table and new name table using index

}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    CurrentBlockIndex_ = beginIndex;
    auto& blockMeta = BlockMetaExt_.entries(CurrentBlockIndex_);

    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (int index = CurrentBlockIndex_; index < endIndex; ++index) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = index;
        blockInfo.Size = BlockMetaExt_.entries(index).block_size();
        blocks.push_back(blockInfo);
    }
    return blocks;
}

void TSchemalessChunkReader::InitFirstBlock()
{
    InitFirstBlock();

    if (LowerLimit_.HasRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TSchemalessChunkReader::InitNextBlock()
{
    BlockReader_ = new THorizontalSchemalessBlockReader(SequentialReader->GetBlock(), KeyColumns_.size());
}

bool TSchemalessChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        return OnBlockEnded();
    }

    while (rows->size() < rows->capacity()) {
        ++CurrentRowIndex_;
        if (UpperLimit_.HasRowIndex() && CurrentRowIndex_ == UpperLimit_.GetRowIndex()) {
            return false;
        }

        if (UpperLimit_.HasKey() && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
            return false;
        }

        ++RowCount_;
        rows->push_back(BlockReader_->GetRow(&MemoryPool_));
        
        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }
}

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config, 
    TChunkReaderOptionsPtr options, 
    const TChunkSpec& chunkSpec, 
    IAsyncReaderPtr asyncReader,
    TNameTablePtr nameTable,
    TNullable<TKeyColumns> keyColumns)
{
    // Validate chunk type and version.

    auto lowerLimit = TReadLimit(chunkSpec.lower_limit());
    auto upperLimit = TReadLimit(chunkSpec.upper_limit());

    if (chunkSpec.partition_tag() != DefaultPartitionTag) {
        // This is partition read.
        YCHECK(lowerLimit.IsTrivial());
        YCHECK(upperLimit.IsTrivial());
        YCHECK(chunkSpec.chunk_meta().version() == ETableChunkVersion::SchemalessHorizontal);
        
    }

    auto columnFilter = CreateColumnFilter(chunkSpec.channels(), nameTable);
    // CreateSchemalessReader();

    /*
    bool readSorted = lowerLimit.HasKey() || upperLimit.HasKey() || keyColumns;

    if (readSorted) {
        // CreateSortedSchemalessReader();
        // All key columns should be inside column filter.
    } else {
        
    }
    */
}

/*
IPartitionChunkReaderPtr CreatePartitionChunkReader()
{
    // How does it look like.
}
*/

