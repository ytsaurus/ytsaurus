


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

};


////////////////////////////////////////////////////////////////////////////////

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
    auto lowerLimit = TReadLimit(chunkSpec.lower_limit());
    auto upperLimit = TReadLimit(chunkSpec.upper_limit());

    if (chunkSpec.partition_tag() != DefaultPartitionTag) {
        // This is partition read.
        YCHECK(lowerLimit.IsTrivial());
        YCHECK(upperLimit.IsTrivial());
        YCHECK(chunkSpec.chunk_meta().version() == ETableChunkVersion::SchemalessHorizontal);
        // return CreatePartitionSchemalessReader();
    }

    auto columnFilter = CreateColumnFilter(chunkSpec.channels(), nameTable);
    bool readSorted = lowerLimit.HasKey() || upperLimit.HasKey() || keyColumns;

    if (readSorted) {
        // CreateSortedSchemalessReader();
        // All key columns should be inside column filter.
    } else {
        // CreateSchemalessReader();
    }
}

/*
IPartitionChunkReaderPtr CreatePartitionChunkReader()
{
    // How does it look like.
}
*/

