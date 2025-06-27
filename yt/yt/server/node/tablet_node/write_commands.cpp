#include "write_commands.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TWireWriteCommands ParseWriteCommands(
    const NTableClient::TSchemaData& schemaData,
    NTableClient::IWireProtocolReader* reader,
    bool isVersionedWriteUnversioned)
{
    TWireWriteCommands writeCommands;

    while (!reader->IsFinished()) {
        writeCommands.push_back(reader->ReadWriteCommand(
            schemaData,
            /*captureValues*/ false,
            isVersionedWriteUnversioned));
    }

    return writeCommands;
}

////////////////////////////////////////////////////////////////////////////////

TWireWriteCommandsBatch::TWireWriteCommandsBatch(
    TWireWriteCommands commands,
    NTableClient::TRowBufferPtr rowBuffer,
    TSharedRef data)
    : Commands_(std::move(commands))
    , Data_(std::move(data))
    , RowBuffer_(std::move(rowBuffer))
{ }

////////////////////////////////////////////////////////////////////////////////

TWireWriteCommandsBatchingReader::TWireWriteCommandsBatchingReader(
    TSharedRef data,
    std::unique_ptr<NTableClient::IWireProtocolReader> reader,
    NTableClient::TSchemaData schemaData)
    : Data_(std::move(data))
    , SchemaData_(std::move(schemaData))
    , Reader_(std::move(reader))
{
    CurrentBatchStartingPosition_ = Reader_->GetCurrent();
}

const TWireWriteCommand& TWireWriteCommandsBatchingReader::NextCommand(bool isVersionedWriteUnversioned)
{
    YT_ASSERT(!IsFinished());
    LastCommandPosition_ = Reader_->GetCurrent();
    CurrentBatch_.push_back(Reader_->ReadWriteCommand(SchemaData_, /*captureValues*/ false, isVersionedWriteUnversioned));
    return CurrentBatch_.back();
}

bool TWireWriteCommandsBatchingReader::IsFinished() const
{
    return Reader_->IsFinished();
}

void TWireWriteCommandsBatchingReader::RollbackLastCommand()
{
    YT_VERIFY(!CurrentBatch_.empty());
    YT_VERIFY(LastCommandPosition_.has_value());
    CurrentBatch_.pop_back();
    Reader_->SetCurrent(*LastCommandPosition_);
    LastCommandPosition_.reset();
}

TWireWriteCommandsBatch TWireWriteCommandsBatchingReader::FinishBatch()
{
    YT_VERIFY(!IsBatchEmpty());
    auto batchData = Reader_->Slice(CurrentBatchStartingPosition_, Reader_->GetCurrent());
    CurrentBatchStartingPosition_ = Reader_->GetCurrent();
    return TWireWriteCommandsBatch(
        std::move(CurrentBatch_),
        Reader_->GetRowBuffer(),
        std::move(batchData));
}

bool TWireWriteCommandsBatchingReader::IsBatchEmpty() const
{
    return Reader_->GetCurrent() == CurrentBatchStartingPosition_;
}

////////////////////////////////////////////////////////////////////////////////

TWireWriteCommandsReaderAdapter::TWireWriteCommandsReaderAdapter(const TWireWriteCommands& commands)
    : Commands_(commands)
{ }

const TWireWriteCommand& TWireWriteCommandsReaderAdapter::NextCommand(bool /*isVersionedWriteUnversioned*/)
{
    return Commands_[CurrentIndex_++];
}

bool TWireWriteCommandsReaderAdapter::IsFinished() const
{
    return CurrentIndex_ == Commands_.size();
}

void TWireWriteCommandsReaderAdapter::RollbackLastCommand()
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
