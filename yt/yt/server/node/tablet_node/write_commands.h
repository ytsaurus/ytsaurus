#pragma once

#include "private.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TWireWriteCommands ParseWriteCommands(
    const NTableClient::TSchemaData& schemaData,
    NTableClient::IWireProtocolReader* reader,
    bool isVersionedWriteUnversioned);

////////////////////////////////////////////////////////////////////////////////

class TWireWriteCommandsBatch
{
public:
    DEFINE_BYREF_RO_PROPERTY(TWireWriteCommands, Commands);
    DEFINE_BYREF_RO_PROPERTY(TSharedRef, Data);

    TWireWriteCommandsBatch() = default;
    TWireWriteCommandsBatch(
        TWireWriteCommands commands,
        NTableClient::TRowBufferPtr rowBuffer,
        TSharedRef data);

private:
    friend struct TTransactionWriteRecord;

    NTableClient::TRowBufferPtr RowBuffer_;
};

struct IWireWriteCommandsReader
{
    virtual const TWireWriteCommand& NextCommand(bool isVersionedWriteUnversioned = false) = 0;
    virtual bool IsFinished() const = 0;
    virtual void RollbackLastCommand() = 0;
};

class TWireWriteCommandsBatchingReader
    : public IWireWriteCommandsReader
{
public:
    TWireWriteCommandsBatchingReader(
        TSharedRef data,
        std::unique_ptr<NTableClient::IWireProtocolReader> reader,
        NTableClient::TSchemaData schemaData);

    const TWireWriteCommand& NextCommand(bool isVersionedWriteUnversioned = false) final;
    bool IsFinished() const final;
    void RollbackLastCommand() final;

    TWireWriteCommandsBatch FinishBatch();
    bool IsBatchEmpty() const;

private:
    const TSharedRef Data_;
    const NTableClient::TSchemaData SchemaData_;
    const std::unique_ptr<NTableClient::IWireProtocolReader> Reader_;

    NTableClient::IWireProtocolReader::TIterator CurrentBatchStartingPosition_;
    TWireWriteCommands CurrentBatch_;
    std::optional<NTableClient::IWireProtocolReader::TIterator> LastCommandPosition_;
};

class TWireWriteCommandsReaderAdapter
    : public IWireWriteCommandsReader
{
public:
    explicit TWireWriteCommandsReaderAdapter(const TWireWriteCommands& commands);

    const TWireWriteCommand& NextCommand(bool isVersionedWriteUnversioned = false) final;
    bool IsFinished() const final;
    void RollbackLastCommand() final;

private:
    const TWireWriteCommands& Commands_;

    size_t CurrentIndex_ = 0;
};

} // namespace NYT::NTabletNode
