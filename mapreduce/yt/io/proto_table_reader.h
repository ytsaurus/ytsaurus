#pragma once

#include "lenval_table_reader.h"

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyInput;
class TNodeTableReader;

////////////////////////////////////////////////////////////////////////////////

class TProtoTableReader
    : public IProtoReaderImpl
{
public:
    explicit TProtoTableReader(
        ::TIntrusivePtr<TProxyInput> input,
        yvector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TProtoTableReader() override;

    void ReadRow(Message* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    THolder<TNodeTableReader> NodeReader_;
    yvector<const ::google::protobuf::Descriptor*> Descriptors_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableReader
    : public IProtoReaderImpl
    , public TLenvalTableReader
{
public:
    explicit TLenvalProtoTableReader(
        ::TIntrusivePtr<TProxyInput> input,
        yvector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TLenvalProtoTableReader() override;

    void ReadRow(Message* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

protected:
    void SkipRow() override;

private:
    yvector<const ::google::protobuf::Descriptor*> Descriptors_;
};

// Sometime useful outside mapreduce/yt
void ReadMessageFromNode(const TNode& node, Message* row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
