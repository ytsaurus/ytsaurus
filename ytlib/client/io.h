#pragma once

#include "types.h"
#include "client.h"

#include "../holder/chunk.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRead
{
public:

    TRead();
    TRead(TTransaction& tx);
    TRead(TTransaction& tx, const char* table);

    TRead(TChunkId id, ui64 chunkSize, Stroka node);

    ~TRead();

    // setup
    void SetTransaction(TTransaction& tx);
    void SetTable(const char* table);
    void SetChannel(const TChannel& channel);

    // read by column name
    TValue operator[](const TValue& column);

    // iterating rows
    void NextRow();
    bool End();

    // iterating columns in a row
    void NextColumn();
    bool EndColumn();

    TValue GetColumn();
    TValue GetValue();

    // user column indices
    size_t ColumnIndex(const TValue& column);
    // read by index
    TValue operator[](size_t userIdx);
    // user index of GetColumn()
    size_t GetIndex();

private:

    class TReadImpl;
    THolder<TReadImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

class TWrite
{
public:

    TWrite();
    TWrite(TTransaction& tx);
    TWrite(TTransaction& tx, const char* table);
    ~TWrite();

    // setup
    void SetTransaction(TTransaction& tx);
    void SetTable(const char* table);
    void AddChannel(const TChannel& channel);
    void AddNode(const Stroka& node);

    TChunkId GetChunkId();
    ui64 GetChunkSize();

    // write by column name
    TWrite& operator()(const TValue& column, const TValue& value);

    // flush a row
    void AddRow();

    // flush everything and finish
    void Finish();

    // user column indices
    size_t ColumnIndex(const TValue& column);
    // write by index
    TWrite& operator()(size_t userIdx, const TValue& value);
    // write to a row sequentially
    TWrite& operator()(const TValue& value);

private:

    class TWriteImpl;
    THolder<TWriteImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

}
