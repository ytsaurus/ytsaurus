#include "../misc/stdafx.h"
#include "io.h"
#include "io_channels.h"
#include "chunk_writer.h"
#include "chunk_reader.h"
#include "user_indexes.h"
#include "chunk.pb.h"

#include "../misc/common.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace ::google::protobuf;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("IO");

////////////////////////////////////////////////////////////////////////////////

void MessageFromChannel(const TChannel& channel, TMsgChannel& msg)
{
    for (size_t a = 0; a < channel.Columns.size(); ++a) {
        const TValue& column = channel.Columns[a];
        msg.AddColumns(column.GetData(), column.GetSize());
    }

    for (size_t r = 0; r < channel.Ranges.size(); ++r) {
        const TRange& range = channel.Ranges[r];
        const TValue& begin = range.Begin();
        const TValue& end = range.End();

        TMsgRange* msgRange = msg.AddRanges();
        msgRange->SetBegin(begin.GetData(), begin.GetSize());
        msgRange->SetEnd(end.GetData(), end.GetSize());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TRead::TReadImpl
{
    TTransaction::TTransactionImpl* Tx;
    Stroka Table;

    TChunkId ChunkId;
    i64 ChunkSize;
    Stroka Node;

    TChannel Channel;

    bool Started;
    bool Finished;

    // Must be the same size
    TUserIndexes UserIndexes;
    yvector<TValue> Values;

    size_t ColumnIdx;
    size_t RowIdx;

    TChunkReader::TPtr ChunkReader;

private:
    void ReadRow()
    {
        bool hasRow = ChunkReader->NextRow(); 
        if (!hasRow && !Finished) {
            Finish();
            return;
        }

        Values.clear();
        Values.yresize(UserIndexes.GetSize());

        while (ChunkReader->NextColumn()) {
            size_t idx = ChunkReader->GetIndex();
            Values[idx] = ChunkReader->GetValue();
        }
        
        ColumnIdx = 0;
        while (ColumnIdx < Values.size() && Values[ColumnIdx].IsNull())
            ++ColumnIdx;
    }

public:

    TReadImpl()
        : Tx(NULL)
        , Started(false)
        , Finished(false)
    {
        Channel(RayRange());
    }

    TReadImpl(TTransaction::TTransactionImpl* tx)
        : Tx(tx)
        , Started(false)
        , Finished(false)
    {
        Channel(RayRange());
    }

    TReadImpl(TTransaction::TTransactionImpl* tx, const char* table)
        : Tx(tx)
        , Table(table)
        , Started(false)
        , Finished(false)
    {
        Channel(RayRange());
    }

    TReadImpl(TChunkId id, i64 chunkSize, Stroka node)
        : ChunkId(id)
        , ChunkSize(chunkSize)
        , Node(node)
        , Started(false)
        , Finished(false)
    {
        Channel(RayRange());
    }

    ~TReadImpl()
    {
        if (Started && !Finished)
            Finish();
    }

    void SetTransaction(TTransaction::TTransactionImpl* tx)
    {
        YASSERT(!Started);
        Tx = tx;
    }

    void SetTable(const char* table)
    {
        YASSERT(!Started);
        Table = table;
    }

    void SetChannel(const TChannel& channel)
    {
        YASSERT(!Started);
        Channel = channel;
    }

    void Start()
    {
        ChunkReader = new TChunkReader(ChunkId, Channel, 
            ChunkSize, Node, UserIndexes);
        ReadRow();
        Started = true;
    }

    void Finish()
    {
        Finished = true;
    }

    void NextRow() 
    {
        if (!Started)
            Start();

        ReadRow();
    }

    bool End()
    {
        if (!Started)
            Start();

        return Finished;
    }
    
    void NextColumn() 
    {
        if (!Started)
            Start();

        if (ColumnIdx < Values.size()) {
            do {
                ++ColumnIdx;
            } while (ColumnIdx < Values.size() && Values[ColumnIdx].IsNull());
        }
    }

    bool EndColumn()
    {
        if (!Started)
            Start();

        return ColumnIdx == Values.size();
    }
    
    TValue GetColumn()
    {
        if (!Started)
            Start();

        return UserIndexes[ColumnIdx];
    }

    TValue GetValue()
    {
        if (!Started)
            Start();

        return Values[ColumnIdx];
    }

    TValue Get(const TValue& column)
    {
        return Get(ColumnIndex(column));
    }

    size_t ColumnIndex(const TValue& column)
    {
        if (!Started)
            Start();

        return UserIndexes[column];
    }

    size_t GetIndex()
    {
        if (!Started)
            Start();

        return ColumnIdx;
    }

    TValue Get(size_t idx)
    {
        if (!Started)
            Start();

        return Values[idx];
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWrite::TWriteImpl
{
    TTransaction::TTransactionImpl* Tx;
    Stroka Table;

    yvector<TChannel> Channels;
    yvector<TWriteChannel> Writes;

    bool Started;
    bool Finished;

    TMsgChunkHeader ChunkHeader;

    // ToDo: Create ChunkId here or get in constructor
    TChunkId ChunkId;

#ifdef FILE_IO
    FILE* Output;
#else
    yvector<Stroka> Nodes;
    TIntrusivePtr<IChunkWriter> ChunkWriter;
#endif

    TUserIndexes UserIndexes;
    yvector< yvector<size_t> > ColumnToChannels;

    size_t RowCount;

    size_t SeqIndex;
    yhash_set<size_t> SetColumns;

    ui64 TotalSize;

    void WriteBlock(size_t writeIdx)
    {
        TWriteChannel& write = Writes[writeIdx];

        TBlob data;
        write.GetBlock(&data, UserIndexes);
        ui32 size = data.size();
#ifdef FILE_IO
        fwrite(data.begin(), size, 1, Output);
#else
        ChunkWriter->AddBlock(&data);
#endif

        ui64 offset = TotalSize;
        TotalSize += size;

        TMsgChunkChannel* msgChunkChannel =
            ChunkHeader.MutableChannels(writeIdx);
        TMsgBlock* msgBlock = msgChunkChannel->AddBlocks();

        msgBlock->SetOffset(offset);
        msgBlock->SetSize(size);
        msgBlock->SetLastRowIndex(RowCount);

        LOG_DEBUG("write block: channel %"PRIu64", offset %"PRIu64", size %u",
            writeIdx, offset, size);
    }

    void WriteChunkHeader()
    {
        ChunkHeader.SetVersion(1);
        ChunkHeader.SetRowCount(RowCount);
                
        ui32 size = ChunkHeader.ByteSize();
        TBlob data;
        data.yresize(size);

        io::ArrayOutputStream out(data.begin(), size);   
        if (!ChunkHeader.SerializeToZeroCopyStream(&out))
            LOG_DEBUG("cannot write chunk header");

        TFixedChunkFooter fixed;
        fixed.HeaderOffset = TotalSize;
        fixed.HeaderSize = size;

#ifdef FILE_IO
        fwrite(data.begin(), size, 1, Output);
        fwrite(&fixed, sizeof(TFixedChunkFooter), 1, Output);
#else
        ChunkWriter->AddBlock(&data);
        data.assign((ui8*)&fixed, (ui8*)&fixed + sizeof(TFixedChunkFooter));
        ChunkWriter->AddBlock(&data);
#endif
        TotalSize += size + sizeof(TFixedChunkFooter);
        LOG_DEBUG("write chunk header");
    }

public:

    TWriteImpl()
        : Tx(NULL)
        , Started(false)
        , Finished(false)
    {}

    TWriteImpl(TTransaction::TTransactionImpl* tx)
        : Tx(tx)
        , Started(false)
        , Finished(false)
    {}

    TWriteImpl(TTransaction::TTransactionImpl* tx, const char* table)
        : Tx(tx)
        , Table(table)
        , Started(false)
        , Finished(false)
    {}

    ~TWriteImpl()
    {
        if (Started && !Finished)
            Finish();
    }

    void SetTransaction(TTransaction::TTransactionImpl* tx)
    {
        YASSERT(!Started);
        Tx = tx;
    }

    void SetTable(const char* table)
    {
        YASSERT(!Started);
        Table = table;
    }

    void AddChannel(const TChannel& channel)
    {
        YASSERT(!Started);
        Channels.push_back(channel);
    }

    void AddNode(const Stroka& node)
    {
#ifndef FILE_IO
        Nodes.push_back(node);
#endif
    }

    TChunkId GetChunkId()
    {
        return ChunkId;

    }

    ui64 GetChunkSize()
    {
        return TotalSize;
    }

    void Start()
    {
#ifdef FILE_IO
        Output = fopen(Table.c_str(), "wb");
#else
        yvector<Stroka> nodes;
    /*    TChunkWriterConfig config;
        config.WinSize = 48;
        config.GroupSize = 8 << 20;
        config.MinRepFactor = 1;
        ChunkWriter = new TChunkWriter(config, Nodes);*/
    // ToDo: Get ChunkWriter in constructor
#endif

        // add misc channel
        TChannel miscChannel;
        miscChannel( TRange(TValue(), TValue()) );
        Channels.push_back(miscChannel);

        // initialize channels
        Writes.resize(Channels.size());
        for (size_t c = 0; c < Channels.size(); ++c) {            
            TMsgChunkChannel* msgChunkChannel = ChunkHeader.AddChannels();
            TMsgChannel* msgChannel = msgChunkChannel->MutableChannel();

            const TChannel& channel = Channels[c];
            MessageFromChannel(channel, *msgChannel);

            TWriteChannel& write = Writes[c];
            write.SetChannel(channel);
            write.SetBlockSizeLimit(1 << 20);
            write.StartBlock();
        }

        RowCount = 0;
        TotalSize = 0;

        Started = true;
    }

    void Finish()
    {
        for (size_t w = 0; w < Writes.size(); ++w)
            WriteBlock(w);
        WriteChunkHeader();
#ifdef FILE_IO
        fclose(Output);
#else
        ChunkWriter->Close();
#endif
        Finished = true;
    }

    size_t ColumnIndex(const TValue& column)
    {
        if (!Started)
            Start();

        size_t size = UserIndexes.GetSize();
        size_t idx = UserIndexes[column];

        if (idx == size) {
            ColumnToChannels.push_back();
            yvector<size_t>& channelIndexes = ColumnToChannels[idx];
            size_t miscIdx = Channels.size() - 1;

            bool inMisc = true;
            for (size_t i = 0; i < miscIdx; ++i) {
                if (Channels[i].Match(column)) {
                    channelIndexes.push_back(i);
                    Writes[i].SetColumnIndex(idx, column);
                    inMisc = false;
                }
            }

            if (inMisc) {
                channelIndexes.push_back(miscIdx);
                Writes[miscIdx].SetColumnIndex(idx, column);
            }
        }

        return idx;
    }

    void Set(size_t userIdx, const TValue& value)
    {
        if (!Started)
            Start();

        if (SetColumns.find(userIdx) != SetColumns.end())
            return;

        SetColumns.insert(userIdx);

        const yvector<size_t>& channelIndices = ColumnToChannels[userIdx];
        for (size_t i = 0; i < channelIndices.size(); ++i)
            Writes[ channelIndices[i] ].Set(userIdx, value);
    }

    void Set(const TValue& column, const TValue& value)
    {
        Set(ColumnIndex(column), value);
    }

    void Set(const TValue& value)
    {
        Set(SeqIndex++, value);
    }

    void AddRow()
    {
        if (!Started)
            Start();

        for (size_t w = 0; w < Writes.size(); ++w) {
            TWriteChannel& write = Writes[w];
            write.AddRow();
            if (write.HasBlock()) {
                WriteBlock(w);
                write.StartBlock();
            }
        }

        SeqIndex = 0;
        SetColumns.clear();
        ++RowCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

TRead::TRead()
    : Impl(new TRead::TReadImpl())
{}

TRead::TRead(TTransaction& tx)
    : Impl(new TRead::TReadImpl(tx.GetImpl()))
{}

TRead::TRead(TTransaction& tx, const char* table)
    : Impl(new TRead::TReadImpl(tx.GetImpl(), table))
{}

TRead::TRead(TChunkId id, ui64 chunkSize, Stroka node)
    : Impl(new TRead::TReadImpl(id, chunkSize, node))
{}

TRead::~TRead()
{}

void TRead::SetTransaction(TTransaction& tx)
{
    Impl->SetTransaction(tx.GetImpl());
}

void TRead::SetTable(const char* table)
{
    Impl->SetTable(table);
}

void TRead::SetChannel(const TChannel& channel)
{
    Impl->SetChannel(channel);
}

TValue TRead::operator[](const TValue& column)
{
    return Impl->Get(column);
}

void TRead::NextRow()
{
    Impl->NextRow();
}

bool TRead::End()
{
    return Impl->End();
}

void TRead::NextColumn()
{
    Impl->NextColumn();
}

bool TRead::EndColumn()
{
    return Impl->EndColumn();
}

TValue TRead::GetColumn()
{
    return Impl->GetColumn();
}

TValue TRead::GetValue()
{
    return Impl->GetValue();
}

size_t TRead::ColumnIndex(const TValue& column)
{
    return Impl->ColumnIndex(column);
}

TValue TRead::operator[](size_t userIdx)
{
    return Impl->Get(userIdx);
}

size_t TRead::GetIndex()
{
    return Impl->GetIndex();
}

////////////////////////////////////////////////////////////////////////////////

TWrite::TWrite()
    : Impl(new TWrite::TWriteImpl())
{}

TWrite::TWrite(TTransaction& tx)
    : Impl(new TWrite::TWriteImpl(tx.GetImpl()))
{}

TWrite::TWrite(TTransaction& tx, const char* table)
    : Impl(new TWrite::TWriteImpl(tx.GetImpl(), table))
{}

TWrite::~TWrite()
{}

void TWrite::SetTransaction(TTransaction& tx)
{
    Impl->SetTransaction(tx.GetImpl());
}

void TWrite::SetTable(const char* table)
{
    Impl->SetTable(table);
}

void TWrite::AddChannel(const TChannel& channel)
{
    Impl->AddChannel(channel);
}

void TWrite::AddNode(const Stroka& node)
{
    Impl->AddNode(node);
}

TWrite& TWrite::operator()(const TValue& column, const TValue& value)
{
    Impl->Set(column, value);
    return *this;
}

void TWrite::AddRow()
{
    Impl->AddRow();
}

TChunkId TWrite::GetChunkId()
{
    return Impl->GetChunkId();
}

ui64 TWrite::GetChunkSize()
{
    return Impl->GetChunkSize();
}

void TWrite::Finish()
{
    Impl->Finish();
}

size_t TWrite::ColumnIndex(const TValue& column)
{
    return Impl->ColumnIndex(column);
}

TWrite& TWrite::operator()(size_t userIdx, const TValue& value)
{
    Impl->Set(userIdx, value);
    return *this;
}

TWrite& TWrite::operator()(const TValue& value)
{
    Impl->Set(value);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

}
