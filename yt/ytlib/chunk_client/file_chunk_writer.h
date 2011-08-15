#pragma once

#include "chunk_writer.h"
#include "format.h"

#include "../misc/serialize.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
//! Provides a local and synchronous implementation of IChunkWriter.
class TFileChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TFileChunkWriter> TPtr;

    TFileChunkWriter(Stroka fileName)
    {
        File.Reset(new TFile(fileName, CreateAlways|WrOnly|Seq));
    }

    //! A synchronous version of #AsyncAddBlock.
    void AddBlock(const TSharedRef& data)
    {
        TBlockInfo blockInfo;
        blockInfo.Size = data.Size();
        blockInfo.Checksum = GetChecksum(data);
        BlockInfos.push_back(blockInfo);

        File->Write(data.Begin(), data.Size());
        File->Flush();
    }

    //! Implements IChunkWriter and calls #AddBlock.
    virtual bool AsyncAddBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready)
    {
        UNUSED(ready);
        AddBlock(data);
        return true;
    }

    //! A synchronous version of #Close.
    void Close()
    {
        WritePadding(*File, File->GetLength());

        TChunkFooter footer;
        footer.Singature = TChunkFooter::ExpectedSignature;
        footer.BlockInfoOffset = File->GetLength();
        footer.BlockCount = BlockInfos.ysize();

        TBlockInfo* infoBegin = &*BlockInfos.begin();
        TBlockInfo* infoEnd = &*BlockInfos.end();

        // Check alignment.
        YASSERT(
            static_cast<i64>(reinterpret_cast<char*>(infoEnd) - reinterpret_cast<char*>(infoBegin)) ==
            static_cast<i64>(footer.BlockCount * sizeof (TBlockInfo)));

        File->Write(infoBegin, footer.BlockCount * sizeof (TBlockInfo));

        File->Write(&footer, sizeof (footer));

        File->Flush();
        File->Close();
        File.Destroy();
    }

    //! Implements IChunkWriter and calls #Close.
    virtual TAsyncResult<TVoid>::TPtr AsyncClose()
    {
        Close();
        return new TAsyncResult<TVoid>(TVoid());
    }

    virtual void Cancel()
    {
        File.Destroy();
    }

private:
    THolder<TFile> File;
    yvector<TBlockInfo> BlockInfos;

};


///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
