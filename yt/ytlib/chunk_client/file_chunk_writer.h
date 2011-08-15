#pragma once

#include "chunk_writer.h"
#include "format.h"

#include <util/system/file.h>

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
class TFileChunkWriter
    : public IChunkWriter
{
public:
    typedef TIntrusivePtr<TFileChunkWriter> TPtr;

    TFileChunkWriter(Stroka fileName)
    {
        File.Reset(new TFile(fileName, CreateAlways|WrOnly|Seq));
    }

    void AddBlock(const TSharedRef& data)
    {
        TBlockInfo blockInfo;
        blockInfo.Size = data.Size();
        blockInfo.Checksum = GetChecksum(data);
        BlockInfos.push_back(blockInfo);

        File->Write(data.Begin(), data.Size());
        File->Flush();
    }

    virtual bool AsyncAddBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready)
    {
        UNUSED(ready);
        AddBlock(data);
        return true;
    }

    void Close()
    {
        // TODO: write more
        File->Flush();
        File->Close();
        File.Destroy();
    }

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
