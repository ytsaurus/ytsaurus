#include "stdafx.h"
#include "file_chunk_writer.h"

namespace NYT
{

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileChunkWriter::TFileChunkWriter(Stroka fileName)
    : FileName(fileName)
    , Result(New<TAsyncStreamState::TAsyncResult>())
{
    File.Reset(new TFile(fileName, CreateAlways|WrOnly|Seq));
    Result->Set(TAsyncStreamState::TResult());
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileChunkWriter::AsyncWriteBlock(const TSharedRef& data)
{
    TBlockInfo* blockInfo = Meta.AddBlocks();
    blockInfo->SetSize(static_cast<int>(data.Size()));
    blockInfo->SetChecksum(GetChecksum(data));

    File->Write(data.Begin(), data.Size());
    return Result;
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileChunkWriter::AsyncClose(const TSharedRef& masterMeta)
{
    Meta.SetMasterMeta(masterMeta.Begin(), masterMeta.Size());

    TBlob metaBlob(Meta.ByteSize());
    if (!Meta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        ythrow yexception() << Sprintf("Failed to serialize chunk meta in %s",
            ~FileName.Quote());
    }

    TChunkFooter footer;
    footer.Singature = TChunkFooter::ExpectedSignature;
    footer.MetaOffset = File->GetLength();
    footer.MetaSize = metaBlob.ysize();

    File->Write(metaBlob.begin(), metaBlob.ysize());
    File->Write(&footer, sizeof (footer));

    File->Close();
    File.Destroy();
    return Result;
}

void TFileChunkWriter::Cancel(const Stroka& /*errorMessage*/)
{
    File.Destroy();
}

const TChunkId& TFileChunkWriter::GetChunkId() const
{
    // ToDo: consider using ChunkId instead of file name
    // and implementing this.
    return TChunkId();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

