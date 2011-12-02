#include "stdafx.h"
#include "file_writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(const Stroka& fileName)
    : FileName(fileName)
    , Result(New<TAsyncStreamState::TAsyncResult>())
{
    File.Reset(new TFile(fileName, CreateAlways|WrOnly|Seq));
    Result->Set(TAsyncStreamState::TResult());
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncWriteBlock(const TSharedRef& data)
{
    TBlockInfo* blockInfo = Meta.add_blocks();
    blockInfo->set_size(static_cast<int>(data.Size()));
    blockInfo->set_checksum(GetChecksum(data));

    File->Write(data.Begin(), data.Size());
    return Result;
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncClose(const TSharedRef& masterMeta)
{
    Meta.set_mastermeta(masterMeta.Begin(), masterMeta.Size());

    TBlob metaBlob(Meta.ByteSize());
    if (!Meta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        ythrow yexception() << Sprintf("Failed to serialize chunk meta in %s",
            ~FileName.Quote());
    }

    TChunkFooter footer;
    footer.Signature = TChunkFooter::ExpectedSignature;
    footer.MetaOffset = File->GetLength();
    footer.MetaSize = metaBlob.ysize();

    File->Write(metaBlob.begin(), metaBlob.ysize());
    File->Write(&footer, sizeof (footer));

    File->Close();
    File.Destroy();
    return Result;
}

void TFileWriter::Cancel(const Stroka& /*errorMessage*/)
{
    File.Destroy();
}

TChunkId TFileWriter::GetChunkId() const
{
    // ToDo: consider using ChunkId instead of file name
    // and implementing this.
    return TChunkId();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

