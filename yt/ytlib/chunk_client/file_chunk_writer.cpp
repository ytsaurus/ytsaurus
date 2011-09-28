#include "file_chunk_writer.h"

namespace NYT
{

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileChunkWriter::TFileChunkWriter(Stroka fileName)
    : FileName(fileName)
{
    File.Reset(new TFile(fileName, CreateAlways|WrOnly|Seq));
}

void TFileChunkWriter::WriteBlock(const TSharedRef& data)
{
    TBlockInfo* blockInfo = Meta.AddBlocks();
    blockInfo->SetSize(data.Size());
    blockInfo->SetChecksum(GetChecksum(data));

    File->Write(data.Begin(), data.Size());
}

IChunkWriter::EResult TFileChunkWriter::AsyncWriteBlock(
    const TSharedRef& data,
    TFuture<TVoid>::TPtr* ready)
{
    *ready = NULL;
    WriteBlock(data);
    return EResult::OK;
}

void TFileChunkWriter::Close()
{
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
}

TFuture<IChunkWriter::EResult>::TPtr TFileChunkWriter::AsyncClose()
{
    Close();
    return New< TFuture<EResult> >(EResult::OK);
}

void TFileChunkWriter::Cancel()
{
    File.Destroy();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT

