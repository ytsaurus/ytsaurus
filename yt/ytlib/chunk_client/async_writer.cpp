#include "stdafx.h"
#include "async_writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TAsyncError IAsyncWriter::AsyncWriteBlocks(const std::vector<TSharedRef>& blocks)
{
    class TWriteSession
        : public TRefCounted
    {
    public:
        TWriteSession(
            IAsyncWriterPtr writer,
            std::vector<TSharedRef> blocks)
            : Writer(writer)
            , CurrentIndex(-1)
            , Promise(NewPromise<TError>())
        {
            // TODO(babenko): use move ctor
            Blocks.swap(blocks);
        }

        TAsyncError Run()
        {
            WriteNextBlock();
            return Promise;
        }

    private:
        IAsyncWriterPtr Writer;
        std::vector<TSharedRef> Blocks;
        int CurrentIndex;
        TPromise<TError> Promise;

        void WriteNextBlock()
        {
            ++CurrentIndex;
            if (CurrentIndex == Blocks.size()) {
                Promise.Set(TError());
            } else {
                // ToDo: via writer thread.
                Writer->AsyncWriteBlock(Blocks[CurrentIndex]).Subscribe(BIND(
                    &TWriteSession::OnBlockWritten,
                    MakeStrong(this)));
            }
        }

        void OnBlockWritten(TError error)
        {
            if (error.IsOK()) {
                WriteNextBlock();
            } else {
                Promise.Set(error);
            }
        }

    };

    return New<TWriteSession>(this, blocks)->Run();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

