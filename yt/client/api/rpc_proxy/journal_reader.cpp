#include "journal_reader.h"

#include <yt/client/api/journal_reader.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcJournalReader
    : public IJournalReader
{
public:
    TRpcJournalReader(
        TApiServiceProxy::TReqCreateJournalReaderPtr request):
        Request_(request)
    {
        YCHECK(Request_);
        LastReadResult_ = MakeFuture(std::vector<TSharedRef>());
    }

    virtual TFuture<void> Open() override
    {
        Request_->Invoke();

        Opened_ = Request_->GetResponseAttachmentsStream()->Read() // TODOKETE this should be a separated function?? (ExpectHandshake)
            .Apply(BIND ([] (const TErrorOr<TSharedRef>& refOrError) {
                const auto& ref = refOrError.ValueOrThrow();
                if (ToString(ref) != JournalReaderHandshake) {
                    THROW_ERROR_EXCEPTION("Failed to open a journal reader: handshake mismatch");
                }
            }));

        return Opened_;
    }

    virtual TFuture<std::vector<TSharedRef>> Read() override
    {
        ValidateOpened();

        auto guard = Guard(SpinLock_);

        LastReadResult_ = LastReadResult_.Apply(
            BIND ([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TSharedRef>>& resultOrError) {
                resultOrError.ThrowOnError();
                return DoRead();
            }));

        return LastReadResult_;
    }

    ~TRpcJournalReader()
    {
        // TODO(kiselyovp) doing work in destructor but there's no better place?
        NConcurrency::WaitFor(Request_->GetRequestAttachmentsStream()->Close());
    }

private:
    TApiServiceProxy::TReqCreateJournalReaderPtr Request_;
    TFuture<void> Opened_;
    TFuture<std::vector<TSharedRef>> LastReadResult_;
    TFuture<TSharedRef> CurrentRow_;
    TSpinLock SpinLock_;

    void ValidateOpened()
    {
        if (!Opened_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't read from an unopened journal reader");
        }
        Opened_.Get().ThrowOnError();
    }

    TFuture<std::vector<TSharedRef>> DoRead()
    {
        if (!CurrentRow_) {
            CurrentRow_ = Request_->GetResponseAttachmentsStream()->Read();
        }

        return CurrentRow_.Apply(BIND ([=, this_ = MakeStrong(this)] (const TSharedRef& firstRow) {
            std::vector<TSharedRef> result;

            auto asyncNextRow = MakeFuture(firstRow);
            while (asyncNextRow.IsSet()) {
                auto nextRow = asyncNextRow.Get().ValueOrThrow();
                if (!nextRow) {
                    break;
                }
                result.push_back(nextRow);
                asyncNextRow = Request_->GetResponseAttachmentsStream()->Read();
            }
            CurrentRow_ = asyncNextRow;

            return result;
        }));
    }
};

IJournalReaderPtr CreateRpcJournalReader(
    TApiServiceProxy::TReqCreateJournalReaderPtr request)
{
    return New<TRpcJournalReader>(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
