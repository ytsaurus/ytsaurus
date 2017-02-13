#include "unordered_schemaful_reader.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TSession
{
    ISchemafulReaderPtr Reader;
    TFutureHolder<void> ReadyEvent;
    bool Exhausted = false;
};

class TUnorderedSchemafulReader
    : public ISchemafulReader
{
public:
    TUnorderedSchemafulReader(
        std::function<ISchemafulReaderPtr()> getNextReader,
        std::vector<TSession> sessions,
        bool exhausted)
        : GetNextReader_(std::move(getNextReader))
        , Sessions_(std::move(sessions))
        , Exhausted_(exhausted)
    { }

    ~TUnorderedSchemafulReader()
    {
        CancelableContext_->Cancel();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        bool pending = false;
        rows->clear();

        for (auto& session : Sessions_) {
            if (session.Exhausted) {
                continue;
            }

            if (session.ReadyEvent) {
                if (!session.ReadyEvent->IsSet()) {
                    pending = true;
                    continue;
                }

                const auto& error = session.ReadyEvent->Get();
                if (!error.IsOK()) {
                    TWriterGuard guard(SpinLock_);
                    ReadyEvent_ = MakePromise<void>(error);
                    return true;
                }

                session.ReadyEvent->Reset();
            }

            if (!session.Reader->Read(rows)) {
                YCHECK(rows->empty());

                session.Exhausted = true;
                if (RefillSession(session)) {
                    pending = true;
                }
                continue;
            }

            if (!rows->empty()) {
                return true;
            }

            Y_ASSERT(!session.ReadyEvent);
            UpdateSession(session);
            pending = true;
        }

        if (!pending) {
            return false;
        }

        auto readyEvent = NewPromise<void>();
        {
            TWriterGuard guard(SpinLock_);
            ReadyEvent_ = readyEvent;
        }

        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                readyEvent.TrySetFrom(*session.ReadyEvent);
            }
        }

        readyEvent.OnCanceled(BIND(&TUnorderedSchemafulReader::OnCanceled, MakeWeak(this)));

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return DoGetReadyEvent();
    }

private:
    std::function<ISchemafulReaderPtr()> GetNextReader_;
    std::vector<TSession> Sessions_;
    bool Exhausted_;

    TPromise<void> ReadyEvent_ = MakePromise<void>(TError());
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();
    TReaderWriterSpinLock SpinLock_;

    TPromise<void> DoGetReadyEvent()
    {
        TReaderGuard guard(SpinLock_);
        return ReadyEvent_;
    }

    void UpdateSession(TSession& session)
    {
        session.ReadyEvent = session.Reader->GetReadyEvent();
        session.ReadyEvent->Subscribe(BIND(&TUnorderedSchemafulReader::OnReady, MakeStrong(this)));
        CancelableContext_->PropagateTo(*session.ReadyEvent);
    }

    bool RefillSession(TSession& session)
    {
        session.Reader.Reset();

        if (Exhausted_) {
            return false;
        }

        auto reader = GetNextReader_();
        if (!reader) {
            Exhausted_ = true;
            return false;
        }

        session.Exhausted = false;
        session.Reader = std::move(reader);
        UpdateSession(session);
        return true;
    }

    void OnReady(const TError& value)
    {
        DoGetReadyEvent().TrySet(value);
    }

    void OnCanceled()
    {
        DoGetReadyEvent().TrySet(TError(NYT::EErrorCode::Canceled, "Table reader canceled"));
        CancelableContext_->Cancel();
    }
};

ISchemafulReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader,
    int concurrency)
{
    std::vector<TSession> sessions;
    bool exhausted = false;
    for (int index = 0; index < concurrency; ++index) {
        auto reader = getNextReader();
        if (!reader) {
            exhausted = true;
            break;
        }
        sessions.emplace_back();
        sessions.back().Reader = std::move(reader);
    }

    return New<TUnorderedSchemafulReader>(
        std::move(getNextReader),
        std::move(sessions),
        exhausted);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
