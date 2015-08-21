#include "stdafx.h"
#include "unordered_schemaful_reader.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"

#include <core/actions/cancelable_context.h>

#include <core/concurrency/rw_spinlock.h>

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
        , ReadyEvent_(MakePromise<void>(TError()))
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

            YASSERT(!session.ReadyEvent);
            session.ReadyEvent = session.Reader->GetReadyEvent();
            session.ReadyEvent->Subscribe(BIND(&TUnorderedSchemafulReader::OnReady, MakeStrong(this)));
            CancelableContext_->PropagateTo(*session.ReadyEvent);
            pending = true;
        }

        if (!pending) {
            return false;
        }

        {
            TWriterGuard guard(SpinLock_);
            ReadyEvent_ = NewPromise<void>();
        }

        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                ReadyEvent_.TrySetFrom(*session.ReadyEvent);
            }
        }

        ReadyEvent_.OnCanceled(BIND(&TUnorderedSchemafulReader::OnCanceled, MakeWeak(this)));

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        TReaderGuard guard(SpinLock_);
        auto readyEvent = ReadyEvent_;
        return readyEvent;
    }

private:
    std::function<ISchemafulReaderPtr()> GetNextReader_;
    std::vector<TSession> Sessions_;
    bool Exhausted_;

    TPromise<void> ReadyEvent_;
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();
    TReaderWriterSpinLock SpinLock_;

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

        session.Reader = std::move(reader);
        session.ReadyEvent->Reset();
        session.Exhausted = false;
        return true;
    }

    void OnReady(const TError& value)
    {
        TWriterGuard guard(SpinLock_);
        ReadyEvent_.TrySet(value);
    }

    void OnCanceled()
    {
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

    return New<TUnorderedSchemafulReader>(std::move(getNextReader), std::move(sessions), exhausted);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
