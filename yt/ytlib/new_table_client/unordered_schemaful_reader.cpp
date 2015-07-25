#include "stdafx.h"
#include "unordered_schemaful_reader.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"

#include <core/actions/cancelable_context.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TUnorderedSchemafulReader
    : public ISchemafulReader
{
public:
    TUnorderedSchemafulReader(std::function<ISchemafulReaderPtr()> getNextReader, int concurrency)
        : GetNextReader_(std::move(getNextReader))
    {
        for (int index = 0; index < concurrency; ++index) {
            auto reader = GetNextReader_();
            if (!reader) {
                Exhausted_ = true;
                break;
            }
            Sessions_.emplace_back();
            Sessions_.back().Reader = std::move(reader);
        }
    }

    ~TUnorderedSchemafulReader()
    {
        CancelableContext_->Cancel();
    }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        Schema_ = schema;
        for (auto& session : Sessions_) {
            session.ReadyEvent = MakeHolder(session.Reader->Open(schema));
            CancelableContext_->PropagateTo(session.ReadyEvent.Get());
        }
        return VoidFuture;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        bool pending = false;
        rows->clear();

        for (auto& session : Sessions_) {
            if (session.Exhausted && !RefillSession(session)) {
                continue;
            }

            if (session.ReadyEvent) {
                if (!session.ReadyEvent->IsSet()) {
                    pending = true;
                    continue;
                }

                const auto& error = session.ReadyEvent->Get();
                if (!error.IsOK()) {
                    ReadyEvent_ = MakePromise(error);
                    return true;
                }

                session.ReadyEvent->Reset();
            }

            if (!session.Reader->Read(rows)) {
                session.Exhausted = true;
                continue;
            }

            if (!rows->empty()) {
                return true;
            }

            YASSERT(!session.ReadyEvent);
            session.ReadyEvent = session.Reader->GetReadyEvent();
            CancelableContext_->PropagateTo(*session.ReadyEvent);
            pending = true;
        }

        if (!pending) {
            return false;
        }

        auto readyEvent = NewPromise<void>();
        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                readyEvent.TrySetFrom(*session.ReadyEvent);
            }
        }
        readyEvent.OnCanceled(BIND(&TUnorderedSchemafulReader::OnCanceled, MakeWeak(this)));
        ReadyEvent_ = readyEvent;

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    std::function<ISchemafulReaderPtr()> GetNextReader_;
    TTableSchema Schema_;
    bool Exhausted_ = false;

    struct TSession
    {
        ISchemafulReaderPtr Reader;
        TFutureHolder<void> ReadyEvent;
        bool Exhausted = false;
    };

    std::vector<TSession> Sessions_;
    TPromise<void> ReadyEvent_;
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

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
        session.ReadyEvent = session.Reader->Open(Schema_);
        session.Exhausted = false;
        CancelableContext_->PropagateTo(session.ReadyEvent.Get());
        return true;
    }

    void OnCanceled()
    {
        ReadyEvent_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader canceled"));
        CancelableContext_->Cancel();
    }

};

ISchemafulReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader,
    int concurrency)
{
    return New<TUnorderedSchemafulReader>(std::move(getNextReader), concurrency);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
