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
    explicit TUnorderedSchemafulReader(const std::vector<ISchemafulReaderPtr>& readers)
    {
        for (const auto& reader : readers) {
            TSession session;
            session.Reader = reader;
            Sessions_.push_back(session);
        }
    }

    ~TUnorderedSchemafulReader()
    {
        CancelableContext_->Cancel();
    }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        for (auto& session : Sessions_) {
            session.ReadyEvent = session.Reader->Open(schema);
            CancelableContext_->PropagateTo(session.ReadyEvent);
        }
        return VoidFuture;
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
                if (!session.ReadyEvent.IsSet()) {
                    pending = true;
                    continue;
                }

                const auto& error = session.ReadyEvent.Get();
                if (!error.IsOK()) {
                    ReadyEvent_ = MakePromise(error);
                    return true;
                }

                session.ReadyEvent.Reset();
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
            CancelableContext_->PropagateTo(session.ReadyEvent);
            pending = true;
        }

        if (!pending) {
            return false;
        }

        auto readyEvent = NewPromise<void>();
        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                readyEvent.TrySetFrom(session.ReadyEvent);
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
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();

    struct TSession
    {
        ISchemafulReaderPtr Reader;
        TFuture<void> ReadyEvent;
        bool Exhausted = false;
    };

    std::vector<TSession> Sessions_;
    TPromise<void> ReadyEvent_;


    void OnCanceled()
    {
        ReadyEvent_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader canceled"));
        CancelableContext_->Cancel();
    }

};

ISchemafulReaderPtr CreateUnorderedSchemafulReader(const std::vector<ISchemafulReaderPtr>& readers)
{
    return New<TUnorderedSchemafulReader>(readers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
