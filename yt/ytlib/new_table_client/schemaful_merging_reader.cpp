#include "stdafx.h"
#include "schemaful_merging_reader.h"
#include "schemaful_reader.h"

#include <core/actions/future.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulMergingReader
    : public ISchemafulReader
{
public:
    explicit TSchemafulMergingReader(const std::vector<ISchemafulReaderPtr>& readers)
    {
        for (const auto& reader : readers) {
            TSession session;
            session.Reader = reader;
            Sessions_.push_back(session);
        }
    }

    virtual TAsyncError Open(const TTableSchema& schema) override
    {
        for (auto& session : Sessions_) {
            session.ReadyEvent = session.Reader->Open(schema);
        }
        return OKFuture;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        bool pending = false;

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
                    ReadyEvent_ = session.ReadyEvent;
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
            pending = true;
        }

        if (!pending) {
            return false;
        }

        auto readyEvent = NewPromise<TError>();
        for (auto& session : Sessions_) {
            if (session.ReadyEvent) {
                readyEvent.TrySetFrom(session.ReadyEvent);
            }
        }
        ReadyEvent_ = readyEvent;

        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        return ReadyEvent_;
    }

private:
    struct TSession
    {
        ISchemafulReaderPtr Reader;
        TAsyncError ReadyEvent;
        bool Exhausted = false;
    };

    std::vector<TSession> Sessions_;
    TAsyncError ReadyEvent_;

};

ISchemafulReaderPtr CreateSchemafulMergingReader(const std::vector<ISchemafulReaderPtr>& readers)
{
    return New<TSchemafulMergingReader>(readers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
