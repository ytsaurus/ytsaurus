#include "unordered_schemaful_reader.h"

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/schemaful_reader.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

// 1. Sequential prefetch
//    - 0
//    - 1
//    - all
// 2. Unordered
//    - full concurrency and prefetch

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

    virtual TDataStatistics GetDataStatistics() const override
    {
        TReaderGuard guard(SpinLock_);
        auto dataStatistics = DataStatistics_;
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                dataStatistics += session.Reader->GetDataStatistics();
            }
        }
        return dataStatistics;
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        TReaderGuard guard(SpinLock_);
        auto result = DecompressionStatistics_;
        for (const auto& session : Sessions_) {
            if (session.Reader) {
                result += session.Reader->GetDecompressionStatistics();
            }
        }
        return result;
    }

private:
    std::function<ISchemafulReaderPtr()> GetNextReader_;
    std::vector<TSession> Sessions_;
    bool Exhausted_;
    TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics DecompressionStatistics_;

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
        auto dataStatistics = session.Reader->GetDataStatistics();
        auto cpuCompressionStatistics = session.Reader->GetDecompressionStatistics();
        {
            TWriterGuard guard(SpinLock_);
            DataStatistics_ += dataStatistics;
            DecompressionStatistics_ += cpuCompressionStatistics;
            session.Reader.Reset();
        }

        if (Exhausted_) {
            return false;
        }

        auto reader = GetNextReader_();
        if (!reader) {
            Exhausted_ = true;
            return false;
        }

        {
            TWriterGuard guard(SpinLock_);
            session.Exhausted = false;
            session.Reader = std::move(reader);
        }

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

ISchemafulReaderPtr CreateOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader)
{
    return CreateUnorderedSchemafulReader(getNextReader, 1);
}

ISchemafulReaderPtr CreatePrefetchingOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader)
{
    auto nextReader = getNextReader();
    auto readerGenerator = [
        nextReader = std::move(nextReader),
        getNextReader = std::move(getNextReader)
    ] () mutable -> ISchemafulReaderPtr {
        auto currentReader = nextReader;
        if (currentReader) {
            nextReader = getNextReader();
        }

        return currentReader;
    };

    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

ISchemafulReaderPtr CreateFullPrefetchingOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader)
{
    std::vector<ISchemafulReaderPtr> readers;

    while (auto nextReader = getNextReader()) {
        readers.push_back(nextReader);
    }

    auto readerGenerator = [
        index = 0,
        readers = std::move(readers)
    ] () mutable -> ISchemafulReaderPtr {
        if (index == readers.size()) {
            return nullptr;
        }

        return readers[index++];
    };

    return CreateUnorderedSchemafulReader(readerGenerator, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
