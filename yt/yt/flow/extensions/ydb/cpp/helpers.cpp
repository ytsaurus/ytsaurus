#include "helpers.h"

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <util/generic/noncopyable.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NYdb::NTopic::ECodec ParseYdbCodec(const std::string& codec)
{
    if (codec == "raw") {
        return NYdb::NTopic::ECodec::RAW;
    } else if (codec == "gzip") {
        return NYdb::NTopic::ECodec::GZIP;
    } else if (codec == "lzop") {
        return NYdb::NTopic::ECodec::LZOP;
    } else if (codec == "zstd") {
        return NYdb::NTopic::ECodec::ZSTD;
    }
    THROW_ERROR_EXCEPTION("Unsupported codec");
}

std::string DecompressYdbTopicMessage(const std::string& data, NYdb::NTopic::ECodec codecId)
{
    if (codecId == NYdb::NTopic::ECodec::RAW) {
        return data;
    }
    const auto* codec = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(codecId));
    return codec->Decompress(data);
}

////////////////////////////////////////////////////////////////////////////////

class TInvokerExecutorWrapper
    : public NYdb::IExecutor
{
public:
    explicit TInvokerExecutorWrapper(IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
    { }

    bool IsAsync() const final
    {
        return true;
    }

    void Post(TFunction&& f) final
    {
        Invoker_->Invoke(BIND([f = std::move(f)] {
            f();
        }));
    }

private:
    IInvokerPtr Invoker_;

private:
    void DoStart() final
    { }

    void Stop() final
    { }
};

NYdb::IExecutor::TPtr CreateThreadPoolExecutorAdapter(IInvokerPtr invoker)
{
    return std::make_shared<TInvokerExecutorWrapper>(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TYdbReadSessionDestructionThread
{
public:
    const IInvokerPtr& GetInvoker()
    {
        return Queue_->GetInvoker();
    }

private:
    const NConcurrency::TActionQueuePtr Queue_ = New<NConcurrency::TActionQueue>("YdbReadSessionDestroy");
};

IInvokerPtr GetYdbReadSessionDestructionInvoker()
{
    return LeakySingleton<TYdbReadSessionDestructionThread>()->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

struct TReadSessionDestructionOwner
{
    // The driver must outlive SDK destruction even when the queued callback is discarded.
    IResourcePtr Driver;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
};

struct TReadSessionLifetimeGuard
    : private TNonCopyable
{
    TReadSessionDestructionOwner Owner;

    ~TReadSessionLifetimeGuard()
    {
        Owner.ReadSession->Close(TDuration::Zero());

        // Work around the YDB SDK destructor waiting for tasks scheduled on job executors.
        // Destroying the session on the same pool can block those tasks until the SDK timeout.
        GetYdbReadSessionDestructionInvoker()->Invoke(BIND([owner = std::move(Owner)] () mutable {
            owner.ReadSession.reset();
        }));
    }
};

} // namespace

std::shared_ptr<NYdb::NTopic::IReadSession> WrapReadSessionForAsyncDestruction(
    std::shared_ptr<NYdb::NTopic::IReadSession> readSession,
    IResourcePtr driver)
{
    auto* pointer = readSession.get();
    auto guard = std::make_shared<TReadSessionLifetimeGuard>();
    guard->Owner.Driver = std::move(driver);
    guard->Owner.ReadSession = std::move(readSession);
    return std::shared_ptr<NYdb::NTopic::IReadSession>(std::move(guard), pointer);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int YdbTopicKeyEndpointColumn = 1;
constexpr int YdbTopicKeyPartitionIndexColumn = 2;
constexpr int YdbTopicKeyExpectedColumns = 3;

} // namespace

TKey GenerateYdbTopicKey(
    std::string_view sourceIdentity,
    const std::string& endpoint,
    int partitionIndex)
{
    return MakeKey(TStringBuf(sourceIdentity), endpoint, partitionIndex);
}

std::pair<std::string, int> UnpackYdbTopicKey(const TKey& key)
{
    if (key.Underlying().GetCount() != YdbTopicKeyExpectedColumns) {
        THROW_ERROR_EXCEPTION("Topic partition key should have exactly %v fields, got: %v",
            YdbTopicKeyExpectedColumns,
            key.Underlying().GetCount())
            .With("topic_partition_key", key);
    }
    return {
        FromUnversionedValue<std::string>(key.Underlying()[YdbTopicKeyEndpointColumn]),
        FromUnversionedValue<i64>(key.Underlying()[YdbTopicKeyPartitionIndexColumn]),
    };
}

std::string GetYdbTopicKeyGroup(const TKey& key)
{
    // Grouping is advisory, so fall back instead of failing: a throw here aborts the whole controller
    // traverse iteration, freezing stream metrics and completion confirmations for every computation.
    if (key.Underlying().GetCount() != YdbTopicKeyExpectedColumns) {
        return "legacy-key-group";
    }
    return UnpackYdbTopicKey(key).first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
