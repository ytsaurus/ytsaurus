#pragma once

#include "fakes.h"

#include <yt/yt/flow/extensions/ydb/cpp/state.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/suspendable_invoker.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFlow::NYdbTesting {

////////////////////////////////////////////////////////////////////////////////

extern const TStreamId SinkTestStreamId;

struct TSinkSpecShape
{
    std::string SinkClassName;
    std::string ResourceClassName;
    THashMap<std::string, NYTree::INodePtr> ConnectionParameters;
    std::string WriterIdentityType;
};

struct TSinkTestEnvironment
{
    TSinkSpecShape Shape;
    NTableClient::TTableSchemaPtr Schema;
    TComputationStreamSpecStoragePtr StreamSpecStorage;
    TSinkContextPtr Context;
    TDynamicSinkContextPtr DynamicContext;
    NProfiling::TSolomonRegistryPtr Registry = New<NProfiling::TSolomonRegistry>();
    TStateManagerMockPtr StateManager = New<TStateManagerMock>();
    std::shared_ptr<TFakeYdbWriteState> WriteState = std::make_shared<TFakeYdbWriteState>();
    TFakeDeferredPublicationClientPtr DeferredClient = New<TFakeDeferredPublicationClient>();
    TFakeYdbResourcePtr Resource;
    NConcurrency::TActionQueuePtr SerializedQueue;
    ISuspendableInvokerPtr SuspendableInvoker;

    TSinkTestEnvironment(
        TSinkSpecShape shape,
        bool framing,
        i64 maxRowsPerBatch = 1000,
        ui64 jobGeneration = 10,
        bool asyncInvoker = false);

    std::string GetExternalPublicationId() const;
    std::string MakeWriterIdentity(TUniqueSeqNo generation, ui64 preparationAttempt = 1) const;
    TOutputMessageConstPtr MakeMessage(i64 id, std::string payload) const;

    template <class TSink>
    std::shared_ptr<int> Distribute(const TIntrusivePtr<TSink>& sink, const TOutputMessageConstPtr& message) const;

    template <class TSink>
    void Sync(const TIntrusivePtr<TSink>& sink, bool persist = true) const;

    void SetJobGeneration(ui64 generation) const;
    void DrainAsync() const;

    std::optional<TYdbDeferredPublicationStatePtr> TryGetPersistedDeferredState() const;
    TYdbDeferredPublicationStatePtr GetPersistedDeferredState() const;
    TYdbPendingPublicationStatePtr GetPersistedDeferredPublication() const;

    ISinkPtr CreateSink() const;

    void PersistCookie(
        ui64 id,
        std::string endpoint = "localhost:2135",
        std::string database = "/Root") const;
    void PersistCookies(const std::vector<std::tuple<ui64, std::string, std::string>>& cookies) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting

#define SINK_TEST_ENVIRONMENT_INL_H_
#include "sink_test_environment-inl.h"
#undef SINK_TEST_ENVIRONMENT_INL_H_
