#pragma once

#include <contrib/ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <contrib/ydb/library/yql/providers/solomon/actors/dq_solomon_write_actor.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <contrib/ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

namespace NYql::NDq {

void InitAsyncOutput(
    TFakeCASetup& caSetup,
    NSo::NProto::TDqSolomonShard&& settings,
    i64 freeSpace = 100000);

void CleanupSolomon(TString cloudId, TString folderId, TString service, bool isCloud);

TString GetSolomonMetrics(TString folderId, TString service);

NSo::NProto::TDqSolomonShard BuildSolomonShardSettings(bool isCloud);

NUdf::TUnboxedValue CreateStruct(
    NKikimr::NMiniKQL::THolderFactory& holderFactory,
    std::initializer_list<NUdf::TUnboxedValuePod> fields);

int GetMetricsCount(TString metrics);

} // namespace NYql::NDq
