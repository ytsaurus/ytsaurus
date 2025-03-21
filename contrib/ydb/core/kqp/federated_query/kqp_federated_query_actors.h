#pragma once

#include <contrib/ydb/core/kqp/common/events/script_executions.h>
#include <contrib/ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <library/cpp/threading/future/future.h>


namespace NKikimr::NKqp {

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, TActorSystem* actorSystem);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem);

}  // namespace NKikimr::NKqp
