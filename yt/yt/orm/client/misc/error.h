// AUTOMATICALLY GENERATED. DO NOT EDIT!
// See https://yt.yandex-team.ru/docs/orm/description/codegen#codegen-setup for details.
// See yt/yt/orm/codegen/generator/templates/error.h.j2 for source template.
// Use yt/yt/orm/codegen/run_static_codegen.sh to regenerate this file.

#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NOrm::NClient {

DEFINE_ENUM(EErrorCode,
    ((Ok) (0))
    ((InvalidObjectId) (100000))
    ((DuplicateObjectId) (100001))
    ((NoSuchObject) (100002))
    ((InvalidObjectType) (100004))
    ((AuthenticationError) (100005))
    ((AuthorizationError) (100006))
    ((InvalidTransactionState) (100007))
    ((InvalidTransactionId) (100008))
    ((InvalidObjectState) (100009))
    ((NoSuchTransaction) (100010))
    ((UserBanned) (100011))
    ((PrerequisiteCheckFailure) (100014))
    ((InvalidContinuationToken) (100015))
    ((RowsAlreadyTrimmed) (100016))
    ((InvalidObjectSpec) (100017))
    ((ContinuationTokenVersionMismatch) (100018))
    ((TimestampOutOfRange) (100019))
    ((RequestThrottled) (100020))
    ((UnstableDatabaseSettings) (100021))
    ((EntryNotFound) (100022))
    ((DuplicateRequest) (100023))
    ((NoSuchIndex) (100024))
    ((NotWatchable) (100025))
    ((TooManyAffectedObjects) (100026))
    ((IndexNotApplicable) (100027))
    ((WatchesConfigurationMismatch) (100028))
    ((InvalidRequestArguments) (100029))
    ((UniqueValueAlreadyExists) (100030))
    ((LimitTooLarge) (100031))
    ((SemaphoreFull) (100032))
    ((RemovalForbidden) (100033))
    ((InvalidAccessControlPermission) (100034))
);

} // NYT::NOrm::NClient
