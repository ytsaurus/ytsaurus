// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include <yt/yt/orm/example/client/misc/autogen/enums.h>
#include <yt/yt/orm/example/client/misc/autogen/schema_transitive.h>

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/orm/server/objects/public.h>
#include <yt/yt/orm/server/objects/semaphore_detail.h>
#include <yt/yt/orm/server/objects/semaphore_set_detail.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////
class TMotherShip;

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NPlugins

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterConfig)
DECLARE_REFCOUNTED_CLASS(TMasterDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(IDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

class TAuthor;class TBook;class TBufferedTimestampId;class TCat;class TEditor;class TEmployer;class TExecutor;class TGenre;class TGroup;class THitchhiker;class TIllustrator;class TIndexedIncrementId;class TInterceptor;class TManualId;class TMotherShip;class TMultipolicyId;class TNestedColumns;class TNexus;class TNirvanaDMProcessInstance;class TPublisher;class TRandomId;class TSchema;class TSemaphore;class TSemaphoreSet;class TTimestampId;class TTypographer;class TUser;class TWatchLogConsumer;

using TFinalSemaphore = NYT::NOrm::NServer::NObjects::TSemaphoreMixin<TSemaphore, NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore>;
using TFinalSemaphoreSet = NYT::NOrm::NServer::NObjects::TSemaphoreSetMixin<
    TSemaphoreSet,
    TFinalSemaphore,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet,
    NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore
>;

////////////////////////////////////////////////////////////////////////////////

using NYT::NOrm::NExample::NClient::NApi::EObjectType;
using NYT::NOrm::NExample::NClient::NApi::EEyeColor;
using NYT::NOrm::NExample::NClient::NApi::EHealthCondition;
using NYT::NOrm::NExample::NClient::NApi::EMood;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
