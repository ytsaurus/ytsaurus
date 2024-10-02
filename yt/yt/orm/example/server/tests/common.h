#pragma once

#include <yt/yt/orm/example/server/library/bootstrap.h>

#include <yt/yt/orm/example/client/native/autogen/public.h>

#include <yt/yt/orm/server/objects/watch_query_executor.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NOrm::NExample::NServer::NTests {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NOrm::NExample::NClient::NNative;
using namespace NYT::NOrm::NExample::NServer::NLibrary;

using namespace NYT::NOrm::NClient::NObjects;
using namespace NYT::NOrm::NServer::NObjects;

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IBootstrap* GetBootstrap();

TObjectKey CreateObject(
    TObjectTypeValue type,
    NYTree::IMapNodePtr attributes);

TObjectKey CreateUser();
TObjectKey CreateEditor();
TObjectKey CreateAuthor();
TObjectKey CreatePublisher();
TObjectKey CreateBook(NYTree::IMapNodePtr spec);
TObjectKey CreateCat(std::optional<int> friendCatsCount);

TTransactionPtr StartTransaction();
void CommitTransaction(TTransactionPtr&& transaction);

////////////////////////////////////////////////////////////////////////////////

template<typename TTypedObject, std::same_as<TObjectKey> ...TObjectKeys>
auto GetObjects(const TTransactionPtr& transaction, TObjectKeys... keys);

////////////////////////////////////////////////////////////////////////////////

TWatchQueryResult WatchObject(IBootstrap* bootstrap, TWatchQueryOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests

#define COMMON_INL_H_
#include "common-inl.h"
#undef COMMON_INL_H_
