#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NFq {

NActors::IActor* CreateDatabaseResolver(NActors::TActorId httpProxy, NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);
NActors::TActorId MakeDatabaseResolverActorId();

} /* namespace NFq */
