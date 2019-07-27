#include "public.h"

#include <yt/client/table_client/serialize.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
