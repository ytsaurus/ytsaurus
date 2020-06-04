#include "persistence.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion TLoadContext::GetVersion() const
{
    return static_cast<ESnapshotVersion>(NTableClient::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

ESnapshotVersion TSaveContext::GetVersion() const
{
    return static_cast<ESnapshotVersion>(NTableClient::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
