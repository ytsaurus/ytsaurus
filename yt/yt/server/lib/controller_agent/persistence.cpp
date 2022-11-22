#include "persistence.h"

#include "serialize.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(
    IZeroCopyInput* input,
    NTableClient::TRowBufferPtr rowBuffer,
    ESnapshotVersion version)
    : NTableClient::TLoadContext(input, std::move(rowBuffer))
{
    SetVersion(ToUnderlying(version));
}

ESnapshotVersion TLoadContext::GetVersion() const
{
    return static_cast<ESnapshotVersion>(NTableClient::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(IZeroCopyOutput* output)
    : NTableClient::TSaveContext(output, ToUnderlying(GetCurrentSnapshotVersion()))
{ }

ESnapshotVersion TSaveContext::GetVersion() const
{
    return static_cast<ESnapshotVersion>(NTableClient::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
