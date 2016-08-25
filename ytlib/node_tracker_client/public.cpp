#include "public.h"

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

const TRackId NullRackId;
const Stroka DefaultNetworkName = "default";
const Stroka InterconnectNetworkName = "interconnect";
const TNetworkPreferenceList DefaultNetworkPreferences = {InterconnectNetworkName, DefaultNetworkName};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

