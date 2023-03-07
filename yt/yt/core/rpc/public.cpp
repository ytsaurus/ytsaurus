#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;
const TMutationId NullMutationId;
const TString RootUserName("root");

const TString RequestIdAnnotation("rpc.request_id");
const TString EndpointAnnotation("rpc.endpoint");
const TString RequestInfoAnnotation("rpc.request_info");
const TString ResponseInfoAnnotation("rpc.response_info");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
