#include "public.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

//! Creates an authenticator which is used for all the internal RPC services.
//!
//! This authenticator uses connection to verify if the ticket source is valid. Now,
//! it's considered valid if it's either from the same cluster or from some cluster
//! specified in the connection's cluster directory.
NRpc::IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
