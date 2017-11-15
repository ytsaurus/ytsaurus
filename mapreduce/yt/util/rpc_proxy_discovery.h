#pragma once


#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYT {

/*
 * Temporary solution to get the list of awailable rpc proxies
 *
 * NB!!!  DO NOT USE INSIDE THE JOBS (currently implemented using HTTP API)
 */
TVector<TString> GetRpcProxyHosts(const TString& proxy, const TString& token = TString());

}

