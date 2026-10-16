#pragma once

#include "row.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/experiments/public/lsm_simulator/lib/simulator.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedOwningRow BuildNativeKey(TKey key);

template <class T>
T ReadYsonSerializableWithComments(const std::string& filename)
{
    std::string configString;

    // TODO(babenko): drop TString cast once TIFStream accepts std::string.
    TIFStream stream{TString(filename)};
    // TODO(babenko): drop TString once TInputStream::ReadLine accepts std::string.
    TString line;

    while (stream.ReadLine(line)) {
        auto hashPos = line.find_first_of('#');
        if (hashPos != TString::npos) {
            line = line.substr(0, hashPos);
        }
        configString += line + "\n";
    }
    return NYT::NYTree::ConvertTo<T>(NYT::NYson::TYsonString(configString));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
