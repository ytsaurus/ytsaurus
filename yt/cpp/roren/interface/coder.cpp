#include "coder.h"

#include <library/cpp/yson/node/node_io.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

void TCoder<NYT::TNode>::Encode(IOutputStream* out, const NYT::TNode& node)
{
    auto data = NodeToYsonString(node, NYson::EYsonFormat::Binary);
    ::Save(out, data);
}

void TCoder<NYT::TNode>::Decode(IInputStream* in, NYT::TNode& node)
{
    TString data;
    ::Load(in, data);
    node = NYT::NodeFromYsonString(data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
