#pragma once

#include "parse_config.h"

#include <mapreduce/yt/interface/fwd.h>

namespace google {
    namespace protobuf {
        class Message;
    }
}

void YtNodeToProto(const NYT::TNode& node, ::google::protobuf::Message& proto,
                   const TParseConfig config = {});
