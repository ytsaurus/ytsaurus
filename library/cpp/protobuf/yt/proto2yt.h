#pragma once

#include "parse_config.h"

#include <mapreduce/yt/interface/fwd.h>

namespace google {
    namespace protobuf {
        class Message;
    }
}

NYT::TNode DeduceSchema(const ::google::protobuf::Message& proto,
                        const bool inferAnyFromRepeated = false,
                        const bool useYtExtention = false,
                        const bool useLower = false,
                        const bool enumsAsString = false);

NYT::TNode ProtoToYtNode(const ::google::protobuf::Message& proto,
                        const TParseConfig config = {});
