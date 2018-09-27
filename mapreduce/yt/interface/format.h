#pragma once

#include "node.h"

#include <contrib/libs/protobuf/message.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Data format for communication with YT proxies.
struct TFormat {
public:
    TNode Config;

public:
    explicit TFormat(const TNode& config = TNode());

    // Prefer using these methods to create your formats.
    static TFormat YsonText();
    static TFormat YsonBinary();
    static TFormat YaMRLenval();
    static TFormat Protobuf(const TVector<const ::google::protobuf::Descriptor*>& descriptors);

    template<typename T>
    static inline TFormat Protobuf();

    bool IsTextYson() const;
};

////////////////////////////////////////////////////////////////////////////////

template<typename T>
TFormat TFormat::Protobuf() {
    return TFormat::Protobuf({T::descriptor()});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
