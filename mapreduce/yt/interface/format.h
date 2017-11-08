#pragma once

#include "node.h"

#include <contrib/libs/protobuf/message.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum class EFormatType : int {
    YsonText,
    YsonBinary,
    YaMRLenval,
    Protobuf
};

////////////////////////////////////////////////////////////////////////////////

// Data format for communication with YT proxies
struct TFormat {
    EFormatType Type;
    TNode Config;

    TFormat(EFormatType type, const TNode& config = TNode());
    TFormat(const yvector<const ::google::protobuf::Descriptor*>& descriptors);

    // Prefer using these methods to create your formats
    static inline TFormat YsonText();
    static inline TFormat YsonBinary();
    static inline TFormat YaMRLenval();
    template<typename T>
    static inline TFormat Protobuf();

};

////////////////////////////////////////////////////////////////////////////////

TFormat TFormat::YsonText() {
    return TFormat(EFormatType::YsonText);
}

TFormat TFormat::YsonBinary() {
    return TFormat(EFormatType::YsonBinary);
}

TFormat TFormat::YaMRLenval() {
    return TFormat(EFormatType::YaMRLenval);
}

template<typename T>
TFormat TFormat::Protobuf() {
    return TFormat({T::descriptor()});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
