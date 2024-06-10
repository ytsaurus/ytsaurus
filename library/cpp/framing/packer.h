#pragma once

#include "format.h"

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/fwd.h>

namespace google::protobuf {
    class Message;
}

namespace NFraming {
    // @brief serialize frames to output stream
    class TPacker {
    public:
        TPacker(EFormat format, IOutputStream& out)
            : Out_(&out)
            , Format_(EFormat::Auto == format ? EFormat::Protoseq : format)
        { }

        explicit TPacker(IOutputStream& out) : TPacker(EFormat::Protoseq, out)
        { }

        size_t GetFrameSize(size_t messageSize) const;

        // @brief add a new frame to output
        // @note frame size should fit into 4 bytes
        void Add(TStringBuf data);

        // @brief add protobuf
        // @param useCachedSize - if true protobuf will be added without recomputing the size
        // @note size of message should fit into 4 bytes
        void Add(const google::protobuf::Message& message, bool useCachedSize = false);

        // @brief flush underlying stream
        void Flush();

    private:
        IOutputStream* Out_;
        EFormat Format_;
        TBuffer Buffer_;
    };

    // @brief convenient function to pack sequence of frames to stream
    // @tparam It should contain value which is convertible to TStringBuf or google::protobuf::Message&
    template <class TIt>
    inline void Pack(EFormat format, IOutputStream& out, TIt first, TIt last) {
        TPacker packer{format, out};
        for (; first != last; ++first) {
            packer.Add(*first);
        }
        packer.Flush();
    }

    // @brief pack sequence of frames to stream by using protoseq codec
    template <class TIt>
    inline void Pack(IOutputStream& out, TIt first, TIt last) {
        Pack(EFormat::Protoseq, out, first, last);
    }

    // @brief convenient function to pack container
    template <class TOut, class TCont>
    inline void Pack(EFormat format, TOut& out, const TCont& cont) {
        Pack(format, out, cont.begin(), cont.end());
    }

    template <class TOut, class TCont>
    inline void Pack(TOut& out, const TCont& cont) {
        Pack(EFormat::Protoseq, out, cont.begin(), cont.end());
    }

    // @brief functions to pack string or protobuf to a string
    // it may be necessary to concatenate frames in external services (for example in unified-agent)
    TString PackToString(EFormat format, TStringBuf text);

    TString PackToString(EFormat format, const google::protobuf::Message& proto, const bool useCachedSize = false);

    inline TString PackToString(TStringBuf text) {
        return PackToString(EFormat::Protoseq, text);
    }

    inline TString PackToString(const google::protobuf::Message& proto, const bool useCachedSize = false) {
        return PackToString(EFormat::Protoseq, proto, useCachedSize);
    }
}
