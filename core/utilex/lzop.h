#pragma once

#include "util/stream/ios.h"

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

class TLzopCompress: public TOutputStream {
    public:
        TLzopCompress(TOutputStream* slave, ui16 maxBlockSize = 1 << 15);
        virtual ~TLzopCompress() throw ();

    private:
        virtual void DoWrite(const void* buf, size_t len);
        virtual void DoFlush();
        virtual void DoFinish();

    private:
        class TImpl;
        THolder<TImpl> Impl_;
};

class TLzopDecompress: public TInputStream {
    public:
        TLzopDecompress(TInputStream* slave, ui32 initialBufferSize = 1 << 16);
        virtual ~TLzopDecompress() throw ();

    private:
        virtual size_t DoRead(void* buf, size_t len);

    private:
        class TImpl;
        THolder<TImpl> Impl_;
};
