#pragma once

#include "types.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// simple block buffer able to write numbers (varints) and arrays of bytes

class TBlockBuffer
{
    ui32 SizeLimit; // static buffer size

    TBlob Buffer; // static
    TBlob Excess; // dynamic, when overfilling

    ui32 WritePos;
    ui32 ReadPos;

    inline size_t Sizeof(size_t number) const;
    inline char* GetPtr(ui32 pos) const;
    inline void Allocate(size_t size);
    inline void Append(size_t number);
    inline void Append(const char* data, size_t size);

public:

    TBlockBuffer();
    ~TBlockBuffer();

    void SetSizeLimit(ui32 sizeLimit);
    void Clear();

    // read
    void SetData(TBlob* data);

    ui32 GetReadPos() const;
    void SetReadPos(ui32 pos);

    size_t ReadNumber();
    TValue ReadValue();

    // write
    void GetData(TBlob* data);

    ui32 GetWritePos() const;

    size_t WriteNumber(size_t number);
    size_t WriteValue(const TValue& value);
};

////////////////////////////////////////////////////////////////////////////////

}
