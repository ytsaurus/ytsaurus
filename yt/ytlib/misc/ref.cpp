#include "stdafx.h"
#include "ref.h"

#include <util/ysaveload.h>

////////////////////////////////////////////////////////////////////////////////

void Save(TOutputStream* output, const NYT::TSharedRef& ref)
{
    if (ref == NYT::TSharedRef()) {
        ::Save(output, static_cast<i64>(-1));
    } else {
        ::Save(output, static_cast<i64>(ref.Size()));
        output->Write(ref.Begin(), ref.Size());
    }
}

void Load(TInputStream* input, NYT::TSharedRef& ref)
{
    i64 size;
    ::Load(input, size);
    if (size == -1) {
        ref = NYT::TSharedRef();
    } else {
        YASSERT(size >= 0);
        NYT::TBlob blob(static_cast<size_t>(size));
        YVERIFY(input->Load(blob.begin(), size) == size);
        ref = NYT::TSharedRef(NYT::MoveRV(blob));
    }
}

////////////////////////////////////////////////////////////////////////////////
