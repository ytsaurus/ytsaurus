#pragma once

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NTestOps {

class TIdReduce
    : public NMR::IReduce
{
    OBJECT_METHODS(TIdReduce);
public:
    void Do(NMR::TValue key, NMR::TTableIterator& input, NMR::TUpdate& output) override {
        for (; input.IsValid(); ++input) {
            output.AddSub(key, input.GetSubKey(), input.GetValue());
        }
    }
};

} // namespace NTestOps
} // namespace NYT

using namespace NYT::NTestOps;
REGISTER_SAVELOAD_CLASS(0x00000003, TIdReduce);
