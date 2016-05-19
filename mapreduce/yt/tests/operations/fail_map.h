#pragma once

#include <mapreduce/interface/all.h>

#include <util/generic/yexception.h>


namespace NYT {
namespace NTestOps {

class TFailMap
    : public NMR::IMap
{
    OBJECT_METHODS(TFailMap);
public:
    void DoSub(NMR::TValue, NMR::TValue, NMR::TValue, NMR::TUpdate&) override {
        throw yexception();
    }
};

} // namespace NTestOps
} // namespace NYT

using namespace NYT::NTestOps;
REGISTER_SAVELOAD_CLASS(0x00000002, TFailMap);
