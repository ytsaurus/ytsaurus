#pragma once

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NTestOps {

class TIdMap
    : public NMR::IMap
{
    OBJECT_METHODS(TIdMap);
public:
    void DoSub(NMR::TValue k, NMR::TValue s, NMR::TValue v, NMR::TUpdate& update) override {
        update.AddSub(k, s, v);
    }
};

} // namespace NTestOps
} // namespace NYT

using namespace NYT::NTestOps;
REGISTER_SAVELOAD_CLASS(0x00000001, TIdMap);
