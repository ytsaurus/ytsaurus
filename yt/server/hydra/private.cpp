#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TLazyIntrusivePtr<NConcurrency::TActionQueue> HydraIOQueue(
    NConcurrency::TActionQueue::CreateFactory("HydraIO"));

const Stroka LogSuffix(".log");
const Stroka IndexSuffix(".index");
const Stroka MultiplexedDirectory("multiplexed");
const Stroka SplitSuffix(".split");
const Stroka CleanSuffix(".clean");

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
