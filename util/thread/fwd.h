#pragma once

#include <util/generic/stlfwd.h>

struct TDefaultLFCounter;

template <class T, class TCounter = TDefaultLFCounter>
class TLockFreeQueue;

template <class T, class TCounter = TDefaultLFCounter>
class TAutoLockFreeQueue;

template <class T>
class TLockFreeStack;

class IThreadFactory;

struct IObjectInQueue;
class TThreadFactoryHolder;

using TThreadFunction = std::function<void()>;

class IThreadPool;
class TFakeThreadPool;
class TThreadPool;
class TAdaptiveThreadPool;
class TSimpleThreadPool;

template <class TQueueType, class TSlave>
class TThreadPoolBinder;
