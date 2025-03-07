//
// FIFOEvent.h
//
// Library: Foundation
// Package: Events
// Module:  FIFOEvent
//
// Implementation of the FIFOEvent template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_FIFOEvent_INCLUDED
#define DB_Foundation_FIFOEvent_INCLUDED


#include "DBPoco/AbstractDelegate.h"
#include "DBPoco/AbstractEvent.h"
#include "DBPoco/FIFOStrategy.h"


namespace DBPoco
{


//@ deprecated
template <class TArgs, class TMutex = FastMutex>
class FIFOEvent : public AbstractEvent<TArgs, FIFOStrategy<TArgs, AbstractDelegate<TArgs>>, AbstractDelegate<TArgs>, TMutex>
/// A FIFOEvent uses internally a FIFOStrategy which guarantees
/// that delegates are invoked in the order they were added to
/// the event.
///
/// Note that as of release 1.4.2, this is the default behavior
/// implemented by BasicEvent, so this class is provided
/// for backwards compatibility only.
{
public:
    FIFOEvent() { }

    ~FIFOEvent() { }

private:
    FIFOEvent(const FIFOEvent & e);
    FIFOEvent & operator=(const FIFOEvent & e);
};


} // namespace DBPoco


#endif // DB_Foundation_FIFOEvent_INCLUDED
