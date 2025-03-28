//
// StreamChannel.h
//
// Library: Foundation
// Package: Logging
// Module:  StreamChannel
//
// Definition of the StreamChannel class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_StreamChannel_INCLUDED
#define DB_Foundation_StreamChannel_INCLUDED


#include <ostream>
#include "DBPoco/Channel.h"
#include "DBPoco/Foundation.h"
#include "DBPoco/Mutex.h"


namespace DBPoco
{


class Foundation_API StreamChannel : public Channel
/// A channel that writes to an ostream.
///
/// Only the message's text is written, followed
/// by a newline.
///
/// Chain this channel to a FormattingChannel with an
/// appropriate Formatter to control what is contained
/// in the text.
{
public:
    StreamChannel(std::ostream & str);
    /// Creates the channel.

    void log(const Message & msg);
    /// Logs the given message to the channel's stream.

protected:
    virtual ~StreamChannel();

private:
    std::ostream & _str;
    FastMutex _mutex;
};


} // namespace DBPoco


#endif // DB_Foundation_StreamChannel_INCLUDED
