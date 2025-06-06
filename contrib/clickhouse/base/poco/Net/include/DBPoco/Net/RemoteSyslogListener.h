//
// RemoteSyslogListener.h
//
// Library: Net
// Package: Logging
// Module:  RemoteSyslogListener
//
// Definition of the RemoteSyslogListener class.
//
// Copyright (c) 2007-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_RemoteSyslogListener_INCLUDED
#define DB_Net_RemoteSyslogListener_INCLUDED


#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketAddress.h"
#include "DBPoco/NotificationQueue.h"
#include "DBPoco/SplitterChannel.h"
#include "DBPoco/ThreadPool.h"


namespace DBPoco
{
namespace Net
{


    class RemoteUDPListener;
    class SyslogParser;


    class Net_API RemoteSyslogListener : public DBPoco::SplitterChannel
    /// RemoteSyslogListener implements listening for syslog messages
    /// sent over UDP, according to RFC 5424 "The Syslog Protocol"
    /// and RFC 5426 "Transmission of syslog messages over UDP".
    ///
    /// In addition, RemoteSyslogListener also supports the "old" BSD syslog
    /// protocol, as described in RFC 3164.
    ///
    /// The RemoteSyslogListener is a subclass of DBPoco::SplitterChannel.
    /// Every received log message is sent to the channels registered
    /// with addChannel() or the "channel" property.
    ///
    /// DBPoco::Message objects created by RemoteSyslogListener will have
    /// the following named parameters:
    ///   - addr: IP address of the host/interface sending the message.
    ///   - host: host name; only for "new" syslog messages.
    ///   - app:  application name; only for "new" syslog messages.
    ///   - structured-data: RFC 5424 structured data, or empty if not present.
    {
    public:
        RemoteSyslogListener();
        /// Creates the RemoteSyslogListener.

        RemoteSyslogListener(DBPoco::UInt16 port);
        /// Creates the RemoteSyslogListener, listening on the given port number.

        RemoteSyslogListener(DBPoco::UInt16 port, int threads);
        /// Creates the RemoteSyslogListener, listening on the given port number
        /// and using the number of threads for message processing.

        void setProperty(const std::string & name, const std::string & value);
        /// Sets the property with the given value.
        ///
        /// The following properties are supported:
        ///     * port: The UDP port number where to listen for UDP packets
        ///       containing syslog messages. If 0 is specified, does not
        ///       listen for UDP messages.
        ///     * threads: The number of parser threads processing
        ///       received syslog messages. Defaults to 1. A maximum
        ///       of 16 threads is supported.

        std::string getProperty(const std::string & name) const;
        /// Returns the value of the property with the given name.

        void open();
        /// Starts the listener.

        void close();
        /// Stops the listener.

        void processMessage(const std::string & messageText);
        /// Parses a single line of text containing a syslog message
        /// and sends it down the filter chain.

        void enqueueMessage(const std::string & messageText, const DBPoco::Net::SocketAddress & senderAddress);
        /// Enqueues a single line of text containing a syslog message
        /// for asynchronous processing by a parser thread.

        static void registerChannel();
        /// Registers the channel with the global LoggingFactory.

        static const std::string PROP_PORT;
        static const std::string PROP_THREADS;

        static const std::string LOG_PROP_APP;
        static const std::string LOG_PROP_HOST;
        static const std::string LOG_PROP_STRUCTURED_DATA;

    protected:
        ~RemoteSyslogListener();
        /// Destroys the RemoteSyslogListener.

    private:
        RemoteUDPListener * _pListener;
        SyslogParser * _pParser;
        DBPoco::ThreadPool _threadPool;
        DBPoco::NotificationQueue _queue;
        DBPoco::UInt16 _port;
        int _threads;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_RemoteSyslogListener_INCLUDED
