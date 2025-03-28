//
// ServerApplication.h
//
// Library: Util
// Package: Application
// Module:  ServerApplication
//
// Definition of the ServerApplication class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Util_ServerApplication_INCLUDED
#define DB_Util_ServerApplication_INCLUDED


#include "DBPoco/Event.h"
#include "DBPoco/Util/Application.h"
#include "DBPoco/Util/Util.h"


namespace DBPoco
{
namespace Util
{


    class Util_API ServerApplication : public Application
    /// A subclass of the Application class that is used
    /// for implementing server applications.
    ///
    /// A ServerApplication allows for the application
    /// to run as a Windows service or as a Unix daemon
    /// without the need to add extra code.
    ///
    /// For a ServerApplication to work both from the command line
    /// and as a daemon or service, a few rules must be met:
    ///   - Subsystems must be registered in the constructor.
    ///   - All non-trivial initializations must be made in the
    ///     initialize() method.
    ///   - At the end of the main() method, waitForTerminationRequest()
    ///     should be called.
    ///   - New threads must only be created in initialize() or main() or
    ///     methods called from there, but not in the application class'
    ///     constructor or in the constructor of instance variables.
    ///     The reason for this is that fork() will be called in order to
    ///     create the daemon process, and threads created prior to calling
    ///     fork() won't be taken over to the daemon process.
    ///   - The main(argc, argv) function must look as follows:
    ///
    ///   int main(int argc, char** argv)
    ///   {
    ///       MyServerApplication app;
    ///       return app.run(argc, argv);
    ///   }
    ///
    /// The DB_POCO_SERVER_MAIN macro can be used to implement main(argc, argv).
    /// If POCO has been built with DB_POCO_WIN32_UTF8, DB_POCO_SERVER_MAIN supports
    /// Unicode command line arguments.
    ///
    /// On Windows platforms, an application built on top of the
    /// ServerApplication class can be run both from the command line
    /// or as a service.
    ///
    /// To run an application as a Windows service, it must be registered
    /// with the Windows Service Control Manager (SCM). To do this, the application
    /// can be started from the command line, with the /registerService option
    /// specified. This causes the application to register itself with the
    /// SCM, and then exit. Similarly, an application registered as a service can
    /// be unregistered, by specifying the /unregisterService option.
    /// The file name of the application executable (excluding the .exe suffix)
    /// is used as the service name. Additionally, a more user-friendly name can be
    /// specified, using the /displayName option (e.g., /displayName="Demo Service")
    /// and a service description can be added with the /description option.
    /// The startup mode (automatic or manual) for the service can be specified
    /// with the /startup option.
    ///
    /// An application can determine whether it is running as a service by checking
    /// for the "application.runAsService" configuration property.
    ///
    ///     if (config().getBool("application.runAsService", false))
    ///     {
    ///         // do service specific things
    ///     }
    ///
    /// Note that the working directory for an application running as a service
    /// is the Windows system directory (e.g., C:\Windows\system32). Take this
    /// into account when working with relative filesystem paths. Also, services
    /// run under a different user account, so an application that works when
    /// started from the command line may fail to run as a service if it depends
    /// on a certain environment (e.g., the PATH environment variable).
    ///
    /// An application registered as a Windows service can be started
    /// with the NET START <name> command and stopped with the NET STOP <name>
    /// command. Alternatively, the Services MMC applet can be used.
    ///
    /// On Unix platforms, an application built on top of the ServerApplication
    /// class can be optionally run as a daemon by giving the --daemon
    /// command line option. A daemon, when launched, immediately
    /// forks off a background process that does the actual work. After launching
    /// the background process, the foreground process exits.
    ///
    /// After the initialization is complete, but before entering the main() method,
    /// the current working directory for the daemon process is changed to the root
    /// directory ("/"), as it is common practice for daemon processes. Therefore, be
    /// careful when working with files, as relative paths may not point to where
    /// you expect them point to.
    ///
    /// An application can determine whether it is running as a daemon by checking
    /// for the "application.runAsDaemon" configuration property.
    ///
    ///     if (config().getBool("application.runAsDaemon", false))
    ///     {
    ///         // do daemon specific things
    ///     }
    ///
    /// When running as a daemon, specifying the --pidfile option (e.g.,
    /// --pidfile=/var/run/sample.pid) may be useful to record the process ID of
    /// the daemon in a file. The PID file will be removed when the daemon process
    /// terminates (but not, if it crashes).
    {
    public:
        ServerApplication();
        /// Creates the ServerApplication.

        ~ServerApplication();
        /// Destroys the ServerApplication.

        bool isInteractive() const;
        /// Returns true if the application runs from the command line.
        /// Returns false if the application runs as a Unix daemon
        /// or Windows service.

        int run(int argc, char ** argv);
        /// Runs the application by performing additional initializations
        /// and calling the main() method.

        int run(const std::vector<std::string> & args);
        /// Runs the application by performing additional initializations
        /// and calling the main() method.


        static void terminate();
        /// Sends a friendly termination request to the application.
        /// If the application's main thread is waiting in
        /// waitForTerminationRequest(), this method will return
        /// and the application can shut down.

    protected:
        int run();
        virtual void waitForTerminationRequest();
        void defineOptions(OptionSet & options);

    private:
#if   defined(DB_POCO_OS_FAMILY_UNIX)
        void handleDaemon(const std::string & name, const std::string & value);
        void handleUMask(const std::string & name, const std::string & value);
        void handlePidFile(const std::string & name, const std::string & value);
        bool isDaemon(int argc, char ** argv);
        void beDaemon();
#    if DB_POCO_OS == DB_POCO_OS_ANDROID
        static DBPoco::Event _terminate;
#    endif
#endif
    };


}
} // namespace DBPoco::Util


//
// Macro to implement main()
//
#    define DB_POCO_SERVER_MAIN(App) \
        int main(int argc, char ** argv) \
        { \
            try \
            { \
                App app; \
                return app.run(argc, argv); \
            } \
            catch (DBPoco::Exception & exc) \
            { \
                std::cerr << exc.displayText() << std::endl; \
                return DBPoco::Util::Application::EXIT_SOFTWARE; \
            } \
        }


#endif // DB_Util_ServerApplication_INCLUDED
