#pragma once

namespace clickhouse {

    /// Types of packets received from server
    namespace ServerCodes {
        enum {
            Hello                = 0,    /// Name, version, revision.
            Data                 = 1,    /// `Block` of data, may be compressed.
            Exception            = 2,    /// Exception that occurred on server side during query execution.
            Progress             = 3,    /// Query execcution progress: rows and bytes read.
            Pong                 = 4,    /// response to Ping sent by client.
            EndOfStream          = 5,    /// All packets were sent.
            ProfileInfo          = 6,    /// Profiling data
            Totals               = 7,    /// Block of totals, may be compressed.
            Extremes             = 8,    /// Block of mins and maxs, may be compressed.
            TablesStatusResponse = 9,    /// Response to TableStatus.
            Log                  = 10,   /// Query execution log.
            TableColumns         = 11,   /// Columns' description for default values calculation
            PartUUIDs            = 12,   /// List of unique parts ids.
            ReadTaskRequest      = 13,   /// String (UUID) describes a request for which next task is needed
                                         /// This is such an inverted logic, where server sends requests
                                         /// And client returns back response
            ProfileEvents        = 14,   /// Packet with profile events from server.
        };
    }

    /// Types of packets sent by client.
    namespace ClientCodes {
        enum {
            Hello       = 0,    /// Name, version, default database name.
            Query       = 1,    /** Query id, query settings, query processing stage,
                                  * compression status, and query text (no INSERT data).
                                  */
            Data        = 2,    /// Data `Block` (e.g. INSERT data), may be compressed.
            Cancel      = 3,    /// Cancel query.
            Ping        = 4,    /// Check server connection.
        };
    }

    /// Should we compress `Block`s of data
    namespace CompressionState {
        enum {
            Disable     = 0,
            Enable      = 1,
        };
    }

    namespace Stages {
        enum {
            Complete    = 2,
        };
    }
}
