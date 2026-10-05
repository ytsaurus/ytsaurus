#include "client.h"
#include "clickhouse/version.h"
#include "protocol.h"

#include "base/compressed.h"
#include "base/socket.h"
#include "base/wire_format.h"

#include "columns/factory.h"

#include <cassert>
#include <optional>
#include <sstream>
#include <system_error>
#include <variant>
#include <vector>

#if defined(WITH_OPENSSL)
#include "base/sslsocket.h"
#endif

#define CLIENT_NAME "clickhouse-cpp"

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES         50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS   51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO               51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO              54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE          54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060
//#define DBMS_MIN_REVISION_WITH_TABLES_STATUS            54226
#define DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE 54337
#define DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME      54372
#define DBMS_MIN_REVISION_WITH_VERSION_PATCH            54401
#define DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE     54405
#define DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA 54410
#define DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO        54420
#define DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS 54429
#define DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET       54441
#define DBMS_MIN_REVISION_WITH_OPENTELEMETRY            54442
#define DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH        54448
#define DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME 54449
#define DBMS_MIN_REVISION_WITH_INCREMENTAL_PROFILE_EVENTS 54451
#define DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS 54453
#define DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION  54454 // Client can get some fields in JSon format
#define DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM 54458 // send quota key after handshake
#define DBMS_MIN_PROTOCOL_REVISION_WITH_QUOTA_KEY 54458 // the same
#define DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS 54459

#define DMBS_PROTOCOL_REVISION  DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS

namespace clickhouse {

struct ClientInfo {
    uint8_t iface_type = 1; // TCP
    uint8_t query_kind;
    std::string initial_user;
    std::string initial_query_id;
    std::string quota_key;
    std::string os_user;
    std::string client_hostname;
    std::string client_name;
    std::string initial_address = "[::ffff:127.0.0.1]:0";
    uint64_t client_version_major = 0;
    uint64_t client_version_minor = 0;
    uint64_t client_version_patch = 0;
    uint32_t client_revision = 0;
};

std::ostream& operator<<(std::ostream& os, const Endpoint& endpoint) {
    return os << endpoint.host << ":" << endpoint.port;
}

std::ostream& operator<<(std::ostream& os, const ClientOptions& opt) {
    os << "Client("
       << " Endpoints : [";
    size_t extra_endpoints = 0;

    if (!opt.host.empty()) {
        extra_endpoints = 1;
        os << opt.user << '@' << Endpoint{opt.host, opt.port};

        if (opt.endpoints.size())
            os << ", ";
    }

    for (size_t i = 0; i < opt.endpoints.size(); i++) {
        os << opt.user << '@' << opt.endpoints[i]
           << ((i == opt.endpoints.size() - 1) ? "" : ", ");
    }

    os << "] (" << opt.endpoints.size() + extra_endpoints << " items )"
       << " ping_before_query:" << opt.ping_before_query
       << " send_retries:" << opt.send_retries
       << " retry_timeout:" << opt.retry_timeout.count()
       << " compression_method:"
       << (opt.compression_method == CompressionMethod::LZ4    ? "LZ4"
           : opt.compression_method == CompressionMethod::ZSTD ? "ZSTD"
                                                               : "None");
#if defined(WITH_OPENSSL)
    if (opt.ssl_options) {
        const auto & ssl_options = *opt.ssl_options;
        os << " SSL ("
           << " ssl_context: " << (ssl_options.ssl_context ? "provided by user" : "created internally")
           << " use_default_ca_locations: " << ssl_options.use_default_ca_locations
           << " path_to_ca_files: " << ssl_options.path_to_ca_files.size() << " items"
           << " path_to_ca_directory: " << ssl_options.path_to_ca_directory
           << " min_protocol_version: " << ssl_options.min_protocol_version
           << " max_protocol_version: " << ssl_options.max_protocol_version
           << " context_options: " << ssl_options.context_options
           << ")";
    }
#endif
    os << ")";
    return os;
}

ClientOptions& ClientOptions::SetSSLOptions(ClientOptions::SSLOptions options)
{
#ifdef WITH_OPENSSL
    ssl_options = options;
    return *this;
#else
    (void)options;
    throw OpenSSLError("Library was built with no SSL support");
#endif
}

namespace {

// Compared to std::visit this is a more convenient way to unpack std::variant values. The
// `VariantIndex` trait allows to get index of the variant by type. This way, the variant can be
// unpacked using the old and simple `switch` statement. While the standard way of doing do is
// std::visit, using it is very inconvenient, it's semantics are often unclear and it lead to
// bizarre and hard to parse errors.
template <typename T>
struct VariantIndexTag {};
template <typename T, typename V>
struct VariantIndex;
template <typename T, typename... Ts>
struct VariantIndex<T, std::variant<Ts...>>
    : std::integral_constant<size_t, std::variant<VariantIndexTag<Ts>...>{VariantIndexTag<T>{}}.index()>
{
};

struct Pong {
};
struct Hello {
};
struct Log {
    Block block;
};
struct TableColumns {
};
struct ProfileEvents {
    Block block;
};
struct EndOfStream {
};
using DecodedPacket = std::variant<
    std::monostate,
    Block,
    ServerException,
    Profile,
    Progress,
    Pong,
    Hello,
    Log,
    TableColumns,
    ProfileEvents,
    EndOfStream>;

std::unique_ptr<SocketFactory> GetSocketFactory(const ClientOptions& opts) {
    (void)opts;
#if defined(WITH_OPENSSL)
    if (opts.ssl_options)
        return std::make_unique<SSLSocketFactory>(opts);
    else
#endif
        return std::make_unique<NonSecureSocketFactory>();
}

std::unique_ptr<EndpointsIteratorBase> GetEndpointsIterator(const ClientOptions& opts) {
    if (opts.endpoints.empty())
    {
        throw ValidationError("The list of endpoints is empty");
    }

    return std::make_unique<RoundRobinEndpointsIterator>(opts.endpoints);
}

} // anonymous namespace

class Client::Impl {
public:
     Impl(const ClientOptions& opts);
     Impl(const ClientOptions& opts,
          std::unique_ptr<SocketFactory> socket_factory);
    ~Impl();

    void ExecuteQuery(Query query);
    void BeginExecuteQuery(const Query& query, bool finalize = true);
    
    // Note, next block returns the block, but also notifies `query.OnData()` if it is set.
    std::optional<Block> NextBlock();

    void SelectWithExternalData(Query query, const ExternalTables& external_tables);

    void SendCancel();

    void Cancel();

    bool IsSelecting() const { return state_ == State::Selecting; }

    void Insert(const std::string& table_name, const std::string& query_id, const Block& block);

    Block BeginInsert(Query query);

    void SendInsertBlock(const Block& block);

    void EndInsert();

    bool IsInserting() const { return state_ == State::Inserting; }

    void Ping();

    void ResetConnection();

    void ResetConnectionEndpoint();

    const ServerInfo& GetServerInfo() const;

    const std::optional<Endpoint>& GetCurrentEndpoint() const;

private:
    bool Handshake();

    DecodedPacket ReceivePacket(uint64_t* server_packet = nullptr);
    bool ProcessPacket(uint64_t* server_packet = nullptr);
    void ResetState();

    void SendQuery(const Query& query, bool finalize = true);
    void FinalizeQuery();

    void SendData(const Block& block);

    void SendBlockData(const Block& block);
    void SendExternalData(const ExternalTables& external_tables);

    bool SendHello();

    bool ReadBlock(InputStream& input, Block* block);

    bool ReceiveHello();

    /// Reads data packet form input stream.
    bool ReceiveData(Block & block);

    /// Reads exception packet form input stream.
    bool ReceiveException(bool rethrow = false, ServerError * error = nullptr);

    void WriteBlock(const Block& block, OutputStream& output);

    void CreateConnection();

    void InitializeStreams(std::unique_ptr<SocketBase>&& socket);

    inline size_t GetConnectionAttempts() const
    {
        return options_.endpoints.size() * options_.send_retries;
    }

private:
    /// In case of network errors tries to reconnect to server and
    /// call fuc several times.
    void RetryGuard(std::function<void()> func);

    void RetryConnectToTheEndpoint(std::function<void()>& func);

private:
    enum class State : uint8_t {
        Idle = 0,
        Selecting = 1,
        Inserting = 2,
    };

    class EnsureNull {
    public:
        inline EnsureNull(QueryEvents* ev, QueryEvents** ptr)
            : ptr_(ptr)
        {
            if (ptr_) {
                *ptr_ = ev;
            }
        }

        inline ~EnsureNull() {
            if (ptr_) {
                *ptr_ = nullptr;
            }
        }

    private:
        QueryEvents** ptr_;

    };


    const ClientOptions options_;
    Query query_;
    QueryEvents* events_;
    int compression_ = CompressionState::Disable;

    std::unique_ptr<SocketFactory> socket_factory_;

    std::unique_ptr<InputStream> input_;
    std::unique_ptr<OutputStream> output_;
    std::unique_ptr<SocketBase> socket_;
    std::unique_ptr<EndpointsIteratorBase> endpoints_iterator;

    std::optional<Endpoint> current_endpoint_;

    ServerInfo server_info_;

    State state_ = State::Idle;
};

ClientOptions modifyClientOptions(ClientOptions opts)
{
    if (opts.host.empty())
        return opts;

    Endpoint default_endpoint({opts.host, opts.port});
    opts.endpoints.emplace(opts.endpoints.begin(), default_endpoint);
    return opts;
}

Client::Impl::Impl(const ClientOptions& opts)
    : Impl(opts, GetSocketFactory(opts)) {}

Client::Impl::Impl(const ClientOptions& opts,
                   std::unique_ptr<SocketFactory> socket_factory)
    : options_(modifyClientOptions(opts))
    , events_(nullptr)
    , socket_factory_(std::move(socket_factory))
    , endpoints_iterator(GetEndpointsIterator(options_))
{
    CreateConnection();

    if (options_.compression_method != CompressionMethod::None) {
        compression_ = CompressionState::Enable;
    }
}

Client::Impl::~Impl() {
    try {
        if (state_ == State::Inserting) {
            EndInsert();
        }
    } catch (...) {
    }
}

void Client::Impl::BeginExecuteQuery(const Query& query, bool finalize) {
    if (state_ != State::Idle) {
        throw ValidationError("cannot execute query while executing another operation");
    }

    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    query_ = query;
    events_ = static_cast<QueryEvents*>(&query_);
    state_ = State::Selecting;

    try {
        SendQuery(query_, finalize);
    }
    catch (...) {
        ResetState();
        throw;
    }
}

std::optional<Block> Client::Impl::NextBlock() {
    if (state_ != State::Selecting) {
        throw ValidationError("cannot execute NextBlock while not selecting");
    }

    try {
        while (true) {
            auto packet = ReceivePacket();
            switch (packet.index()) {
            case VariantIndex<Block, decltype(packet)>(): {
                Block & block = std::get<Block>(packet);
                if (block.GetColumnCount() == 0) {
                    continue;
                }
                return {std::move(block)};
            }
            case VariantIndex<ServerError, decltype(packet)>():
            case VariantIndex<std::monostate, decltype(packet)>():
            case VariantIndex<EndOfStream, decltype(packet)>():
                ResetState();
                return std::nullopt;
            default:
                continue;
            }
        }
    }
    catch (...) {
        ResetState();
        throw;
    }
}

void Client::Impl::ExecuteQuery(Query query) {
    BeginExecuteQuery(query);
    while (NextBlock().has_value()) {
        ;
    }
}


void Client::Impl::SelectWithExternalData(Query query, const ExternalTables& external_tables) {
    if (server_info_.revision < DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
       throw UnimplementedError("This version of ClickHouse server doesn't support temporary tables");
    }

    BeginExecuteQuery(query, /*finalize=*/ false);
    try {
        SendExternalData(external_tables);
        FinalizeQuery();
    }
    catch (...) {
        ResetState();
        throw;
    }

    while (NextBlock().has_value()) {
        ;
    }
}

void Client::Impl::SendBlockData(const Block& block) {
    if (compression_ == CompressionState::Enable) {
        std::unique_ptr<OutputStream> compressed_output = std::make_unique<CompressedOutput>(output_.get(), options_.max_compression_chunk_size, options_.compression_method);
        BufferedOutput buffered(std::move(compressed_output), options_.max_compression_chunk_size);
    
        WriteBlock(block, buffered);
    } else {
        WriteBlock(block, *output_);
    }
}

void Client::Impl::SendExternalData(const ExternalTables& external_tables) {
    for (const auto& table: external_tables) {
        if (!table.data.GetRowCount()) {
           // skip empty blocks to keep the connection in the consistent state as the current request would be marked as finished by such an empty block
           continue;
        }
        WireFormat::WriteFixed<uint8_t>(*output_, ClientCodes::Data);
        WireFormat::WriteString(*output_, table.name);
        SendBlockData(table.data);
    }
}


std::string NameToQueryString(const std::string &input)
{
    std::string output;
    output.reserve(input.size() + 2);
    output += '`';

    for (const auto & c : input) {
        if (c == '`') {
            //escape ` with ``
            output.append("``");
        } else {
            output.push_back(c);
        }
    }

    output += '`';
    return output;
}

void Client::Impl::Insert(const std::string& table_name, const std::string& query_id, const Block& block) {
    if (state_ == State::Inserting) {
        throw ValidationError("cannot execute query while inserting, use SendInsertData instead");
    }
    if (state_ != State::Idle) {
        throw ValidationError("cannot execute query while executing another operation");
    }

    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    state_ = State::Inserting;

    std::stringstream fields_section;
    const auto num_columns = block.GetColumnCount();

    for (unsigned int i = 0; i < num_columns; ++i) {
        if (i == num_columns - 1) {
            fields_section << NameToQueryString(block.GetColumnName(i));
        } else {
            fields_section << NameToQueryString(block.GetColumnName(i)) << ",";
        }
    }

    Query query("INSERT INTO " + table_name + " ( " + fields_section.str() + " ) VALUES", query_id);
    SendQuery(query);

    // Wait for a data packet and return
    uint64_t server_packet = 0;
    while (ProcessPacket(&server_packet)) {
        if (server_packet == ServerCodes::Data) {
            SendData(block);
            EndInsert();
            return;
        }
    }

    throw ProtocolError("fail to receive data packet");
}

Block Client::Impl::BeginInsert(Query query) {
    if (state_ != State::Idle) {
        throw ValidationError("cannot execute query while executing another operation");
    }

    EnsureNull en(static_cast<QueryEvents*>(&query), &events_);

    if (options_.ping_before_query) {
        RetryGuard([this]() { Ping(); });
    }

    state_ = State::Inserting;

    // Create a callback to extract the block with the proper query columns.
    Block block;
    query.OnData([&block](const Block& b) {
        block = std::move(b);
        return true;
    });

    SendQuery(query.GetText());

    // Wait for a data packet and return
    uint64_t server_packet = 0;
    while (ProcessPacket(&server_packet)) {
        if (server_packet == ServerCodes::Data) {
            return block;
        }
    }

    throw ProtocolError("fail to receive data packet");
}

void Client::Impl::SendInsertBlock(const Block& block) {
    if (state_ != State::Inserting) {
        throw ValidationError("illegal to send insert data without first calling BeginInsert");
    }

    SendData(block);
}

void Client::Impl::EndInsert() {
    if (state_ != State::Inserting) {
        return;
    }

    // Send empty block as marker of end of data.
    SendData(Block());

    // Wait for EOS.
    uint64_t eos_packet{0};
    while (ProcessPacket(&eos_packet)) {
        ;
    }

    if (eos_packet != ServerCodes::EndOfStream && eos_packet != ServerCodes::Exception
        && eos_packet != ServerCodes::Log && options_.rethrow_exceptions) {
        throw ProtocolError(std::string{"unexpected packet from server while receiving end of query, expected (expected Exception, EndOfStream or Log, got: "}
                            + (eos_packet ? std::to_string(eos_packet) : "nothing") + ")");
    }
    state_ = State::Idle;
}

void Client::Impl::Ping() {
    if (state_ != State::Idle) {
        throw ValidationError("cannot execute query while executing another operation");
    }

    WireFormat::WriteUInt64(*output_, ClientCodes::Ping);
    output_->Flush();

    uint64_t server_packet;
    const bool ret = ProcessPacket(&server_packet);

    if (!ret || server_packet != ServerCodes::Pong) {
        throw ProtocolError("fail to ping server");
    }
}

void Client::Impl::ResetConnection() {
    InitializeStreams(socket_factory_->connect(options_, current_endpoint_.value()));
    state_ = State::Idle;

    if (!Handshake()) {
        throw ProtocolError("fail to connect to " + options_.host);
    }
}

void Client::Impl::ResetConnectionEndpoint() {
    current_endpoint_.reset();
    for (size_t i = 0; i < options_.endpoints.size();)
    {
        try
        {
            current_endpoint_ = endpoints_iterator->Next();
            ResetConnection();
            return;
        } catch (const std::system_error&) {
            if (++i == options_.endpoints.size())
            {
                current_endpoint_.reset();
                throw;
            }
        }
    }
}

void Client::Impl::CreateConnection() {
    // make sure to try to connect to each endpoint at least once even if `options_.send_retries` is 0
    const size_t max_attempts = (options_.send_retries ? options_.send_retries : 1);
    for (size_t i = 0; i < max_attempts;)
    {
        try
        {
            // Try to connect to each endpoint before throwing exception.
            ResetConnectionEndpoint();
            return;
        } catch (const std::system_error&) {
            if (++i >= max_attempts)
            {
                throw;
            }
        }
    }
}

const ServerInfo& Client::Impl::GetServerInfo() const {
    return server_info_;
}


const std::optional<Endpoint>& Client::Impl::GetCurrentEndpoint() const {
    return current_endpoint_;
}

bool Client::Impl::Handshake() {
    if (!SendHello()) {
        return false;
    }
    if (!ReceiveHello()) {
        return false;
    }

    if (server_info_.revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM) {
        WireFormat::WriteString(*output_, std::string());
    }

    return true;
}

DecodedPacket Client::Impl::ReceivePacket(uint64_t* server_packet) {
    uint64_t packet_type = 0;

    if (!WireFormat::ReadVarint64(*input_, &packet_type)) {
        return {};
    }
    if (server_packet) {
        *server_packet = packet_type;
    }

    switch (packet_type) {
    case ServerCodes::Data: {
        Block ret{};
        if (!ReceiveData(ret)) {
            throw ProtocolError("can't read data packet from input stream");
        }
        return ret;
    }

    case ServerCodes::Exception: {
        ServerError ret{std::make_shared<Exception>()};
        if (!ReceiveException(false, &ret)) {
            throw ProtocolError("can't read exception packet from input stream");
        }
        return ret;
    }

    case ServerCodes::ProfileInfo: {
        Profile ret{};

        if (!WireFormat::ReadUInt64(*input_, &ret.rows)) {
            return {};
        }
        if (!WireFormat::ReadUInt64(*input_, &ret.blocks)) {
            return {};
        }
        if (!WireFormat::ReadUInt64(*input_, &ret.bytes)) {
            return {};
        }
        if (!WireFormat::ReadFixed(*input_, &ret.applied_limit)) {
            return {};
        }
        if (!WireFormat::ReadUInt64(*input_, &ret.rows_before_limit)) {
            return {};
        }
        if (!WireFormat::ReadFixed(*input_, &ret.calculated_rows_before_limit)) {
            return {};
        }

        if (events_) {
            events_->OnProfile(ret);
        }

        return ret;
    }

    case ServerCodes::Progress: {
        Progress ret{};

        if (!WireFormat::ReadUInt64(*input_, &ret.rows)) {
            return {};
        }
        if (!WireFormat::ReadUInt64(*input_, &ret.bytes)) {
            return {};
        }
        if constexpr(DMBS_PROTOCOL_REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
            if (!WireFormat::ReadUInt64(*input_, &ret.total_rows)) {
                return {};
            }
        }
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
        {
            if (!WireFormat::ReadUInt64(*input_, &ret.written_rows)) {
                return {};
            }
            if (!WireFormat::ReadUInt64(*input_, &ret.written_bytes)) {
                return {};
            }
        }

        if (events_) {
            events_->OnProgress(ret);
        }

        return ret;
    }

    case ServerCodes::Pong: {
        return Pong{};
    }

    case ServerCodes::Hello: {
        return Hello{};
    }

    case ServerCodes::EndOfStream: {
        if (events_) {
            events_->OnFinish();
        }
        return EndOfStream{};
    }

    case ServerCodes::Log: {
        // log tag
        if (!WireFormat::SkipString(*input_)) {
            return {};
        }
        Log ret;

        // Use uncompressed stream since log blocks usually contain only one row
        if (!ReadBlock(*input_, &ret.block)) {
            return {};
        }

        if (events_) {
            events_->OnServerLog(ret.block);
        }
        return ret;
    }

    case ServerCodes::TableColumns: {
        // external table name
        if (!WireFormat::SkipString(*input_)) {
            return {};
        }

        //  columns metadata
        if (!WireFormat::SkipString(*input_)) {
            return {};
        }
        return TableColumns{};
    }

    case ServerCodes::ProfileEvents: {
        if (!WireFormat::SkipString(*input_)) {
            return {};
        }

        ProfileEvents ret;
        if (!ReadBlock(*input_, &ret.block)) {
            return {};
        }

        if (events_) {
            events_->OnProfileEvents(ret.block);
        }
        return ret;
    }

    default:
        throw UnimplementedError("unimplemented " + std::to_string((int)packet_type));
        break;
    }
}

bool Client::Impl::ProcessPacket(uint64_t* server_packet) {
    auto packet = ReceivePacket(server_packet);
    switch (packet.index()) {
    case VariantIndex<ServerError, decltype(packet)>():
    case VariantIndex<std::monostate, decltype(packet)>():
    case VariantIndex<EndOfStream, decltype(packet)>():
        return false;
    default:
        return true;
    }
}

void Client::Impl::ResetState()
{
    state_ = State::Idle;
    query_ = {};
    events_ = nullptr;
}

bool Client::Impl::ReadBlock(InputStream& input, Block* block) {
    // Additional information about block.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
        uint64_t num;
        BlockInfo info;

        // BlockInfo
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input, &info.is_overflows)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }
        if (!WireFormat::ReadFixed(input, &info.bucket_num)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(input, &num)) {
            return false;
        }

        block->SetInfo(std::move(info));
    }

    uint64_t num_columns = 0;
    uint64_t num_rows = 0;

    if (!WireFormat::ReadUInt64(input, &num_columns)) {
        return false;
    }
    if (!WireFormat::ReadUInt64(input, &num_rows)) {
        return false;
    }

    CreateColumnByTypeSettings create_column_settings;
    create_column_settings.low_cardinality_as_wrapped_column = options_.backward_compatibility_lowcardinality_as_wrapped_column;

    for (size_t i = 0; i < num_columns; ++i) {
        std::string name;
        std::string type;
        if (!WireFormat::ReadString(input, &name)) {
            return false;
        }
        if (!WireFormat::ReadString(input, &type)) {
            return false;
        }
    
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION) {
            uint8_t custom_format_len;
            if (!WireFormat::ReadFixed(input, &custom_format_len)) {
                return false;
            }
            if (custom_format_len > 0) {
                throw UnimplementedError(std::string("unsupported custom serialization"));
            }
        }  

        if (ColumnRef col = CreateColumnByType(type, create_column_settings)) {
            if (num_rows && !col->Load(&input, num_rows)) {
                throw ProtocolError("can't load column '" + name + "' of type " + type);
            }

            block->AppendColumn(name, col);
        } else {
            throw UnimplementedError(std::string("unsupported column type: ") + type);
        }
    }

    return true;
}

bool Client::Impl::ReceiveData(Block & block) {

    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        if (!WireFormat::SkipString(*input_)) {
            return false;
        }
    }

    if (compression_ == CompressionState::Enable) {
        CompressedInput compressed(input_.get());
        if (!ReadBlock(compressed, &block)) {
            return false;
        }
    } else {
        if (!ReadBlock(*input_, &block)) {
            return false;
        }
    }

    if (events_) {
        events_->OnData(block);
        if (!events_->OnDataCancelable(block)) {
            SendCancel();
        }
    }

    return true;
}

bool Client::Impl::ReceiveException(bool rethrow, ServerError * error) {
    std::shared_ptr<Exception> e(new Exception);
    bool has_nested = false; // obsolete: https://github.com/ClickHouse/ClickHouse/blob/ef11941cf5a/src/IO/ReadHelpers.cpp#L2017

    bool exception_received =
        WireFormat::ReadFixed(*input_, &e->code)
        && WireFormat::ReadString(*input_, &e->name)
        && WireFormat::ReadString(*input_, &e->display_text)
        && WireFormat::ReadString(*input_, &e->stack_trace)
        && WireFormat::ReadFixed(*input_, &has_nested);

    if (events_) {
        events_->OnServerException(*e);
    }

    if (rethrow || options_.rethrow_exceptions) {
        throw ServerError(e);
    }

    if (exception_received && error != nullptr) {
        *error = ServerError(e);
    }
    return exception_received;
}

void Client::Impl::SendCancel() {
    WireFormat::WriteUInt64(*output_, ClientCodes::Cancel);
    output_->Flush();
}

void Client::Impl::Cancel() {
    if (state_ != State::Selecting) {
        throw ValidationError("cannot cancel while not executing a query");
    }
    SendCancel();
    while (NextBlock().has_value()) {
        ;
    }
}

void Client::Impl::SendQuery(const Query& query, bool finalize) {
    WireFormat::WriteUInt64(*output_, ClientCodes::Query);
    WireFormat::WriteString(*output_, query.GetQueryID());

    /// Client info.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
        ClientInfo info;

        info.query_kind = 1;
        info.client_name          = CLIENT_NAME;
        info.client_version_major = CLICKHOUSE_CPP_VERSION_MAJOR;
        info.client_version_minor = CLICKHOUSE_CPP_VERSION_MINOR;
        info.client_version_patch = CLICKHOUSE_CPP_VERSION_PATCH;
        info.client_revision = DMBS_PROTOCOL_REVISION;


        WireFormat::WriteFixed(*output_, info.query_kind);
        WireFormat::WriteString(*output_, info.initial_user);
        WireFormat::WriteString(*output_, info.initial_query_id);
        WireFormat::WriteString(*output_, info.initial_address);
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_INITIAL_QUERY_START_TIME) {
            WireFormat::WriteFixed<int64_t>(*output_, 0);
        }
        WireFormat::WriteFixed(*output_, info.iface_type);

        WireFormat::WriteString(*output_, info.os_user);
        WireFormat::WriteString(*output_, info.client_hostname);
        WireFormat::WriteString(*output_, info.client_name);
        WireFormat::WriteUInt64(*output_, info.client_version_major);
        WireFormat::WriteUInt64(*output_, info.client_version_minor);
        WireFormat::WriteUInt64(*output_, info.client_revision);

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
            WireFormat::WriteString(*output_, info.quota_key);
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_DISTRIBUTED_DEPTH)
            WireFormat::WriteUInt64(*output_, 0u);
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            WireFormat::WriteUInt64(*output_, info.client_version_patch);
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY) {
            if (const auto& tracing_context = query.GetTracingContext()) {
                // Have OpenTelemetry header.
                WireFormat::WriteFixed(*output_, uint8_t(1));
                // No point writing these numbers with variable length, because they
                // are random and will probably require the full length anyway.
                WireFormat::WriteFixed(*output_, tracing_context->trace_id);
                WireFormat::WriteFixed(*output_, tracing_context->span_id);
                WireFormat::WriteString(*output_, tracing_context->tracestate);
                WireFormat::WriteFixed(*output_, tracing_context->trace_flags);
            } else {
                // Don't have OpenTelemetry header.
                WireFormat::WriteFixed(*output_, uint8_t(0));
            }
        } else {
            if (query.GetTracingContext()) {
                // Current implementation works only for server version >= v20.11.2.1-stable
                throw UnimplementedError(std::string("Can't send open telemetry tracing context to a server, server version is too old"));
            }
        }
        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS) {
            // replica dont supported by client
            WireFormat::WriteUInt64(*output_, 0);
            WireFormat::WriteUInt64(*output_, 0);
            WireFormat::WriteUInt64(*output_, 0);
        }
    }

    /// Per query settings
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) {
        for(const auto& [name, field] : query.GetQuerySettings()) {
            WireFormat::WriteString(*output_, name);
            WireFormat::WriteVarint64(*output_, field.flags);
            WireFormat::WriteString(*output_, field.value);
        }
    }
    else if (query.GetQuerySettings().size() > 0) {
        // Current implementation works only for server version >= v20.1.2.4-stable, since we do not implement binary settings serialization.
        throw UnimplementedError(std::string("Can't send query settings to a server, server version is too old"));
    }
    // Empty string signals end of serialized settings
    WireFormat::WriteString(*output_, std::string());

    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET) {
        WireFormat::WriteString(*output_, "");
    }

    WireFormat::WriteUInt64(*output_, Stages::Complete);
    WireFormat::WriteUInt64(*output_, compression_);
    WireFormat::WriteString(*output_, query.GetText());

    //Send params after query text
    if (server_info_.revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
        for(const auto& [name, value] : query.GetParams()) {
            // params is like query settings
            WireFormat::WriteString(*output_, name);
            const uint64_t Custom = 2;
            WireFormat::WriteVarint64(*output_, Custom);
            if (value)
                WireFormat::WriteQuotedString(*output_, *value);
            else
                WireFormat::WriteParamNullRepresentation(*output_);
        }
        WireFormat::WriteString(*output_, std::string()); // empty string after last param
    }
 
    if (finalize) {
        FinalizeQuery();
    }
}

void Client::Impl::FinalizeQuery() {
    // Send empty block as marker of
    // end of data
    SendData(Block());

    output_->Flush();
}

void Client::Impl::WriteBlock(const Block& block, OutputStream& output) {
    // Additional information about block.
    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
        WireFormat::WriteUInt64(output, 1);
        WireFormat::WriteFixed<uint8_t>(output, block.Info().is_overflows);
        WireFormat::WriteUInt64(output, 2);
        WireFormat::WriteFixed<int32_t>(output, block.Info().bucket_num);
        WireFormat::WriteUInt64(output, 0);
    }

    WireFormat::WriteUInt64(output, block.GetColumnCount());
    WireFormat::WriteUInt64(output, block.GetRowCount());

    for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        WireFormat::WriteString(output, bi.Name());
        WireFormat::WriteString(output, bi.Type()->GetName());

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION) {
            // TODO: custom serialization
            WireFormat::WriteFixed<uint8_t>(output, 0);
        }

        // Empty columns are not serialized and occupy exactly 0 bytes.
        // ref https://github.com/ClickHouse/ClickHouse/blob/39b37a3240f74f4871c8c1679910e065af6bea19/src/Formats/NativeWriter.cpp#L163
        const bool containsData = block.GetRowCount() > 0;
        if (containsData) {
            bi.Column()->Save(&output);
        }
    }
    output.Flush();
}

void Client::Impl::SendData(const Block& block) {
    WireFormat::WriteUInt64(*output_, ClientCodes::Data);

    if (server_info_.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
        WireFormat::WriteString(*output_, std::string());
    }
    SendBlockData(block);

    output_->Flush();
}

void Client::Impl::InitializeStreams(std::unique_ptr<SocketBase>&& socket) {
    std::unique_ptr<OutputStream> output = std::make_unique<BufferedOutput>(socket->makeOutputStream());
    std::unique_ptr<InputStream> input = std::make_unique<BufferedInput>(socket->makeInputStream());

    std::swap(input, input_);
    std::swap(output, output_);
    std::swap(socket, socket_);
}

bool Client::Impl::SendHello() {
    WireFormat::WriteUInt64(*output_, ClientCodes::Hello);
    WireFormat::WriteString(*output_, std::string(CLIENT_NAME));
    WireFormat::WriteUInt64(*output_, CLICKHOUSE_CPP_VERSION_MAJOR);
    WireFormat::WriteUInt64(*output_, CLICKHOUSE_CPP_VERSION_MINOR);
    WireFormat::WriteUInt64(*output_, DMBS_PROTOCOL_REVISION);
    WireFormat::WriteString(*output_, options_.default_database);
    WireFormat::WriteString(*output_, options_.user);
    WireFormat::WriteString(*output_, options_.password);

    output_->Flush();

    return true;
}

bool Client::Impl::ReceiveHello() {
    uint64_t packet_type = 0;

    if (!WireFormat::ReadVarint64(*input_, &packet_type)) {
        return false;
    }

    if (packet_type == ServerCodes::Hello) {
        if (!WireFormat::ReadString(*input_, &server_info_.name)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(*input_, &server_info_.version_major)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(*input_, &server_info_.version_minor)) {
            return false;
        }
        if (!WireFormat::ReadUInt64(*input_, &server_info_.revision)) {
            return false;
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
            if (!WireFormat::ReadString(*input_, &server_info_.timezone)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME) {
            if (!WireFormat::ReadString(*input_, &server_info_.display_name)) {
                return false;
            }
        }

        if (server_info_.revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
            if (!WireFormat::ReadUInt64(*input_, &server_info_.version_patch)) {
                return false;
            }
        }

        return true;
    } else if (packet_type == ServerCodes::Exception) {
        ReceiveException(true);
        return false;
    }

    return false;
}

void Client::Impl::RetryGuard(std::function<void()> func) {

    if (current_endpoint_)
    {
        for (unsigned int i = 0; ; ++i) {
            try {
                func();
                return;
            } catch (const std::system_error&) {
                bool ok = true;

                try {
                    socket_factory_->sleepFor(options_.retry_timeout);
                    ResetConnection();
                } catch (...) {
                    ok = false;
                }

                if (!ok && i == options_.send_retries) {
                    break;
                }
            }
        }
    }
    // Connections with current_endpoint_ are broken.
    // Trying to establish  with the another one from the list.
    size_t connection_attempts_count = GetConnectionAttempts();
    for (size_t i = 0; i < connection_attempts_count;)
    {
        try
        {
            socket_factory_->sleepFor(options_.retry_timeout);
            current_endpoint_ = endpoints_iterator->Next();
            ResetConnection();
            func();
            return;
        } catch (const std::system_error&) {
            if (++i == connection_attempts_count)
            {
                current_endpoint_.reset();
                throw;
            }
        }
    }
}

Client::Client(const ClientOptions& opts)
    : options_(opts)
    , impl_(new Impl(opts))
{
}

Client::Client(const ClientOptions& opts,
               std::unique_ptr<SocketFactory> socket_factory)
    : options_(opts)
    , impl_(new Impl(opts, std::move(socket_factory)))
{
}

Client::~Client()
{ }

void Client::Execute(const Query& query) {
    impl_->ExecuteQuery(query);
}

void Client::Select(const Query& query) {
    Execute(query);
}

void Client::Select(const std::string& query, SelectCallback cb) {
    Execute(Query(query).OnData(std::move(cb)));
}

void Client::Select(const std::string& query, const std::string& query_id, SelectCallback cb) {
    Execute(Query(query, query_id).OnData(std::move(cb)));
}

void Client::SelectCancelable(const std::string& query, SelectCancelableCallback cb) {
    Execute(Query(query).OnDataCancelable(std::move(cb)));
}

void Client::SelectCancelable(const std::string& query, const std::string& query_id, SelectCancelableCallback cb) {
    Execute(Query(query, query_id).OnDataCancelable(std::move(cb)));
}

void Client::SelectWithExternalData(const std::string& query, const ExternalTables& external_tables, SelectCallback cb) {
    impl_->SelectWithExternalData(Query(query).OnData(std::move(cb)), external_tables);
}

void Client::SelectWithExternalData(const std::string& query, const std::string& query_id, const ExternalTables& external_tables, SelectCallback cb) {
    impl_->SelectWithExternalData(Query(query, query_id).OnData(std::move(cb)), external_tables);
}

void Client::SelectWithExternalDataCancelable(const std::string& query, const ExternalTables& external_tables, SelectCancelableCallback cb) {
    impl_->SelectWithExternalData(Query(query).OnDataCancelable(std::move(cb)), external_tables);
}

void Client::SelectWithExternalDataCancelable(const std::string& query, const std::string& query_id, const ExternalTables& external_tables, SelectCancelableCallback cb) {
    impl_->SelectWithExternalData(Query(query, query_id).OnDataCancelable(std::move(cb)), external_tables);
}

void Client::BeginExecute(const Query& query) {
    impl_->BeginExecuteQuery(query);
}

void Client::BeginSelect(const Query& query) {
    impl_->BeginExecuteQuery(query);
}

void Client::BeginSelect(const char* query)
{
    BeginExecute(Query(query));
}

void Client::BeginSelect(const std::string& query)
{
    BeginExecute(Query(query));
}

void Client::BeginSelect(const std::string& query, const std::string& query_id)
{
    BeginExecute(Query(query, query_id));
}

std::optional<Block> Client::NextBlock() {
    return impl_->NextBlock();
}

void Client::Cancel()
{
    impl_->Cancel();
}

bool Client::IsSelecting() const
{
    return impl_->IsSelecting();
}

void Client::Insert(const std::string& table_name, const Block& block) {
    impl_->Insert(table_name, Query::default_query_id, block);
}

void Client::Insert(const std::string& table_name, const std::string& query_id, const Block& block) {
    impl_->Insert(table_name, query_id, block);
}

Block Client::BeginInsert(const std::string& query) {
    return impl_->BeginInsert(Query(query));
}

Block Client::BeginInsert(const std::string& query, const std::string& query_id) {
    return impl_->BeginInsert(Query(query, query_id));
}

void Client::SendInsertBlock(const Block& block) {
    impl_->SendInsertBlock(block);
}

void Client::EndInsert() {
    impl_->EndInsert();
}

bool Client::IsInserting() const {
    return impl_->IsInserting();
}

void Client::Ping() {
    impl_->Ping();
}

void Client::ResetConnection() {
    impl_->ResetConnection();
}

void Client::ResetConnectionEndpoint() {
    impl_->ResetConnectionEndpoint();
}

const std::optional<Endpoint>& Client::GetCurrentEndpoint() const {
    return impl_->GetCurrentEndpoint();
}

const ServerInfo& Client::GetServerInfo() const {
    return impl_->GetServerInfo();
}

Client::Version Client::GetVersion() {
    return Version {
        CLICKHOUSE_CPP_VERSION_MAJOR,
        CLICKHOUSE_CPP_VERSION_MINOR,
        CLICKHOUSE_CPP_VERSION_PATCH,
        CLICKHOUSE_CPP_VERSION_BUILD,
        ""
    };
}

}
