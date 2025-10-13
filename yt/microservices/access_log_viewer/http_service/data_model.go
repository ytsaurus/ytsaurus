package main

const (
	AbortFreeze        = "AbortFreeze"
	AbortMount         = "AbortMount"
	AbortReshard       = "AbortReshard"
	AbortUnmount       = "AbortUnmount"
	Alter              = "Alter"
	BeginCopy          = "BeginCopy"
	BeginUpload        = "BeginUpload"
	CheckPermission    = "CheckPermission"
	CommitFreeze       = "CommitFreeze"
	CommitMount        = "CommitMount"
	CommitRemount      = "CommitRemount"
	CommitReshard      = "CommitReshard"
	CommitUnfreeze     = "CommitUnfreeze"
	CommitUnmount      = "CommitUnmount"
	Copy               = "Copy"
	Create             = "Create"
	EndCopy            = "EndCopy"
	EndUpload          = "EndUpload"
	Exists             = "Exists"
	Fetch              = "Fetch"
	Get                = "Get"
	GetBasicAttributes = "GetBasicAttributes"
	Link               = "Link"
	List               = "List"
	Lock               = "Lock"
	Move               = "Move"
	PrepareFreeze      = "PrepareFreeze"
	PrepareMount       = "PrepareMount"
	PrepareRemount     = "PrepareRemount"
	PrepareReshard     = "PrepareReshard"
	PrepareUnfreeze    = "PrepareUnfreeze"
	PrepareUnmount     = "PrepareUnmount"
	Remove             = "Remove"
	Set                = "Set"
	TTLRemove          = "TtlRemove"
	Unlock             = "Unlock"
)

var methodGroups = map[string][]string{
	"read": []string{
		CheckPermission,
		Exists,
		Fetch,
		Get,
		GetBasicAttributes,
		List,
	},
	"write": []string{
		Alter,
		BeginUpload,
		Create,
		EndUpload,
		Set,
		Remove,
		TTLRemove,
	},
	"lock": []string{
		Lock,
		Unlock,
	},
	"link": []string{
		Link,
	},
	"copy_move": []string{
		Copy,
		BeginCopy,
		EndCopy,
		Move,
	},
	"dynamic_table_commands": []string{
		AbortFreeze,
		AbortMount,
		AbortReshard,
		AbortUnmount,
		CommitFreeze,
		CommitMount,
		CommitRemount,
		CommitReshard,
		CommitUnfreeze,
		CommitUnmount,
		PrepareFreeze,
		PrepareMount,
		PrepareRemount,
		PrepareReshard,
		PrepareUnfreeze,
		PrepareUnmount,
	},
}

type AccessRecord struct {
	Instant                 string `json:"instant"`
	Method                  string `json:"method"`
	User                    string `json:"user"`
	Path                    string `json:"path"`
	Type                    string `json:"type"`
	OriginalPath            string `json:"original_path"`
	TargetPath              string `json:"target_path"`
	DestinationPath         string `json:"destination_path"`
	OriginalDestinationPath string `json:"original_destination_path"`
	SourcePath              string `json:"source_path"`
	OriginalSourcePath      string `json:"original_source_path"`
	TransactionID           string `json:"transaction_id"`
	TransactionInfo         any    `json:"transaction_info"`

	MethodGroup string `json:"method_group"`
	Scope       string `json:"scope"`
	UserType    string `json:"user_type"` //["robot", "human", "system"] = None
}

type AccessLogResponse struct {
	Accesses      []AccessRecord `json:"accesses"`
	TotalRowCount uint32         `json:"total_row_count"`
}

type CountAccessLogResponse struct {
	TotalRowCount uint32 `json:"total_row_count"`
}

type ActionSelector map[string]bool
type ScopeSelector map[string]bool
type UserTypeSelector map[string]bool

type AccessRecordFieldSelector struct {
	OriginalPath    bool `json:"original_path"`
	TargetPath      bool `json:"target_path"`
	TransactionInfo bool `json:"transaction_info"`
	MethodGroup     bool `json:"method_group"`
	Scope           bool `json:"scope"`
	UserType        bool `json:"user_type"`
}

type PaginationRequest struct {
	Index uint32 `json:"index"`
	Size  uint32 `json:"size"`
}

type AccessLogRequest struct {
	Cluster       string                    `json:"cluster"`
	MethodGroup   ActionSelector            `json:"method_group"`
	Scope         ScopeSelector             `json:"scope"`
	Metadata      *bool                     `json:"metadata"`
	UserType      UserTypeSelector          `json:"user_type"`
	UserRegex     string                    `json:"user_regex"`
	Path          string                    `json:"path"`
	Recursive     bool                      `json:"recursive"`
	PathRegex     string                    `json:"path_regex"`
	Begin         float64                   `json:"begin"`
	End           float64                   `json:"end"`
	FieldSelector AccessRecordFieldSelector `json:"field_selector"`
	DistinctBy    string                    `json:"distinct_by"`
	Pagination    PaginationRequest         `json:"pagination"`
}

var AlwaysSelectedFields = []string{
	"method",
	"user",
	"instant",
	"path",
	"type",
	"destination_path",
	"original_destination_path",
	"source_path",
	"original_source_path",
	"transaction_id",
}

var objectTypes = map[string]string{
	"table":     "table",
	"file":      "file",
	"document":  "document",
	"directory": "map_node",
	"list":      "list_node",
}

var reverseObjectTypes = map[string]string{
	"table":     "table",
	"file":      "file",
	"document":  "document",
	"map_node":  "directory",
	"list_node": "list",
}

type VisibleTimeRangeRequest struct {
	Cluster string `json:"cluster"`
}

type VisibleTimeRangeResponse struct {
	Earliest int64 `json:"earliest"`
	Latests  int64 `json:"latests"`
}

type ServedClustersResponse struct {
	Clusters []string `json:"clusters"`
}

type ClickHouseQueryResponse struct {
	Query    string            `json:"query"`
	Settings map[string]string `json:"settings"`
}

type QTAccessLogResponse struct {
	Stage   string `json:"stage"`
	Cluster string `json:"cluster"`
	QueryID string `json:"query_id"`
}

/*
class AccessUserListResponse(BaseModel):
    users: list[str] = []
*/
