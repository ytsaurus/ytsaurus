package consts

import "go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/pkg/instanttime"

type AccessLogRow struct {
	// key values from AccessLogExport
	Cluster string                  `yson:"cluster" json:"cluster"`
	Path    string                  `yson:"path" json:"path"`
	Instant instanttime.InstantTime `yson:"instant" json:"instant"`

	// other values from AccessLogExport
	User                    string          `yson:"user,omitempty" json:"user,omitempty"`
	Method                  string          `yson:"method,omitempty" json:"method,omitempty"`
	Type                    string          `yson:"type,omitempty" json:"type,omitempty"`
	ID                      string          `yson:"id,omitempty" json:"id,omitempty"`
	MutationID              string          `yson:"mutation_id,omitempty" json:"mutation_id,omitempty"`
	RevisionType            string          `yson:"revision_type,omitempty" json:"revision_type,omitempty"`
	Revision                string          `yson:"revision,omitempty" json:"revision,omitempty"`
	Level                   string          `yson:"level,omitempty" json:"level,omitempty"`
	Category                string          `yson:"category,omitempty" json:"category,omitempty"`
	OriginalPath            string          `yson:"original_path,omitempty" json:"original_path,omitempty"`
	DestinationPath         string          `yson:"destination_path,omitempty" json:"destination_path,omitempty"`
	OriginalDestinationPath string          `yson:"original_destination_path,omitempty" json:"original_destination_path,omitempty"`
	TransactionInfo         TransactionInfo `yson:"transaction_info" json:"transaction_info"`

	IsoEventtime string `yson:"iso_eventtime,omitempty" json:"iso_eventtime,omitempty"`
}

type TransactionInfo struct {
	TransactionID    string           `yson:"transaction_id,omitempty" json:"transaction_id,omitempty"`
	TransactionTitle string           `yson:"transaction_title,omitempty" json:"transaction_title,omitempty"`
	OperationID      string           `yson:"operation_id,omitempty" json:"operation_id,omitempty"`
	OperationTitle   string           `yson:"operation_title,omitempty" json:"operation_title,omitempty"`
	Parent           *TransactionInfo `yson:"parent,omitempty" json:"parent,omitempty"`
}
