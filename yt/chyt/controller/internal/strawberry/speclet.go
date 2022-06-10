package strawberry

type Speclet struct {
	Pool *string `yson:"pool"`

	// TODO(dakovalkov): Does someone need it?
	// // OperationDescription is visible in clique operation UI.
	// OperationDescription map[string]interface{} `yson:"operation_description"`
	// // OperationTitle is YT operation title visible in operation UI.
	// OperationTitle *string `yson:"operation_title"`
	// // OperationAnnotations allows adding arbitrary human-readable annotations visible via YT list_operations API.
	// OperationAnnotations map[string]interface{} `yson:"operation_annotations"`
}
