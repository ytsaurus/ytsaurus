package internal

type Verb string

const (
	VerbCreate Verb = "create"
	VerbExists Verb = "exists"
	VerbRemove Verb = "remove"
	VerbGet    Verb = "get"
	VerbSet    Verb = "set"
	VerbList   Verb = "list"
	VerbCopy   Verb = "copy"
	VerbMove   Verb = "move"
	VerbLink   Verb = "link"

	VerbWriteFile        Verb = "write_file"
	VerbReadFile         Verb = "read_file"
	VerbPutFileToCache   Verb = "put_file_to_cache"
	VerbGetFileFromCache Verb = "get_file_from_cache"

	VerbWriteTable Verb = "write_table"
	VerbReadTable  Verb = "read_table"

	VerbStartOperation Verb = "start_operation"
	VerbGetOperation   Verb = "get_operation"
	VerbListJobs       Verb = "list_jobs"
	VerbJobStderr      Verb = "get_job_stderr"

	VerbStartTransaction  Verb = "start_transaction"
	VerbPingTransaction   Verb = "ping_transaction"
	VerbAbortTransaction  Verb = "abort_transaction"
	VerbCommitTransaction Verb = "commit_transaction"

	VerbAddMember    Verb = "add_member"
	VerbRemoveMember Verb = "remove_member"

	VerbLock   Verb = "lock"
	VerbUnlock Verb = "unlock"

	VerbInsertRows Verb = "insert_rows"
	VerbLookupRows Verb = "lookup_rows"
	VerbDeleteRows Verb = "delete_rows"
	VerbSelectRows Verb = "select_rows"

	VerbMountTable   Verb = "mount_table"
	VerbUnmountTable Verb = "unmount_table"
	VerbRemountTable Verb = "remount_table"
	VerbReshardTable Verb = "reshard_table"

	VerbLocateSkynetShare Verb = "locate_skynet_share"
)

func (v Verb) hasInput() bool {
	switch v {
	case VerbSet, VerbWriteFile, VerbWriteTable:
		return true

	case VerbInsertRows, VerbDeleteRows, VerbLookupRows:
		return true
	}

	return false
}

func (v Verb) IsHeavy() bool {
	switch v {
	case VerbReadFile, VerbWriteFile, VerbReadTable, VerbWriteTable:
		return true

	case VerbLocateSkynetShare:
		return true

	case VerbLookupRows, VerbDeleteRows, VerbSelectRows, VerbInsertRows:
		return true
	}

	return false
}

func (v Verb) volatile() bool {
	switch v {
	case VerbGet, VerbList, VerbExists, VerbReadFile, VerbReadTable, VerbGetOperation, VerbGetFileFromCache, VerbListJobs, VerbJobStderr:
		return false

	case VerbLocateSkynetShare:
		return false

	case VerbSelectRows, VerbLookupRows:
		return false
	}

	return true
}

func (v Verb) String() string {
	return string(v)
}

func (v Verb) HTTPMethod() string {
	if v.hasInput() {
		return "PUT"
	} else if v.volatile() {
		return "POST"
	} else {
		return "GET"
	}
}
