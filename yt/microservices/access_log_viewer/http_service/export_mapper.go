package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	_ "time/tzdata"

	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type AccessLogExportRow struct {
	Path                    *string `yson:"path"`
	Instant                 *string `yson:"instant"`
	TargetPath              *string `yson:"target_path"`
	OriginalPath            *string `yson:"original_path"`
	DestinationPath         *string `yson:"destination_path"`
	OriginalDestinationPath *string `yson:"original_destination_path"`
	SourcePath              *string `yson:"source_path"`
	OriginalSourcePath      *string `yson:"original_source_path"`
	Type                    *string `yson:"type"`
	Method                  *string `yson:"method"`
	User                    *string `yson:"user"`
	TransactionInfo         any     `yson:"transaction_info"`
	TransactionID           *string `yson:"transaction_id"`
}

var accessLogExportOutputSchema = schema.MustInfer(&AccessLogExportRow{})

// ExportFilters are the filters of an export with the semantics of the /access-log query builder.
type ExportFilters struct {
	UserRegex   string
	PathRegex   string
	MethodGroup ActionSelector
	Scope       ScopeSelector
	Metadata    bool
	MetadataSet bool
	UserType    UserTypeSelector
}

type AccessLogExportMapper struct {
	Path         string
	Begin        int64
	End          int64
	LogsTimezone string
	Filters      ExportFilters
}

func (m *AccessLogExportMapper) InputTypes() []any {
	return []any{AccessLogExportRow{}}
}

func (m *AccessLogExportMapper) OutputTypes() []any {
	return []any{AccessLogExportRow{}}
}

// unixSecondsFromInstant parses the instant written without a time zone by ALV preprocessing ("2006-01-02 15:04:05").
func unixSecondsFromInstant(instant string, logsLocation *time.Location) (int64, error) {
	parsedTime, err := time.ParseInLocation(time.DateTime, instant, logsLocation)
	if err != nil {
		return 0, fmt.Errorf("cannot parse access log instant %q: %w", instant, err)
	}
	return parsedTime.Unix(), nil
}

func classifyUserType(user string) UserType {
	for _, prefix := range robotUserPrefixes {
		if strings.HasPrefix(user, prefix) {
			return UserTypeRobot
		}
	}
	if containsArr(systemUsers, user) {
		return UserTypeSystem
	}
	return UserTypeHuman
}

func matchesUserTypeFilter(selector UserTypeSelector, user *string) bool {
	human, robot, system := selector[UserTypeHuman], selector[UserTypeRobot], selector[UserTypeSystem]
	if (!human && !robot && !system) || (human && robot && system) {
		return true
	}
	return user != nil && selector[classifyUserType(*user)]
}

func matchesMethodFilter(selector ActionSelector, method *string) bool {
	hasSelectedMethod := false
	for name, selected := range selector {
		if !selected {
			continue
		}
		hasSelectedMethod = true
		if method == nil {
			continue
		}
		if groupMethods, isGroup := methodGroups[name]; isGroup {
			if slices.Contains(groupMethods, *method) {
				return true
			}
		} else if name == *method {
			return true
		}
	}
	return !hasSelectedMethod
}

func matchesScopeFilter(selector ScopeSelector, objectType *string) bool {
	hasSelectedScope := false
	for _, selected := range selector {
		if selected {
			hasSelectedScope = true
			break
		}
	}
	if !hasSelectedScope {
		return true
	}
	if selector["other"] {
		// Every type except the known types of the scopes that are not selected.
		if objectType == nil {
			for knownScope := range objectTypes {
				if !selector[knownScope] {
					return false
				}
			}
			return true
		}
		knownScope, isKnownType := reverseObjectTypes[*objectType]
		return !isKnownType || selector[knownScope]
	}
	if objectType == nil {
		return false
	}
	if knownScope, isKnownType := reverseObjectTypes[*objectType]; isKnownType && selector[knownScope] {
		return true
	}
	return selector[*objectType]
}

// isPathInExportScope reports whether an access log path refers to the requested node, its attributes, or its
// descendants. The scope is a canonical path validated by the HTTP service. The master writes the resolved part of a
// path in the canonical escaping, so a prefix ending with "/" or "&" cannot match a sibling such as //a/bb for //a/b.
func isPathInExportScope(scope, path string) bool {
	return scope == "/" || path == scope || strings.HasPrefix(path, scope+"/") || strings.HasPrefix(path, scope+"&")
}

func (m *AccessLogExportMapper) Do(_ mapreduce.JobContext, input mapreduce.Reader, outputs []mapreduce.Writer) error {
	logsLocation, err := time.LoadLocation(m.LogsTimezone)
	if err != nil {
		return yterrors.Err("could not load the logs timezone", err)
	}
	userRegex, err := regexp.Compile(m.Filters.UserRegex)
	if err != nil {
		return yterrors.Err("invalid user_regex", err)
	}
	pathRegex, err := regexp.Compile(m.Filters.PathRegex)
	if err != nil {
		return yterrors.Err("invalid path_regex", err)
	}

	rowError := func(message string, args ...any) error {
		args = append(args, yterrors.Attr("table_index", input.TableIndex()), yterrors.Attr("row_index", input.RowIndex()))
		return yterrors.Err(append([]any{message}, args...)...)
	}
	for input.Next() {
		var row AccessLogExportRow
		if err := input.Scan(&row); err != nil {
			return rowError("could not read an access log row", err)
		}

		if row.Instant == nil {
			return rowError("access log row has no instant")
		}
		if row.Path == nil {
			return rowError("access log row has no path")
		}
		timestamp, err := unixSecondsFromInstant(*row.Instant, logsLocation)
		if err != nil {
			return rowError("access log row has an invalid instant", err)
		}
		if timestamp < m.Begin || timestamp >= m.End {
			continue
		}

		if !isPathInExportScope(m.Path, *row.Path) {
			continue
		}

		if m.Filters.UserRegex != "" && (row.User == nil || !userRegex.MatchString(*row.User)) {
			continue
		}
		if m.Filters.PathRegex != "" && !pathRegex.MatchString(*row.Path) {
			continue
		}
		if m.Filters.MetadataSet && strings.Contains(*row.Path, "@") != m.Filters.Metadata {
			continue
		}
		if !matchesMethodFilter(m.Filters.MethodGroup, row.Method) ||
			!matchesScopeFilter(m.Filters.Scope, row.Type) ||
			!matchesUserTypeFilter(m.Filters.UserType, row.User) {
			continue
		}

		if err := outputs[0].Write(&row); err != nil {
			return rowError("could not write an access log row to the output table", err)
		}
	}
	return nil
}

func init() {
	mapreduce.Register(&AccessLogExportMapper{})
}
