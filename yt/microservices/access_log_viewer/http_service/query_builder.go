package main

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	yaslices "go.ytsaurus.tech/library/go/slices"
)

var selectorsSubstiture = map[string]string{
	"transaction_info": "ConvertYson(transaction_info, 'text') as transaction_info",
	"instant":          "substr(instant, 1, 19) as instant",
	"user_type":        GetUserTypeSelector(),
	"method_group":     GetMethodGroupsSelector(),
	"scope":            GenerateSubstitutionMultiIf("type", "scope", reverseObjectTypes),
}

func getBeginDefault(request AccessLogRequest) time.Time {
	if int64(request.Begin) == 0 {
		return time.Now().Add(time.Duration(-12 * time.Hour))
	}
	return time.Unix(int64(request.Begin), 0)
}

func quotifyStr(s string) string {
	return fmt.Sprintf("'%s'", s)
}

func quotifyArr(arr []string) (result []string) {
	for _, s := range arr {
		result = append(result, quotifyStr(s))
	}
	return
}

func espaceClickHouseStr(s string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\b", "\\b", "\f", "\\f", "\r", "\\r", "\n", "\\n", "\t", "\\t", "\a", "\\a", "\v", "\\v", "'", "\\'")
	return replacer.Replace(s)
}

func containsArr(arr []string, val string) bool {
	for _, s := range arr {
		if s == val {
			return true
		}
	}
	return false
}

func GetSystemUserSelector() string {
	systemUsers := []string{
		"root",
		"system",
		"scheduler",
		"table_mount_informer",
		"yt-clickhouse-cache",
	}
	return fmt.Sprintf("(user IN (%s) as is_system_user)", strings.Join(quotifyArr(systemUsers), ","))
}

func GetRobotUserSelector() string {
	return "((startsWith(user, 'robot-') OR startsWith(user, 'zomb-')) as is_robot_user)"
}

func GetUserTypeSelector() string {
	return fmt.Sprintf("(multiIf(%s, 'robot', %s, 'system', 'human') as user_type)", GetRobotUserSelector(), GetSystemUserSelector())
}

func GetUserNameFilterClause(request AccessLogRequest) (where string, settings map[string]string, err error) {
	settings = make(map[string]string)
	var clause []string

	if selected, ok := request.UserType["human"]; ok && selected {
		if selected, ok := request.UserType["robot"]; !(ok && selected) {
			clause = append(clause, fmt.Sprintf("NOT %s", GetRobotUserSelector()))
		}
		if selected, ok := request.UserType["system"]; !(ok && selected) {
			clause = append(clause, fmt.Sprintf("NOT %s", GetSystemUserSelector()))
		}
	} else {
		var disjunctionClause []string
		if selected, ok := request.UserType["robot"]; ok && selected {
			disjunctionClause = append(disjunctionClause, GetRobotUserSelector())
		}
		if selected, ok := request.UserType["system"]; ok && selected {
			disjunctionClause = append(disjunctionClause, GetSystemUserSelector())
		}
		if disjunctionClause != nil {
			clause = append(clause, strings.Join(disjunctionClause, " OR "))
		}
	}

	if len(request.UserRegex) > 0 {
		_, err = regexp.Compile(request.UserRegex)
		if err != nil {
			return
		}
		//paramName := "user_regex"
		//clause = append(clause, fmt.Sprintf("match(user, {%s:String})", paramName))
		//settings[fmt.Sprintf("param_%s", paramName)] = request.UserRegex
		clause = append(clause, fmt.Sprintf("match(user, %s)", quotifyStr(request.UserRegex)))
	}

	if clause != nil {
		where = strings.Join(clause, " AND ")
	}
	return
}

func SliceToSet(arr []string) (result map[string]struct{}) {
	if arr == nil {
		return
	}
	result = make(map[string]struct{})
	for _, e := range arr {
		result[e] = struct{}{}
	}
	return
}

func GetTypeFilterClause(request AccessLogRequest) string {
	scopeArr := []string{}
	for scope, selected := range request.Scope {
		if selected {
			scopeArr = append(scopeArr, scope)
		}
	}
	if len(scopeArr) == 0 {
		return ""
	}

	if containsArr(scopeArr, "other") {
		scopeSet := SliceToSet(scopeArr)
		notSelected := []string{}
		for k := range objectTypes {
			if _, ok := scopeSet[k]; !ok {
				notSelected = append(notSelected, k)
			}
		}
		if len(notSelected) == 0 {
			return ""
		}
		return fmt.Sprintf("type NOT IN (%s)", strings.Join(quotifyArr(ReplaceInSlice(notSelected, objectTypes)), ", "))
	} else {
		return fmt.Sprintf("type IN (%s)", strings.Join(quotifyArr(ReplaceInSlice(scopeArr, objectTypes)), ", "))
	}
}

func GetMetadataScopeFilterClause(request AccessLogRequest) string {
	if request.Metadata == nil {
		return ""
	}
	if *request.Metadata {
		return "position(path, '@') != 0"
	} else {
		return "position(path, '@') == 0"
	}
}

func GenerateSubstitutionMultiIf(field string, alias string, substitutions map[string]string) string {
	ifParts := []string{}
	for key, value := range substitutions {
		if key != value {
			ifParts = append(ifParts, fmt.Sprintf("%s == '%s', '%s'", field, key, value))
		}
	}
	return fmt.Sprintf("multiIf(%s, %s) as %s", strings.Join(ifParts, ", "), field, alias)
}

func GetMethodGroupSelector(methodGroup string) string {
	includedMethods := make(map[string]struct{})
	for _, method := range methodGroups[methodGroup] {
		includedMethods[method] = struct{}{}
	}
	includedMethodsArr := []string{}
	for method := range includedMethods {
		includedMethodsArr = append(includedMethodsArr, quotifyStr(method))
	}
	sort.Strings(includedMethodsArr)
	return fmt.Sprintf("(method IN (%s) as is_%s)", strings.Join(includedMethodsArr, ", "), methodGroup)
}

func GetMethodGroupsSelector() string {
	knownMethodGroups := []string{}
	for methodGroup := range methodGroups {
		knownMethodGroups = append(knownMethodGroups, fmt.Sprintf("%s, '%s'", GetMethodGroupSelector(methodGroup), methodGroup))
	}
	return fmt.Sprintf("multiIf(%s, 'unknown') as method_group", strings.Join(knownMethodGroups, ", "))
}

func GetMethodFilterClause(request AccessLogRequest) string {
	if len(request.MethodGroup) == 0 {
		return ""
	}
	clauseParts := []string{}
	includedMethods := make(map[string]struct{})
	for methodGroup, selected := range request.MethodGroup {
		if !selected {
			continue
		}
		if _, ok := methodGroups[methodGroup]; ok {
			clauseParts = append(clauseParts, GetMethodGroupSelector(methodGroup))
		} else {
			includedMethods[methodGroup] = struct{}{}
		}
	}

	includedMethodsArr := []string{}
	for method := range includedMethods {
		includedMethodsArr = append(includedMethodsArr, quotifyStr(method))
	}
	if len(includedMethodsArr) > 0 {
		clauseParts = append(clauseParts, fmt.Sprintf("method IN (%s)", strings.Join(includedMethodsArr, ", ")))
	}
	if len(clauseParts) == 0 {
		return ""
	}
	return fmt.Sprintf("(%s)", strings.Join(clauseParts, " OR "))
}

func GetSubjectFilterClause(cluster string, subject string) string {
	return fmt.Sprintf("dictGetString('ACL', 'action', tuple('%s', '%s', path)) == 'allow'", cluster, subject)
}

func GetPathClause(request AccessLogRequest) (clause string, settings map[string]string, err error) {
	settings = make(map[string]string)
	if request.Path[len(request.Path)-1] == '/' {
		request.Path = request.Path[:len(request.Path)-1]
	}
	//paramName := "path"
	//settings[fmt.Sprintf("param_%s", paramName)] = request.Path
	//clause = fmt.Sprintf("path = {%s:String}", paramName)
	clause = fmt.Sprintf("path = '%s'", request.Path)
	if request.Recursive {
		//clause = fmt.Sprintf("(%s OR startsWith(path, {%s:String} || '/'))", clause, paramName)
		clause = fmt.Sprintf("(%s OR startsWith(path, '%s/'))", clause, request.Path)
	}
	if len(request.PathRegex) > 0 {
		_, err = regexp.Compile(request.PathRegex)
		if err != nil {
			return
		}
		//paramName = "path_regex"
		//settings[fmt.Sprintf("param_%s", paramName)] = request.PathRegex
		//clause = fmt.Sprintf("%s AND match(path, {%s:String})", clause, paramName)
		clause = fmt.Sprintf("%s AND match(path, '%s')", clause, request.PathRegex)
	}
	return
}

func GetInstantClause(request AccessLogRequest) (clause string) {
	clause = fmt.Sprintf("toDateTime(instant) >= toDateTime(%d)", int64(request.Begin))
	if int64(request.End) > 0 {
		clause = fmt.Sprintf("%s AND toDateTime(instant) < toDateTime(%d)", clause, int64(request.End))
	}
	return
}

func MergeMaps(dst map[string]string, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}

func GetWhereClause(subject string, request AccessLogRequest) (clause string, settings map[string]string, err error) {
	settings = make(map[string]string)
	userNameFilterClause, settingsUpd, err := GetUserNameFilterClause(request)
	if err != nil {
		return
	}
	MergeMaps(settings, settingsUpd)
	pathClause, settingsUpd, err := GetPathClause(request)
	if err != nil {
		return
	}
	MergeMaps(settings, settingsUpd)
	clauseParts := []string{
		userNameFilterClause,
		GetTypeFilterClause(request),
		GetMethodFilterClause(request),
		pathClause,
		GetInstantClause(request),
		GetMetadataScopeFilterClause(request),
		GetSubjectFilterClause(request.Cluster, subject),
	}
	clauseParts = yaslices.Filter(clauseParts, func(s string) bool {
		return s != ""
	})
	clause = "  " + strings.Join(clauseParts, " AND\n  ")
	return
}

func GetFromClause(request AccessLogRequest, snapshotRoot string) (clause string, settings map[string]string, err error) {
	settings = make(map[string]string)
	if int64(request.Begin) == 0 {
		err = fmt.Errorf("request begin must be greather than zero")
		return
	}
	if int64(request.End) > 0 {
		clause = fmt.Sprintf("  ytTables(ytListLogTables('%s/%s', toDateTime(%d), toDateTime(%d)))", snapshotRoot, request.Cluster, int64(request.Begin), int64(request.End))
	} else {
		clause = fmt.Sprintf("  ytTables(ytListLogTables('%s/%s', toDateTime(%d)))", snapshotRoot, request.Cluster, int64(request.Begin))
	}
	return
}

func selectFieldsClauseCount(request AccessLogRequest) string {
	return "count() as count"
}

func ReplaceInSlice(arr []string, replacements map[string]string) (result []string) {
	if arr == nil {
		return
	}
	result = []string{}
	for _, val := range arr {
		if replacement, ok := replacements[val]; ok {
			result = append(result, replacement)
		} else {
			result = append(result, val)
		}
	}
	return
}

func GetSelectFieldsClauseGet(request AccessLogRequest) string {
	fields := AlwaysSelectedFields
	selectorValue := reflect.ValueOf(&request.FieldSelector).Elem()
	selectorType := selectorValue.Type()
	for i := 0; i < selectorValue.NumField(); i++ {
		f := selectorValue.Field(i)
		if f.Bool() {
			fields = append(fields, selectorType.Field(i).Tag.Get("json"))
		}
	}
	selectors := ReplaceInSlice(fields, selectorsSubstiture)
	return "  " + strings.Join(selectors, ",\n  ")
}

func GetQuery(subject string, request AccessLogRequest, accessMasterLogRoot string, index, limit uint32) (query string, settings map[string]string, err error) {
	settings = make(map[string]string)
	settings["priority"] = GetQueryPriority()
	//settings["max_result_rows"] = QueryTrackerRowCountLimitStr

	request.Path = espaceClickHouseStr(request.Path)
	request.PathRegex = espaceClickHouseStr(request.PathRegex)
	request.UserRegex = espaceClickHouseStr(request.UserRegex)

	fromClause, settingsUpd, err := GetFromClause(request, accessMasterLogRoot)
	if err != nil {
		return
	}
	MergeMaps(settings, settingsUpd)
	whereClause, settingsUpd, err := GetWhereClause(subject, request)
	if err != nil {
		return
	}
	MergeMaps(settings, settingsUpd)
	query = fmt.Sprintf("SELECT\n%s\nFROM\n%s\nWHERE\n%s\nORDER BY instant", GetSelectFieldsClauseGet(request), fromClause, whereClause)
	if request.DistinctBy == "user" {
		query = query + " LIMIT 1 BY user"
	} else {
		query = query + " DESC"
	}

	if limit > 0 {
		offset := index * limit
		query = fmt.Sprintf("SELECT * FROM (\n%s\n) LIMIT %d, %d", query, offset, limit)
	}

	return
}

func GetVisibleTimeRangeQuery(accessMasterLogRoot string, cluster string) (query string, settings map[string]string) {
	settings = make(map[string]string)
	query = fmt.Sprintf("SELECT toInt64(min(toDateTime($key))) as earliest, toInt64(max(toDateTime($key))) as latests FROM ytListLogTables('%s/%s')", accessMasterLogRoot, cluster)
	return
}
