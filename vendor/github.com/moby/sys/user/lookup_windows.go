//go:build windows
// +build windows

package user

import (
	"errors"
	"io"
)

var errLookupNotSupported = errors.New("user lookup is not supported on windows")

func LookupUser(username string) (User, error) {
	return User{}, errLookupNotSupported
}

func LookupUid(uid int) (User, error) { //nolint:revive // keep compatibility with existing API
	return User{}, errLookupNotSupported
}

func LookupGroup(groupname string) (Group, error) {
	return Group{}, errLookupNotSupported
}

func LookupGid(gid int) (Group, error) {
	return Group{}, errLookupNotSupported
}

func GetPasswdPath() (string, error) {
	return "", errLookupNotSupported
}

func GetPasswd() (io.ReadCloser, error) {
	return nil, errLookupNotSupported
}

func GetGroupPath() (string, error) {
	return "", errLookupNotSupported
}

func GetGroup() (io.ReadCloser, error) {
	return nil, errLookupNotSupported
}

func CurrentUser() (User, error) {
	return User{}, errLookupNotSupported
}

func CurrentGroup() (Group, error) {
	return Group{}, errLookupNotSupported
}

func CurrentUserSubUIDs() ([]SubID, error) {
	return nil, errLookupNotSupported
}

func CurrentUserSubGIDs() ([]SubID, error) {
	return nil, errLookupNotSupported
}

func CurrentProcessUIDMap() ([]IDMap, error) {
	return nil, errLookupNotSupported
}

func CurrentProcessGIDMap() ([]IDMap, error) {
	return nil, errLookupNotSupported
}
