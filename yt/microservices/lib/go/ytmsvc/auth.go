package ytmsvc

// AuthInfo describes who a request is executed on behalf of.
//
// UserLogin is the subject of the request; ServiceLogin identifies the calling
// service. Both may be set at once, when an authenticated service acts on behalf
// of an authenticated user. Either may be empty, but not both.
type AuthInfo struct {
	UserLogin    string `json:"user_login,omitempty"`
	ServiceLogin string `json:"service_login,omitempty"`
}

// ActingLogin returns the login the request is executed on behalf of: the user
// when one is present, the calling service otherwise.
func (a AuthInfo) ActingLogin() string {
	if a.UserLogin != "" {
		return a.UserLogin
	}
	return a.ServiceLogin
}
