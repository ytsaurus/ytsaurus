package tvm

import (
	"net/url"
	"strings"
)

type CheckTicketV2Options struct{ Params url.Values }

type CheckTicketV2Option func(CheckTicketV2Options)

func WithEnvOverride(env BlackboxEnv) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		opts.Params["override_env"] = []string{env.String()}
	}
}

func WithAllRequiredServiceRoles(requiredServiceRoles []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredServiceRoles) == 0 {
			return
		}
		opts.Params["required_service_roles"] = []string{strings.Join(requiredServiceRoles, ",")}
	}
}

func WithOneOfRequiredServiceRoles(requiredServiceRoles []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredServiceRoles) == 0 {
			return
		}
		opts.Params["required_service_roles"] = []string{strings.Join(requiredServiceRoles, ",")}
		opts.Params["service_roles_op"] = []string{"or"}
	}
}

func WithAllRequiredUserRoles(requiredUserRoles []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredUserRoles) == 0 {
			return
		}
		opts.Params["required_user_roles"] = []string{strings.Join(requiredUserRoles, ",")}
	}
}

func WithOneOfRequiredUserRoles(requiredUserRoles []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredUserRoles) == 0 {
			return
		}
		opts.Params["required_user_roles"] = []string{strings.Join(requiredUserRoles, ",")}
		opts.Params["user_roles_op"] = []string{"or"}
	}
}

func WithAllRequiredUserScopes(requiredUserScopes []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredUserScopes) == 0 {
			return
		}
		opts.Params["required_user_scopes"] = []string{strings.Join(requiredUserScopes, ",")}
	}
}

func WithOneOfRequiredUserScopes(requiredUserScopes []string) CheckTicketV2Option {
	return func(opts CheckTicketV2Options) {
		if len(requiredUserScopes) == 0 {
			return
		}
		opts.Params["required_user_scopes"] = []string{strings.Join(requiredUserScopes, ",")}
		opts.Params["user_scopes_op"] = []string{"or"}
	}
}
