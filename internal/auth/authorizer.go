package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &authorizer{
		enforcer: enforcer,
	}
}

// Returns whether the given subject is permitted to run
// given action on the given object based on the configured
// model and policy
func (a *authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
