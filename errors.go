package mqtt

import (
	"errors"
)

var (
	// ErrConnect mqtt connection error
	ErrConnect = errors.New("connection failed")
	// ErrState unexpected runtime state
	ErrState = errors.New("invalid state")
	// ErrClient given mqtt client is not connected
	ErrClient = errors.New("client is not connected")
	// ErrTimeout operation timeout
	ErrTimeout = errors.New("operation timeout")
	// ErrSubscribe failed to subscribe to mqtt topic
	ErrSubscribe = errors.New("subscribe failure")
	// ErrConsumeToken consume token is invalid
	ErrConsumeToken = errors.New("invalid consume token")
	// ErrPublish publish to mqtt failed
	ErrPublish = errors.New("publish failure")
	ErrMandatoryTimeout = errors.New("client requires a timeout value")
	ErrMandatoryClientID = errors.New("client requires a clientID value")
	ErrMandatoryCleansess = errors.New("client requires a cleansess value")
	ErrMandatoryPassword = errors.New("client requires a password value")
	ErrMandatoryUser = errors.New("client requires a user value")
	ErrMandatoryServer = errors.New("client requires a server list")
)
