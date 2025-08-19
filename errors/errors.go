package errors

import (
	"errors"
	"fmt"

	"github.com/zhimiaox/zmqx-core/consts"
)

var (
	New = errors.New
	As  = errors.As
	Is  = errors.Is
	// Join = errors.Join
)

var (
	ErrMalformed = &Error{Code: consts.MalformedPacket}
	ErrProtocol  = &Error{Code: consts.ProtocolError}

	QueueClosed                   = New("queue has been closed")
	QueueDropExceedsMaxPacketSize = New("maximum packet size exceeded")
	QueueDropQueueFull            = New("the message queue is full")
	QueueDropExpired              = New("the message is expired")
	QueueDropExpiredInflight      = New("the inflight message is expired")
)

// Error wraps a MQTT reason code and error details.
type Error struct {
	// Code is the MQTT Reason Code
	Code consts.Code
	ErrorDetails
}

// ErrorDetails wraps reason string and user property for diagnostics.
type ErrorDetails struct {
	// ReasonString is the reason string field in property.
	// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901029
	ReasonString []byte
	// UserProperties is the user property field in property.
	UserProperties []struct {
		K []byte
		V []byte
	}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("operation error: Code = %x, reasonString: %s", e.Code, e.ReasonString)
}

func NewError(code consts.Code) *Error {
	return &Error{Code: code}
}

func Unwrap(err error) *Error {
	if err == nil {
		return nil
	}
	var ex *Error
	if As(err, &ex) {
		return ex
	}
	return &Error{
		Code: consts.UnspecifiedError,
		ErrorDetails: ErrorDetails{
			ReasonString: []byte(err.Error()),
		},
	}
}
