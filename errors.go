package wisptrace

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound reports that a span or trace does not exist (or was deleted).
	ErrNotFound = errors.New("wisptrace: not found")
	// ErrInvalidSpan is wrapped by every span validation failure.
	ErrInvalidSpan = errors.New("wisptrace: invalid span")
	// ErrInvalidCursor reports a Query.Cursor that did not come from a Page.
	ErrInvalidCursor = errors.New("wisptrace: invalid cursor")
	// ErrClosed reports use of a DB after Close.
	ErrClosed = errors.New("wisptrace: database closed")
)

// ValidationError describes the first rule a span broke. It matches
// ErrInvalidSpan under errors.Is.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("wisptrace: invalid span: %s: %s", e.Field, e.Reason)
}

func (e *ValidationError) Is(target error) bool { return target == ErrInvalidSpan }
