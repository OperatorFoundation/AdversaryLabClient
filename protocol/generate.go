//+build generate

package protocol
//go:generate codecgen -o codecs.go message.go
