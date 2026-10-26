package proto

import "context"

type generationContextKey struct{}

// WithGeneration returns a context that carries the directory fencing token for
// direct in-process memnode calls that do not have an RPC envelope.
func WithGeneration(ctx context.Context, generation uint64) context.Context {
	return context.WithValue(ctx, generationContextKey{}, generation)
}

// GenerationFromContext returns the fencing token carried by ctx, if any.
func GenerationFromContext(ctx context.Context) (uint64, bool) {
	generation, ok := ctx.Value(generationContextKey{}).(uint64)
	return generation, ok
}
