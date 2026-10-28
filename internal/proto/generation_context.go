package proto

import "context"

type generationContextKey struct{}

// WithGeneration attaches the directory's fencing token to a direct node call,
// where there is no RPC message to carry it.
func WithGeneration(ctx context.Context, generation uint64) context.Context {
	return context.WithValue(ctx, generationContextKey{}, generation)
}

// GenerationFromContext reads the fencing token from ctx, if present.
func GenerationFromContext(ctx context.Context) (uint64, bool) {
	generation, ok := ctx.Value(generationContextKey{}).(uint64)
	return generation, ok
}
