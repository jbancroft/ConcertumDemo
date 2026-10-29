# Repository guidelines

- **No tautological tests.** Tests should catch plausible bugs, not repeat the
  implementation or verify trivial behavior.

- **No legacy or backward compatibility unless explicitly requested.** Maintain
  one current API, implementation, and data format.

- **No magic numbers.** Use named constants unless a literal is self-explanatory
  or has a clear reason to stay inline.
