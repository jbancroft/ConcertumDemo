# Repository guidelines

This is a small Go demo of distributed transactions, indexing, and SQL.
Prioritize clear code, correct demonstrated behavior, and easy local execution.

- Keep the demo self-contained and standard-library-only. Add dependencies,
  configuration, or abstractions only when the requested behavior requires them.

- Tests should catch plausible behavioral bugs. Assert observable results and
  invariants rather than reproducing the implementation. For transaction,
  storage, and recovery changes, cover relevant failure and boundary cases.

- Keep test-only helpers, accessors, and setup methods in `_test.go` files.
  Do not add exported production APIs solely for tests. Prefer same-package
  tests and private dependency injection; keep unavoidable production testing
  seams minimal and justify them.

- Maintain one current API, implementation, and data format. Add legacy or
  backward compatibility only when explicitly requested.

- Name constants for non-obvious protocol values, storage layouts, limits,
  and durations. Self-explanatory literals and simple example data may stay inline.

- Alphabetize names in `const` and `var` blocks, ignoring case, unless a comment
  at the top specifies another order.

- Consult docs/design.md when changing transaction semantics, storage formats,
  recovery, or index invariants. Keep deliberate limitations documented.

- For implementation requests, finish the in-scope changes and relevant local
  validation. Resolve routine implementation choices independently and report
  material assumptions.

- During development, use focused tests. Before finishing Go code changes,
  run make check; also run make race for concurrency changes and make demo
  when the demonstrated behavior changes. Repeat passing checks only when
  further changes or unresolved concerns justify it.

- Update README.md when demo commands, observable behavior, or limitations change.
  Finish with a concise account of what changed, verification, and remaining issues.
