# Repository guidelines

- **No tautological tests.** Test meaningful behavior and realistic failure modes,
  not a restatement of the implementation. Skip tests that only verify trivial
  arithmetic, getters/setters, or standard-library guarantees. A test should
  catch a plausible bug in this project's logic; don't add one just for coverage.

- **No legacy or backward-compatibility modes unless explicitly requested.**
  Maintain one current implementation and format. Do not add compatibility
  shims, fallbacks, migrations, or support for older APIs or data formats unless
  the user explicitly asks for them.
