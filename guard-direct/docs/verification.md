# Verification of this reproduction package

Before publication, the following checks were run locally:

- Real `repo init` using this XML manifest, followed by SSH `repo sync` of vLLM, KVCR, AIPerf and Dynamo; all four HEADs match `config/pins.json` and tracked files are clean.
- PyPI metadata lookup of both exact Dynamo wheel filenames and SHA256 hashes: both were available and matched.
- The stdlib CPU suite under `tests/`: source/XML lock agreement, single-RNIC/AgentX/cache command contracts, offline pod rendering, PID-reuse protection, graceful owned-child termination, live-service collection refusal, archive content/hash verification, frozen analysis hashes, and successful/failed closed-capture receipts.
- Python syntax compilation of the new scripts and copied canaries.
- All four analysis entry points on an empty raw-data fixture: execution succeeds while the reports explicitly mark measured evidence incomplete. This checks module packaging and refusal to invent benchmark populations, not measured performance.

The repository's pre-existing `tests/run.sh` suite was also checked against both the untouched baseline `e414261` and this change. Both have the same existing failures: the repository-contract test has four stale expectations (the `cuda-env-fixes` manifest revision and three README patterns); the build-all mock fails because its vLLM fixture is not a Git repository; and the test-all mock exits 1 after its printed checks. The bootstrap and environment-verification mocks pass in both trees. These legacy scripts/configs were left unchanged.

No Kubernetes pod was allocated or modified, no model was loaded, and no new AgentX GPU replay was performed while packaging this workflow. Hardware/native correctness gates and the full replay run when an operator executes `prepare`/`all` in the documented image/pod. Passing local package checks is not a claim that a new measured reproduction has completed.
