# Draft patches against upstream teensyp

Held here **only** because this session could not push to the
`casselc/teensyp` fork (the git proxy declined to inject a credential for a
repository outside the session's authorized set). They belong on the fork, not
in this repo — drop this directory once they are pushed there.

Base: `weavejester/teensyp` @ `879da3519480b33cc4c0db3680337d99519ab534`,
which is also where `casselc/teensyp`'s `master` sits, so they apply cleanly.

| Patch | Branch | Findings |
| --- | --- | --- |
| `fix-buffer-bounds-0001.patch` | `claude/fix-buffer-bounds` | 3, 7 |
| `fix-reactor-robustness-0001.patch` | `claude/fix-reactor-robustness` | 1, 9 |

The two are independent and were verified separately; neither is based on the
other.

```sh
git checkout -b claude/fix-buffer-bounds 879da35
git am docs/upstream-patches/fix-buffer-bounds-0001.patch
```

**Not reviewed by a second model, and no pull request has been opened.**
See [`../UPSTREAM-TEENSYP-FINDINGS.md`](../UPSTREAM-TEENSYP-FINDINGS.md) for
the findings, and note that upstream's own suite is flaky in ~47% of runs, so
judge either patch against a multi-run interleaved baseline rather than one run.
