# Security Policy

## Supported Versions

Only the latest minor release line is supported. Right now that's the `0.9.x` series leading into `1.0`.

| Version | Supported |
|---------|-----------|
| 0.9.x   | yes       |
| 0.8.x   | security fixes only |
| < 0.8   | no        |

## Reporting a Vulnerability

Please **do not** open a public issue for security problems.

Email `tyrese@yeema.co` or reach out on Discord at https://discord.gg/inkwell. Include:

- A short description of the issue
- Steps to reproduce (or a minimal repro repo)
- The spacedb version (`exports.spacedb:stats()` includes it, or check `fxmanifest.lua`)
- Whether you've shared this with anyone else

You should get an acknowledgement within 72 hours. Fix targets:

- Critical (RCE, auth bypass, credential leak): patched within 7 days
- High (DoS, data exposure): patched within 14 days
- Everything else: next scheduled release

We'll credit reporters in the changelog unless you'd rather stay anonymous.

## Scope

In scope: the spacedb Lua bridge, the Node.js bridge, the Go core, and the three compat resources shipped in this repo.

Out of scope: bugs in user-written Lua, MariaDB / MySQL / Postgres themselves, the FiveM runtime, or anything reproduced by pointing spacedb at a hostile server you control.
