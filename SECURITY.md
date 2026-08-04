# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Cryptomeria, please report it by
emailing engineering@fibonsai.com. Do not disclose the issue publicly until it
has been addressed by the maintainers.

We will acknowledge receipt within 48 hours and provide an estimated timeline
for a fix. We appreciate your responsible disclosure.

## Scope

This policy covers the Cryptomeria platform, including all Rust and Python
code in this repository, as well as any deployment tooling and configuration.

## Best Practices

- **API keys, tokens, and credentials** must never be committed to the repository.
- Use `.env.local` for local secrets (excluded from version control).
- All secrets must be kept out of logs, error messages, and metrics.
