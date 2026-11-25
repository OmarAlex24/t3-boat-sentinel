# Preview Edge Worker

A single, stateless Cloudflare Worker that proxies **every sandbox dev-server preview** for `t3-boat-sentinel` directly from Cloudflare's edge.

- No Cloudflare API key or token is ever stored in Sentinel.
- No KV, Durable Objects, Containers, queues, caches, or file synchronization.
- One Worker deployment serves all sandboxes and all non-reserved dev-server ports.
- HTTP and WebSocket traffic (including Vite HMR's `vite-hmr` subprotocol) are proxied.
- Preview URLs are protected by short-lived, sealed bootstrap tokens and an `HttpOnly` cookie.

## How it works

Sentinel signs each preview target locally with the shared `PREVIEW_EDGE_SECRET`. The resulting token has this format:

```text
v1.<base64url(12-byte AES-GCM iv)>.<base64url(ciphertext + authentication tag)>
```

The plaintext is a JSON object containing the upstream base URL, optional upstream gate headers, sandbox id, port, expected preview hostname, and expiration time. Opening a preview link with `?__t3e=<token>` causes the Worker to verify the token, bind it to the requested hostname, store the untouched token in an `HttpOnly; Secure; SameSite=None` cookie, and redirect to a clean URL without the bootstrap parameter.

Subsequent requests use that cookie. The Worker removes its own `__t3_edge` cookie before forwarding, preserves application cookies, merges upstream gate cookies such as Boat's `_port_auth`, rewrites loopback redirects back to the preview hostname, and adds `X-Robots-Tag: noindex, nofollow, noarchive` to normal responses. It intentionally does **not** add `X-Frame-Options: DENY` or a restrictive Content Security Policy because previews may run inside desktop application webviews.

Port `3773` is reserved by Sentinel and is rejected by the token schema. Production Workers only proxy `https://` upstreams; plaintext `http://` upstreams require `ALLOW_HTTP_UPSTREAM = "1"`, which should only be used in local tests.

## Deploy

> Replace `example.com` in [`wrangler.toml`](./wrangler.toml) with a zone in your Cloudflare account before deploying.

```sh
cd workers/preview-edge
bun install
wrangler login
wrangler deploy
wrangler secret put PREVIEW_EDGE_SECRET
wrangler deploy
```

When prompted for `PREVIEW_EDGE_SECRET`, paste the same high-entropy secret Sentinel uses to seal preview targets. Generate one locally, for example:

```sh
openssl rand -hex 32
```

This secret is **not** a Cloudflare API key. The Cloudflare API credentials stay only in the operator's local `wrangler login` session; Sentinel needs only the symmetric token-sealing secret.

## Hostname and TLS shape

Cloudflare Workers Routes only allow a leading hostname wildcard (not mid-label wildcards like `p*-*.example.com/*`):

```text
*.example.com/*
```

- **Dedicated preview zone (recommended):** deploy on `*.preview-example.net/*` and set `hub.preview.edgeWorkerHost: preview-example.net`.
- **Single domain:** deploy on `*.example.com/*`, set `hub.preview.edgeWorkerHost: example.com`, and add Cloudflare Dashboard **"Worker: None"** exclusion routes for `boat.example.com/*`, `boat-ctrl.example.com/*`, and `boat-llm.example.com/*` so Sentinel RPC traffic bypasses the Worker directly (the Worker also passes non-preview hostnames through to origin as a safety net).

Cloudflare Universal SSL wildcard certificates cover exactly one hostname level, such as `*.example.com`. Therefore preview hostnames must put both the port and sandbox id in one DNS label:

```text
p5173-a1b2c3d4e5.example.com   # works with *.example.com
p5173.a1b2c3d4e5.example.com   # does not; this is a second-level wildcard
```

Sentinel's `HubPortGateway` is responsible for minting hostnames in the compatible single-label form.

## Test

From the repository root:

```sh
bun test workers/preview-edge/src/index.test.ts
```

Or from this directory:

```sh
bun test
```

The test suite exercises token sealing, bootstrap redirects, wrong-host replay rejection, tampered and expired token rejection, HTTPS-only enforcement, cookie merging, loopback `Location` rewriting, and WebSocket upgrade header forwarding.

## Local plaintext upstreams

Production deployments should leave `ALLOW_HTTP_UPSTREAM` unset. For local tests only, uncomment in `wrangler.toml` or place in `.dev.vars`:

```toml
[vars]
ALLOW_HTTP_UPSTREAM = "1"
```

Do not enable it in production: anyone holding a valid sealed token could otherwise point the Worker at a plaintext upstream.
