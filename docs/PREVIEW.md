# Native Desktop Previews

`t3-boat-sentinel` can expose a dev server running inside a disposable chat VM through a per-`(sandbox, port)` HTTPS preview hostname. With `hub.preview.mode: desktop-proxy`, T3 Desktop opens that URL in its native Electron `<webview>` instead of rendering the page inside the VM and streaming JPEG frames back to the desktop.

This guide covers:

- the zero-extra-infrastructure Sentinel Preview Gateway,
- the optional stateless Cloudflare Edge Worker in [`workers/preview-edge/`](../workers/preview-edge/README.md),
- the preview security model,
- fallback server-browser stream tuning, and
- `t3.preinstallBrowser` for faster first agent browser opens.

## Why `mode: desktop-proxy`?

The default T3 `runtime: "server"` browser preview renders the page in the remote sandbox, captures JPEG screencast frames, sends them to the client, waits for acknowledgements, and forwards every click, keypress, scroll, and hover back to the VM. When the sandbox is in another continent, that stop-and-wait loop typically feels like about 4 FPS with 400-600 ms of click latency.

`desktop-proxy` moves rendering back to the machine that is already displaying the window:

- T3 Desktop's native Electron `<webview>` renders the dev server locally, so local compositing, scrolling, hover, and click feedback can run at 60-120 FPS with approximately 0 ms of additional input latency.
- Only real application traffic crosses the network: HTML, JS, CSS, images, API calls, and WebSockets. No video-like JPEG screencast crosses the VM-to-desktop path for Desktop previews.
- Hot Module Replacement works as a normal browser WebSocket, including Vite's `vite-hmr` subprotocol.
- Sentinel proxies bytes in memory and writes no preview cache or frame data to disk: 0 MB of preview disk usage on Sentinel.
- `exposePort(sandboxId, port)` is 100% lazy. VMs without open previews do zero preview exposure work at boot; the first authenticated preview request exposes only that sandbox's requested port.
- Preview state is isolated per `(sandbox, port)` hostname, so two chats never share cookies, `localStorage`, or Service Workers.

`desktop-proxy` is a **Hub-wide** setting optimized for T3 Desktop: it omits `capabilities.serverBrowser` from the Hub descriptor so T3 Desktop opens user preview tabs in its native Electron `<webview>`. Clients without a native desktop webview (such as T3 Mobile or web clients) should use `mode: server` (or a separate Hub) if they need interactive browser previews; server/agent-created tabs that explicitly carry `runtime: "server"` continue to stream over `/api/preview-stream/ws` and use the Hub stream tuning below (see [Fallback server stream tuning](#fallback-server-stream-tuning)).

## Configuration

```yaml
hub:
  host: boat.example.com
  label: Boat
  maxSandboxes: 20
  preview:
    mode: desktop-proxy        # "desktop-proxy" (recommended for Desktop) or "server" (default JPEG stream)
    # edgeWorkerHost: example.com
    # edgeWorkerSecret: ${PREVIEW_EDGE_SECRET}
    streamMaxWidth: 1280
    streamMaxHeight: 800
    streamQuality: 55
    coalesceInput: true
```

All `preview` keys are optional; `mode` defaults to `server`. `edgeWorkerHost` and `edgeWorkerSecret` must be set together. `streamMaxWidth`, `streamMaxHeight`, `streamQuality`, and `coalesceInput` tune only the fallback server-browser stream; they do not turn a native desktop `<webview>` into a JPEG stream.

Restart Sentinel after changing `sentinel.yaml`.

## Architecture

### Default: Sentinel Preview Gateway

```text
T3 Desktop Electron <webview>
  │  HTTPS + WSS
  │  https://p5173-<sandbox-slug>.example.com/...
  ▼
Cloudflare Tunnel wildcard (*.example.com)
  ▼
Sentinel HubPortGateway
  │  verifies __t3p / __t3_preview
  │  lazily calls exposePort(sandboxId, 5173) on first use
  ▼
Boat / sandbox-provider exposed endpoint
  ▼
dev server inside that chat's disposable VM
```

### Optional: Cloudflare Edge Worker

```text
First preview open (single domain, `edgeWorkerHost: example.com`):
T3 Desktop Electron <webview>
  │  https://boat.example.com/api/preview-port/bootstrap?p=5173&s=<slug>&to=/&__t3p=...
  ▼
Sentinel HubPortGateway (on hub.host)
  │  verifies __t3p, lazily exposes port 5173, seals upstream target as __t3e
  └── 302 redirect ──────────────────────────────┐
                                                 ▼
Steady state:                    https://p5173-<sandbox-slug>.example.com/...
T3 Desktop Electron <webview> ── HTTPS + WSS ──▶ nearest Cloudflare Edge Worker (*.example.com/*)
                                                  │  verifies __t3e / __t3_edge
                                                  ▼
                                        Boat / sandbox-provider endpoint
                                                  ▼
                                        dev server inside the chat VM
```

After the bootstrap redirect, preview HTTP and WebSocket traffic does not hairpin through Sentinel. One stateless Worker deployment serves every sandbox and every non-reserved dev-server port.

> **Cloudflare Worker route syntax:** Cloudflare Workers Routes only accept a leading hostname wildcard (`*.example.com/*`), **not** mid-label wildcards like `p*-*.example.com/*`.
>
> - **Option A (Dedicated preview zone — recommended for strongest SameSite isolation):** deploy the Worker on `*.preview-example.net/*` and set `edgeWorkerHost: preview-example.net`.
> - **Option B (Single domain):** deploy the Worker on `*.example.com/*`, set `edgeWorkerHost: example.com`, and add Cloudflare Dashboard **"Worker: None"** exclusion routes for your Sentinel hostnames (`boat.example.com/*`, `boat-ctrl.example.com/*`, `boat-llm.example.com/*`) so Hub RPC traffic bypasses the Worker directly. As a safety net, the Worker also passes non-preview hostnames through to origin, and Sentinel automatically hosts the initial `__t3p` bootstrap on `hub.host` (`/api/preview-port/bootstrap`).

## Quickstart: zero extra infrastructure

The Sentinel Gateway works with the wildcard Cloudflare Tunnel already used by Sentinel:

```text
*.example.com -> http://127.0.0.1:8080
```

Preview hostnames deliberately keep the port and sandbox slug in one DNS label:

```text
p5173-a1b2c3d4e5.example.com   # valid: one label below example.com
p5173.a1b2c3d4e5.example.com   # invalid for Cloudflare Free Universal SSL
```

Cloudflare Free Universal SSL covers `*.example.com`, not multi-level wildcards. `HubPortGateway` therefore emits the compatible `p<port>-<slug>.<domain>` form. The slug is the first 10 hex characters of the SHA-256 of the sandbox id.

Minimum configuration:

```yaml
hub:
  preview:
    mode: desktop-proxy
```

No extra Cloudflare Tunnel, DNS record, port exposure, or disk cache is required.

**Important Cloudflare Access note:** do not put a Cloudflare Access login page in front of `p*-*.example.com` (or the equivalent edge-worker pattern). Preview URLs already have their own zero-trust bootstrap and cookie authentication, and an Access interstitial would break the `<webview>`, HMR WebSockets, and cookie flow. Continue protecting `boat-ctrl.example.com` with Cloudflare Access as described in the main README.

## Optional: one stateless Cloudflare Worker for all sandboxes

[`workers/preview-edge/`](../workers/preview-edge/README.md) is an optional offload layer for preview HTTP and HMR WebSocket traffic. It is not required for `desktop-proxy`.

Properties:

- one Worker serves all sandboxes and all valid dev-server ports,
- no KV, Durable Objects, Containers, queues, caches, or file synchronization,
- HTTP and WebSocket proxying, including HMR,
- no Cloudflare API key or token is ever stored in Sentinel,
- production upstreams must be `https://`; plaintext `http://` upstreams require `ALLOW_HTTP_UPSTREAM = "1"` and are for local tests only,
- port `3773` is reserved for T3/Sentinel control and is rejected.

### Deploy the Worker

1. Edit [`workers/preview-edge/wrangler.toml`](../workers/preview-edge/wrangler.toml) and replace `example.com` with your Cloudflare zone (either a dedicated preview zone such as `preview-example.net`, or your existing domain `example.com` with Cloudflare Dashboard **"Worker: None"** exclusion routes for `boat.example.com/*`, `boat-ctrl.example.com/*`, and `boat-llm.example.com/*`):

   ```toml
   routes = [
     { pattern = "*.example.com/*", zone_name = "example.com" },
   ]
   ```

2. Generate a random 32-byte shared secret locally:

   ```sh
   openssl rand -hex 32
   ```

   Store the 64-character hex value in Sentinel's environment as `PREVIEW_EDGE_SECRET`; do not commit it.

3. Authenticate Wrangler once on the operator machine and deploy:

   ```sh
   cd workers/preview-edge
   bun install
   npx wrangler login
   npx wrangler deploy
   npx wrangler secret put PREVIEW_EDGE_SECRET
   npx wrangler deploy
   ```

   When `wrangler secret put` prompts, paste the same `PREVIEW_EDGE_SECRET` value used by Sentinel.

4. Configure Sentinel:

   ```yaml
   hub:
     preview:
       mode: desktop-proxy
       edgeWorkerHost: example.com
       edgeWorkerSecret: ${PREVIEW_EDGE_SECRET}
   ```

   Restart Sentinel after editing the file.

`PREVIEW_EDGE_SECRET` is a symmetric token-sealing secret, not a Cloudflare credential. The only Cloudflare authentication in this flow is the operator's local `npx wrangler login` session. Sentinel never needs a Cloudflare API key.

## Security model

Preview access is designed for ephemeral, per-chat dev servers rather than public hosting.

1. **Short-lived HMAC bootstrap (`__t3p`).** Sentinel derives a dedicated HMAC-SHA256 subkey from `controlSecret` and mints a short-lived bootstrap token containing the sandbox id, port, and expiration (default 15 minutes). The token must match the exact sandbox and port encoded in the preview hostname. Treat an unopened bootstrap link as a bearer secret until it expires. If a stale `__t3p` link is reopened while the browser already holds a valid session cookie, the gateway strips the stale parameter and redirects cleanly.
2. **Hardened session cookie.** Without the edge Worker, successful bootstrap sets `__t3_preview` with `Path=/; HttpOnly; SameSite=None; Secure; Max-Age=<session TTL>` (default 8 hours; `SameSite=Lax` when `secureCookies: false` in local tests) and returns a `302` redirect. With the edge Worker, the Worker verifies the sealed `__t3e` bootstrap and sets `__t3_edge` with `Path=/; HttpOnly; Secure; SameSite=None; Max-Age=<remaining token TTL>`, again followed by a `302`.
3. **Tokens leave the URL immediately.** The redirect collapses leading slashes (blocking `//` open redirects) and strips `__t3p` or `__t3e` from the query string before the app loads.
4. **Sealed edge targets.** `__t3e` is an AES-256-GCM-sealed JSON payload containing the upstream URL, optional upstream gate headers, sandbox id, port, exact expected preview hostname, and expiration. Tampering, expiration, wrong-secret decryption, and wrong-host replay are rejected.
5. **Per-`(sandbox, port)` cookie isolation.** Preview hostnames are host-only origins, and both the Sentinel Gateway and the edge Worker strip `Domain=` attributes from upstream `Set-Cookie` headers and drop reserved cookie names (`__t3_preview`, `__t3_edge`, `_port_auth`). A dev server therefore cannot set parent-domain cookies targeting `boat.example.com` or other chats. For full site-level (`SameSite`) isolation as well, deploy previews on a separate registrable domain (e.g. `preview-example.net`).
6. **Upstream gates stay hidden.** The gateway/Worker removes its own authentication cookie before forwarding, preserves application cookies, and merges provider gate cookies such as Boat's `_port_auth`. Boat credentials and provider gate headers are not exposed to the preview page.
7. **Revocation on lifecycle changes.** When a sandbox sleeps, parks, or is destroyed, Sentinel immediately rotates/clears its in-memory sandbox session epoch, invalidates slug and endpoint mappings, and closes its gateway WebSockets — so old 8-hour `__t3_preview` cookies cannot be reused even after the VM wakes later. The edge Worker is intentionally stateless, so its sealed token stays pinned to one exact upstream and hostname until it expires or the provider withdraws the upstream endpoint when the sandbox sleeps/destroys. Note that preview HTTP/WS traffic does not reset the VM watchdog's chat idle timer on its own.
8. **No indexing.** Both layers serve `/robots.txt` with `User-agent: *\nDisallow: /` and add `X-Robots-Tag: noindex, nofollow, noarchive` to normal responses.
9. **Frame headers are intentionally preview-compatible.** The edge Worker does not add `X-Frame-Options: DENY` or a restrictive Content Security Policy to proxied app responses because legitimate previews may render inside desktop application webviews. Do not use preview hostnames for sensitive production applications.

## Fallback server stream tuning

For `runtime: "server"` previews (including server/agent-created tabs), Sentinel's media proxy already avoids making every frame wait for a full desktop-to-VM round trip: it acknowledges each binary frame to the VM as soon as the frame reaches the Hub, then keeps the same one-frame-in-flight discipline toward the app. Newer frames supersede older held frames instead of building a backlog.

The optional `hub.preview` keys tune that fallback stream:

| Key | Example | Behavior |
|---|---:|---|
| `streamMaxWidth` | `1280` | Caps upstream screencast `maxWidth`. Width and height scale down together, preserving the client's aspect ratio; a smaller client is never upscaled. `0` or omitted disables the cap. |
| `streamMaxHeight` | `800` | Caps upstream screencast `maxHeight` using the same proportional scale as `streamMaxWidth`. `0` or omitted disables the cap. |
| `streamQuality` | `55` | JPEG quality cap in the range 1-100. It is injected when the client omitted quality and never raises a lower client-requested quality. `0` or omitted uses T3's default. |
| `coalesceInput` | `true` | Enables leading+trailing coalescing for high-frequency hover mouse moves and wheel input. Hover moves default to at most one send per 50 ms; wheel deltas accumulate for at most 35 ms. Pending input is flushed before clicks, keys, or other ordered input so it cannot be overtaken. |

Recommended starting point for transoceanic links:

```yaml
hub:
  preview:
    streamMaxWidth: 1280
    streamMaxHeight: 800
    streamQuality: 55
    coalesceInput: true
```

These settings trade a small amount of image sharpness and pointer granularity for much lower bandwidth and input traffic. They do not affect native desktop `<webview>` rendering.

## `t3.preinstallBrowser`

Server-run browser previews use T3's pinned `chrome-headless-shell`. On a fresh VM, downloading that browser and installing its system libraries can cost about 132 MB and a noticeable first-open delay.

To move that cost into the immutable base image, opt in under `t3:`:

```yaml
t3:
  preinstallBrowser: true
```

Then re-bake the base snapshot and every project snapshot that should inherit it:

```sh
bun run sentinel bake base
bun run sentinel bake <project>
```

During `bake base`, Sentinel runs T3's browser setup and opens a temporary local server-runtime preview tab until the pinned, checksum-verified browser is installed under `~/.t3/tools/chrome-headless-shell`. The bake-time userdata wipe intentionally keeps `~/.t3/tools`. The default is `false`; omitting the key preserves the existing download-on-first-use behavior.

## Homelab bonus: split-horizon DNS

If T3 Desktop and the Sentinel host are on the same LAN, the public Cloudflare path can be an unnecessary round trip even in `desktop-proxy` mode. A homelab can use split-horizon DNS so preview hostnames resolve directly to a LAN reverse proxy in front of Sentinel while off-site devices continue using Cloudflare.

If you do this:

- terminate TLS with a wildcard certificate trusted by your desktop devices,
- preserve the original `Host` header and WebSocket `Upgrade` headers,
- proxy to Sentinel's normal `listenHost`/`listenPort`,
- keep the preview HMAC/cookie authentication enabled, and
- do not bypass or weaken the Cloudflare Access rules for `controlHost`.

Split-horizon DNS is purely a network optimization. It does not change the preview security model, and it is not required for either the Sentinel Gateway or the edge Worker.
