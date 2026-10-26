# relayway

RelayWay proxy (`proxy/`, Rust) and a small control UI (`control-ui/`, Vite).

The proxy exposes a SOCKS5 listener and an HTTP API. It dials an upstream SOCKS proxy (`RELAYWAY_UPSTREAM_SOCKS_ADDR`, default `127.0.0.1:1080`).

The control UI talks to that API. Point it at a provider with:

```bash
VITE_PROVIDER_URL=http://127.0.0.1:7664
VITE_PROVIDER_SOCKS_PORT=10080
VITE_PROVIDER_API_PORT=18088
```

```bash
cd proxy && cargo test
cd control-ui && bun install && bun run build
```
