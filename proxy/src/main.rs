use anyhow::{anyhow, Context, Result};
use axum::extract::{Path, State};
use axum::http::{header, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::env;
use std::io as stdio;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Clone)]
struct Config {
    provider_id: String,
    socks_addr: String,
    api_addr: String,
    provider_server: String,
    provider_port: u16,
    provider_password: String,
    provider_name_prefix: String,
    relayway_socks: String,
    health_check_urls: Vec<HealthCheckUrl>,
    relayway_base_url: String,
    relayway_token: String,
    relayway_admin_pass: String,
    relayway_token_file: PathBuf,
    relayway_mode: String,
    switch_delay: Duration,
    connect_ttl: Duration,
    connect_timeout: Duration,
    dial_timeout: Duration,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    max_active_conns: usize,
    debug_health_checks: bool,
    debug_proxy_logs: bool,
    subscription_ttl: Duration,
    watchdog_interval: Duration,
    startup_timeout: Duration,
    airport_id: String,
    client_version: String,
}

#[derive(Clone, Serialize)]
struct HealthCheckUrl {
    raw: String,
    host_port: String,
    path: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct Subscription {
    nodes: Vec<NfNode>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct NfNode {
    #[serde(default, deserialize_with = "string_or_default")]
    id: String,
    #[serde(default, deserialize_with = "string_or_default")]
    name: String,
    #[serde(default, deserialize_with = "string_or_default")]
    protocol: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct ProbeResult {
    #[serde(default, deserialize_with = "string_or_default")]
    id: String,
    ok: bool,
    rtt_ms: Option<i64>,
    warm_ms: Option<i64>,
    #[serde(default, deserialize_with = "string_or_default")]
    error: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct NfCloudStatus {
    connected: bool,
    #[serde(default, deserialize_with = "string_or_default")]
    active_mode: String,
    #[serde(default, deserialize_with = "string_or_default")]
    active_node_id: String,
    #[serde(default, deserialize_with = "string_or_default")]
    active_node_name: String,
    up_bytes: i64,
    down_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    up_human: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    down_human: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct RelayHealth {
    ok: bool,
    #[serde(default, deserialize_with = "string_or_default")]
    service: String,
    #[serde(default, deserialize_with = "string_or_default")]
    version: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct BootstrapResponse {
    code: i64,
    data: Option<BootstrapInfo>,
    message: Value,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct BootstrapInfo {
    #[serde(default, deserialize_with = "string_or_default")]
    service_id: String,
    #[serde(default, deserialize_with = "string_or_default")]
    service_name: String,
    #[serde(default, deserialize_with = "string_or_default")]
    panel_type: String,
    api_domains: Vec<String>,
    dns_list: Vec<String>,
    subscription: BootstrapSubscription,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct BootstrapSubscription {
    #[serde(default, deserialize_with = "string_or_default")]
    ua: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct UnlockResponse {
    ok: bool,
    #[serde(default, deserialize_with = "string_or_default")]
    token: String,
    #[serde(default)]
    user: Option<Value>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ErrorEnvelope {
    ok: Option<bool>,
    #[serde(default, deserialize_with = "string_or_default")]
    error: String,
}

fn string_or_default<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn is_session_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("session") || lower.contains("无效或已过期") || lower.contains("expired")
}

struct AppStateInner {
    token: String,
    last_token_refresh: Option<SystemTime>,
    active_node: String,
    last_switch_at: Option<SystemTime>,
    last_switch_mono: Option<Instant>,
    last_error: String,
    relay_ready: bool,
    relay_health_ok: bool,
    relay_socks_ok: bool,
    last_watchdog_at: Option<SystemTime>,
    last_relay_health: Option<RelayHealth>,
    last_relay_status: Option<NfCloudStatus>,
    last_bootstrap: Option<BootstrapInfo>,
    session_cookie: String,
    consumer_user: Option<Value>,
    debug_health_checks: bool,
    debug_proxy_logs: bool,
    subscription_cache: Option<Subscription>,
    subscription_cached: Option<Instant>,
}

struct App {
    cfg: Config,
    started_at: Instant,
    http: reqwest::Client,
    state: Mutex<AppStateInner>,
    route_lock: Mutex<()>,
    token_lock: Mutex<()>,
    active_sem: Arc<Semaphore>,
    logs: LogBuffer,
    switches: AtomicU64,
    connections: AtomicU64,
    failed_switches: AtomicU64,
    failed_dials: AtomicU64,
    failed_handshakes: AtomicU64,
    active_conns: AtomicU64,
    peak_conns: AtomicU64,
    rejected_conns: AtomicU64,
    client_to_up_bytes: AtomicU64,
    up_to_client_bytes: AtomicU64,
    watchdog_checks: AtomicU64,
    watchdog_failures: AtomicU64,
}

type SharedApp = Arc<App>;

#[derive(Clone)]
struct LogBuffer(Arc<Mutex<VecDeque<String>>>, usize);

impl LogBuffer {
    fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(VecDeque::new())), limit)
    }

    async fn push(&self, line: String) {
        let mut lines = self.0.lock().await;
        lines.push_back(line);
        while lines.len() > self.1 {
            lines.pop_front();
        }
    }

    async fn lines(&self) -> Vec<String> {
        self.0.lock().await.iter().cloned().collect()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let log_buffer = LogBuffer::new(500);
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .with_target(false)
        .with_writer(LogWriter::new(log_buffer.clone()))
        .init();

    let cfg = load_config();
    let app = Arc::new(App {
        started_at: Instant::now(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?,
        active_sem: Arc::new(Semaphore::new(cfg.max_active_conns)),
        logs: log_buffer,
        state: Mutex::new(AppStateInner {
            token: cfg.relayway_token.clone(),
            last_token_refresh: None,
            active_node: String::new(),
            last_switch_at: None,
            last_switch_mono: None,
            last_error: String::new(),
            relay_ready: false,
            relay_health_ok: false,
            relay_socks_ok: false,
            last_watchdog_at: None,
            last_relay_health: None,
            last_relay_status: None,
            last_bootstrap: None,
            session_cookie: String::new(),
            consumer_user: None,
            debug_health_checks: cfg.debug_health_checks,
            debug_proxy_logs: cfg.debug_proxy_logs,
            subscription_cache: None,
            subscription_cached: None,
        }),
        route_lock: Mutex::new(()),
        token_lock: Mutex::new(()),
        switches: AtomicU64::new(0),
        connections: AtomicU64::new(0),
        failed_switches: AtomicU64::new(0),
        failed_dials: AtomicU64::new(0),
        failed_handshakes: AtomicU64::new(0),
        active_conns: AtomicU64::new(0),
        peak_conns: AtomicU64::new(0),
        rejected_conns: AtomicU64::new(0),
        client_to_up_bytes: AtomicU64::new(0),
        up_to_client_bytes: AtomicU64::new(0),
        watchdog_checks: AtomicU64::new(0),
        watchdog_failures: AtomicU64::new(0),
        cfg,
    });

    app.log_startup();
    tokio::spawn({
        let app = app.clone();
        async move {
            if let Err(err) = wait_for_relayway(app).await {
                warn!("relayway startup wait failed: {err:#}");
            }
        }
    });
    tokio::spawn(watch_relayway(app.clone()));
    tokio::spawn(serve_api(app.clone()));
    serve_socks5(app).await
}

fn load_config() -> Config {
    let max_active = env_usize_any(&["RELAYWAY_MAX_ACTIVE_CONNECTIONS"], 512).max(1);
    Config {
        provider_id: env_default("RELAYWAY_PROVIDER_ID", "relayway"),
        socks_addr: normalize_bind_addr(&env_default("RELAYWAY_SOCKS_ADDR", ":10080")),
        api_addr: normalize_bind_addr(&env_default("RELAYWAY_API_ADDR", ":18088")),
        provider_server: env_default("RELAYWAY_PROVIDER_SERVER", "relayway-proxy"),
        provider_port: env_u16_any(&["RELAYWAY_PROVIDER_PORT"], 10080),
        provider_password: env_default("RELAYWAY_PROVIDER_PASSWORD", "relayway"),
        provider_name_prefix: env_default("RELAYWAY_PROVIDER_NAME_PREFIX", "RELAYWAY"),
        relayway_socks: env_default_any(&["RELAYWAY_RELAYWAY_SOCKS_ADDR", "RELAYWAY_UPSTREAM_SOCKS_ADDR"], "127.0.0.1:1080"),
        health_check_urls: parse_health_check_urls(&env_default("RELAYWAY_HEALTH_CHECK_URLS", "http://cp.cloudflare.com/generate_204,http://www.gstatic.com/generate_204")),
        relayway_base_url: env_default("RELAYWAY_BASE_URL", "http://127.0.0.1:7654").trim_end_matches('/').to_string(),
        relayway_token: env_default("RELAYWAY_TOKEN", ""),
        relayway_admin_pass: env_default("RELAYWAY_ADMIN_PASSWORD", ""),
        relayway_token_file: PathBuf::from(env_default("RELAYWAY_TOKEN_FILE", "/data/token")),
        relayway_mode: env_default("RELAYWAY_MODE", "global"),
        switch_delay: Duration::from_millis(env_u64_any(&["RELAYWAY_SWITCH_DELAY_MS"], 250)),
        connect_ttl: Duration::from_secs(env_u64_any(&["RELAYWAY_CONNECT_TTL_SECONDS"], 0)),
        connect_timeout: Duration::from_secs(env_u64_any(&["RELAYWAY_CONNECT_TIMEOUT_SECONDS"], 8)),
        dial_timeout: Duration::from_secs(env_u64_any(&["RELAYWAY_DIAL_TIMEOUT_SECONDS"], 20)),
        handshake_timeout: Duration::from_secs(env_u64_any(&["RELAYWAY_HANDSHAKE_TIMEOUT_SECONDS"], 15)),
        idle_timeout: Duration::from_secs(env_u64_any(&["RELAYWAY_IDLE_TIMEOUT_SECONDS"], 300)),
        max_active_conns: max_active,
        debug_health_checks: env_bool_any(&["RELAYWAY_DEBUG_HEALTH_CHECKS"], false),
        debug_proxy_logs: env_bool_any(&["RELAYWAY_DEBUG_PROXY_LOGS"], false),
        subscription_ttl: Duration::from_secs(env_u64_any(&["RELAYWAY_SUBSCRIPTION_TTL_SECONDS"], 30)),
        watchdog_interval: Duration::from_secs(env_u64_any(&["RELAYWAY_WATCHDOG_INTERVAL_SECONDS"], 30)),
        startup_timeout: Duration::from_secs(env_u64_any(&["RELAYWAY_STARTUP_TIMEOUT_SECONDS"], 90)),
        airport_id: env_default("RELAYWAY_CORED_AIRPORT_ID", ""),
        client_version: env_default("RELAYWAY_CLIENT_VERSION", "1.4.10"),
    }
}

fn parse_health_check_urls(raw: &str) -> Vec<HealthCheckUrl> {
    raw.split(',')
        .filter_map(|item| {
            let raw = item.trim();
            if raw.is_empty() {
                return None;
            }
            let (scheme, without_scheme) = raw.split_once("://").map(|(scheme, rest)| (scheme.to_ascii_lowercase(), rest)).unwrap_or_else(|| ("http".to_string(), raw));
            let (host, path) = without_scheme.split_once('/').map(|(h, p)| (h, format!("/{p}"))).unwrap_or((without_scheme, "/".to_string()));
            let default_port = if scheme == "https" { 443 } else { 80 };
            let host_port = if host.contains(':') { host.to_lowercase() } else { format!("{}:{default_port}", host.to_lowercase()) };
            Some(HealthCheckUrl { raw: raw.to_string(), host_port, path })
        })
        .collect()
}

fn env_default(key: &str, fallback: &str) -> String {
    env::var(key).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| fallback.to_string())
}

fn env_default_any(keys: &[&str], fallback: &str) -> String {
    keys.iter().find_map(|key| env::var(key).ok().filter(|value| !value.is_empty())).unwrap_or_else(|| fallback.to_string())
}

fn env_u64_any(keys: &[&str], fallback: u64) -> u64 {
    keys.iter().find_map(|key| env::var(key).ok().and_then(|value| value.parse().ok())).unwrap_or(fallback)
}

fn env_usize_any(keys: &[&str], fallback: usize) -> usize {
    keys.iter().find_map(|key| env::var(key).ok().and_then(|value| value.parse().ok())).unwrap_or(fallback)
}

fn env_u16_any(keys: &[&str], fallback: u16) -> u16 {
    keys.iter().find_map(|key| env::var(key).ok().and_then(|value| value.parse().ok())).unwrap_or(fallback)
}

fn env_bool_any(keys: &[&str], fallback: bool) -> bool {
    keys.iter().find_map(|key| match env::var(key).ok().map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on") => Some(true),
        Some(value) if matches!(value.as_str(), "0" | "false" | "no" | "off") => Some(false),
        _ => None,
    }).unwrap_or(fallback)
}

fn normalize_bind_addr(value: &str) -> String {
    if let Some(port) = value.strip_prefix(':') {
        return format!("0.0.0.0:{port}");
    }
    value.to_string()
}

impl App {
    fn log_startup(&self) {
        info!("relayway-proxy starting provider={}", self.cfg.provider_id);
        info!("role: exposes RelayWay as a socks5 provider provider={}", self.cfg.provider_id);
        info!("listen: socks={} api={} provider={}:{} relayway_socks={}", self.cfg.socks_addr, self.cfg.api_addr, self.cfg.provider_server, self.cfg.provider_port, self.cfg.relayway_socks);
        info!("relayway: base_url={} desired_mode={} watchdog_interval={:?} startup_timeout={:?}", self.cfg.relayway_base_url, self.cfg.relayway_mode, self.cfg.watchdog_interval, self.cfg.startup_timeout);
        if !self.cfg.relayway_mode.is_empty() && self.cfg.relayway_mode != "global" {
            warn!("RELAYWAY_MODE={} is not 'global'; global is recommended for provider usage", self.cfg.relayway_mode);
        }
        info!("relay: max_active={} idle_timeout={:?} handshake_timeout={:?}", self.cfg.max_active_conns, self.cfg.idle_timeout, self.cfg.handshake_timeout);
    }
}

async fn serve_api(app: SharedApp) {
    let listener = match TcpListener::bind(&app.cfg.api_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("api bind failed addr={}: {err}", app.cfg.api_addr);
            return;
        }
    };
    info!("api listening on {}", app.cfg.api_addr);
    let router = Router::new()
        .route("/", get(root))
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/debug", get(debug_get).post(debug_post))
        .route("/logs", get(logs))
        .route("/relayway/status", get(relayway_status))
        .route("/relayway/bootstrap", get(relayway_bootstrap))
        .route("/consumer/overview", get(consumer_overview))
        .route("/nodes", get(nodes))
        .route("/probes", get(probes))
        .route("/provider.yaml", get(provider_yaml))
        .route("/probe/:node", get(probe))
        .route("/egress", get(egress))
        .route("/connect/:node", post(connect))
        .with_state(app);
    if let Err(err) = axum::serve(listener, router).await {
        error!("api server failed: {err}");
    }
}

async fn root(State(app): State<SharedApp>) -> Json<Value> {
    Json(json!({
        "service": "relayway-proxy",
        "provider_id": app.cfg.provider_id,
        "endpoints": ["/healthz", "/status", "/debug", "/logs", "/relayway/status", "/relayway/bootstrap", "/consumer/overview", "/nodes", "/probes", "/provider.yaml", "/probe/:node", "/egress", "/connect/:node"]
    }))
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn status(State(app): State<SharedApp>) -> Json<Value> {
    Json(app.status().await)
}

async fn debug_get(State(app): State<SharedApp>) -> Json<Value> {
    let state = app.state.lock().await;
    Json(json!({ "proxy_logs": state.debug_proxy_logs, "health_checks": state.debug_health_checks }))
}

#[derive(Deserialize)]
struct DebugRequest {
    proxy_logs: Option<bool>,
    health_checks: Option<bool>,
}

async fn debug_post(State(app): State<SharedApp>, Json(req): Json<DebugRequest>) -> Json<Value> {
    let mut state = app.state.lock().await;
    if let Some(value) = req.proxy_logs {
        state.debug_proxy_logs = value;
    }
    if let Some(value) = req.health_checks {
        state.debug_health_checks = value;
    }
    info!("debug logging updated: proxy_logs={} health_check_logs={}", state.debug_proxy_logs, state.debug_health_checks);
    Json(json!({ "proxy_logs": state.debug_proxy_logs, "health_checks": state.debug_health_checks }))
}

async fn logs(State(app): State<SharedApp>, axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>) -> Response {
    let lines = app.logs.lines().await;
    if query.get("format").map(|value| value == "text").unwrap_or(false) {
        ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], lines.join("\n") + "\n").into_response()
    } else {
        Json(json!({ "lines": lines })).into_response()
    }
}

#[derive(Clone)]
struct LogWriter {
    buffer: LogBuffer,
}

impl LogWriter {
    fn new(buffer: LogBuffer) -> Self {
        Self { buffer }
    }
}

impl<'a> MakeWriter<'a> for LogWriter {
    type Writer = LogWriterGuard;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriterGuard {
            buffer: self.buffer.clone(),
            line: Vec::new(),
        }
    }
}

struct LogWriterGuard {
    buffer: LogBuffer,
    line: Vec<u8>,
}

impl stdio::Write for LogWriterGuard {
    fn write(&mut self, buf: &[u8]) -> stdio::Result<usize> {
        stdio::stderr().write_all(buf)?;
        self.line.extend_from_slice(buf);
        while let Some(pos) = self.line.iter().position(|b| *b == b'\n') {
            let chunk: Vec<u8> = self.line.drain(..=pos).collect();
            let text = String::from_utf8_lossy(&chunk).trim_end().to_string();
            if !text.is_empty() {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let buffer = self.buffer.clone();
                    handle.spawn(async move {
                        buffer.push(text).await;
                    });
                }
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> stdio::Result<()> {
        stdio::stderr().flush()
    }
}

async fn relayway_status(State(app): State<SharedApp>) -> Result<Json<NfCloudStatus>, AppError> {
    let status: NfCloudStatus = app.do_relayway_json(Method::GET, "/consumer/status", None).await?;
    Ok(Json(with_human_bytes(status)))
}

async fn relayway_bootstrap(State(app): State<SharedApp>) -> Result<Json<BootstrapInfo>, AppError> {
    Ok(Json(app.bootstrap().await?))
}

async fn consumer_overview(State(app): State<SharedApp>) -> Json<Value> {
    let plans = app.consumer_query_value("plan.list", json!({})).await;
    let notices = app.consumer_query_value("notice.list", json!({})).await;
    let devices = app.consumer_query_value("device.list", json!({ "page": 1, "page_size": 1000 })).await;
    let update = app.consumer_query_value("app.check_update", json!({ "platform": "openwrt", "version_code": 10411, "os_version": "Web" })).await;
    let user = app.consumer_user_value().await;
    let subscription = app.consumer_subscription_value().await;

    Json(json!({
        "plans": plans,
        "notices": notices,
        "devices": devices,
        "update": update,
        "user": user,
        "subscription": subscription,
    }))
}

async fn nodes(State(app): State<SharedApp>) -> Result<Json<Subscription>, AppError> {
    Ok(Json(app.subscription().await?))
}

async fn probes(State(app): State<SharedApp>) -> Result<Json<Value>, AppError> {
    let sub = app.subscription().await?;
    let mut results = Vec::with_capacity(sub.nodes.len());
    for node in sub.nodes {
        let probe = app.probe_rtt(&node.id).await;
        results.push(match probe {
            Ok(result) => json!({
                "id": node.id,
                "name": node.name,
                "protocol": node.protocol,
                "ok": result.ok,
                "rtt_ms": result.rtt_ms,
                "warm_ms": result.warm_ms,
                "error": result.error,
            }),
            Err(err) => json!({
                "id": node.id,
                "name": node.name,
                "protocol": node.protocol,
                "ok": false,
                "rtt_ms": null,
                "warm_ms": null,
                "error": err.to_string(),
            }),
        });
    }
    Ok(Json(json!({ "results": results })))
}

async fn provider_yaml(State(app): State<SharedApp>) -> Result<Response, AppError> {
    let sub = app.subscription().await?;
    let mut body = String::from("proxies:\n");
    for node in sub.nodes {
        let name = if node.name.is_empty() { node.id.clone() } else { node.name.clone() };
        let provider_name = if app.cfg.provider_name_prefix.is_empty() { name } else { format!("{} {name}", app.cfg.provider_name_prefix) };
        body.push_str(&format!("  - name: {provider_name:?}\n"));
        body.push_str("    type: socks5\n");
        body.push_str(&format!("    server: {}\n", app.cfg.provider_server));
        body.push_str(&format!("    port: {}\n", app.cfg.provider_port));
        body.push_str(&format!("    username: {:?}\n", node.id));
        body.push_str(&format!("    password: {:?}\n", app.cfg.provider_password));
        body.push_str("    udp: false\n");
    }
    Ok(([(header::CONTENT_TYPE, "application/yaml")], body).into_response())
}

async fn probe(State(app): State<SharedApp>, Path(node): Path<String>) -> Result<Json<ProbeResult>, AppError> {
    Ok(Json(app.probe_rtt(&node).await?))
}

async fn connect(State(app): State<SharedApp>, Path(node): Path<String>) -> Result<Json<Value>, AppError> {
    app.connect_node(&node).await?;
    Ok(Json(app.status().await))
}

async fn egress(State(app): State<SharedApp>) -> Result<Json<Value>, AppError> {
    let _guard = app.route_lock.lock().await;
    let before: NfCloudStatus = app.do_relayway_json(Method::GET, "/consumer/status", None).await?;
    let info: Value = app.http.get("https://ipinfo.io/json").send().await?.error_for_status()?.json().await?;
    let after: NfCloudStatus = app.do_relayway_json(Method::GET, "/consumer/status", None).await?;
    if !before.active_node_id.is_empty() && !after.active_node_id.is_empty() && before.active_node_id != after.active_node_id {
        warn!("egress lookup changed active node from {} to {}; restoring", before.active_node_id, after.active_node_id);
        app.connect_node_locked(&before.active_node_id).await?;
    }
    Ok(Json(json!({ "active_before": with_human_bytes(before), "active_after": with_human_bytes(after), "ipinfo": info })))
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(value: E) -> Self {
        AppError(value.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_GATEWAY, self.0.to_string()).into_response()
    }
}

impl App {
    async fn status(&self) -> Value {
        let state = self.state.lock().await;
        json!({
            "active_node": state.active_node,
            "provider_id": self.cfg.provider_id,
            "health_check_urls": self.cfg.health_check_urls.iter().map(|item| item.raw.clone()).collect::<Vec<_>>(),
            "has_token": !state.token.is_empty(),
            "last_token_refresh": state.last_token_refresh,
            "last_switch_at": state.last_switch_at,
            "last_error": state.last_error,
            "switches": self.switches.load(Ordering::Relaxed),
            "connections": self.connections.load(Ordering::Relaxed),
            "active_connections": self.active_conns.load(Ordering::Relaxed),
            "peak_connections": self.peak_conns.load(Ordering::Relaxed),
            "rejected_connections": self.rejected_conns.load(Ordering::Relaxed),
            "client_to_up_bytes": self.client_to_up_bytes.load(Ordering::Relaxed),
            "client_to_up_human": format_bytes(self.client_to_up_bytes.load(Ordering::Relaxed)),
            "up_to_client_bytes": self.up_to_client_bytes.load(Ordering::Relaxed),
            "up_to_client_human": format_bytes(self.up_to_client_bytes.load(Ordering::Relaxed)),
            "failed_switches": self.failed_switches.load(Ordering::Relaxed),
            "failed_dials": self.failed_dials.load(Ordering::Relaxed),
            "failed_handshakes": self.failed_handshakes.load(Ordering::Relaxed),
            "watchdog_checks": self.watchdog_checks.load(Ordering::Relaxed),
            "watchdog_failures": self.watchdog_failures.load(Ordering::Relaxed),
            "relay_ready": state.relay_ready,
            "relay_health_ok": state.relay_health_ok,
            "relay_socks_ok": state.relay_socks_ok,
            "last_watchdog_at": state.last_watchdog_at,
            "relay_health": state.last_relay_health,
            "relayway_status": state.last_relay_status,
            "bootstrap": state.last_bootstrap,
            "debug_proxy_logs": state.debug_proxy_logs,
            "debug_health_checks": state.debug_health_checks,
            "uptime_seconds": self.started_at.elapsed().as_secs(),
            "relayway_mode": self.cfg.relayway_mode,
        })
    }

    async fn do_json<T: for<'de> Deserialize<'de>>(&self, method: Method, url: String, payload: Option<Value>) -> Result<T> {
        let mut last_err = None;
        for attempt in 0..3 {
            if attempt > 0 {
                sleep(Duration::from_millis(attempt * 200)).await;
            }
            let mut req = self.http.request(method.clone(), &url).header("Accept", "application/json").header("User-Agent", format!("RelayWay/{} relayway-proxy", self.cfg.client_version));
            if let Some(payload) = &payload {
                req = req.json(payload);
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.bytes().await?;
                    if matches!(status.as_u16(), 502 | 503 | 504) {
                        last_err = Some(anyhow!("temporary upstream status {status}: {}", String::from_utf8_lossy(&body).trim()));
                        continue;
                    }
                    if !status.is_success() {
                        return Err(anyhow!("{} failed: {status}: {}", url, String::from_utf8_lossy(&body).trim()));
                    }
                    return Ok(serde_json::from_slice(&body)?);
                }
                Err(err) => last_err = Some(err.into()),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("request failed")))
    }

    async fn do_relayway_json<T: for<'de> Deserialize<'de>>(&self, method: Method, path: &str, payload: Option<Value>) -> Result<T> {
        let mut last_err = None;
        for _ in 0..2 {
            let token = self.get_token().await?;
            let url = format!("{}{}", self.cfg.relayway_base_url, path);
            let mut req = self.http.request(method.clone(), &url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Accept", "application/json")
                .header("User-Agent", format!("RelayWay/{} relayway-proxy", self.cfg.client_version));
            let session_cookie = self.state.lock().await.session_cookie.clone();
            if !session_cookie.is_empty() {
                req = req.header(header::COOKIE, session_cookie);
            }
            if let Some(payload) = &payload {
                req = req.json(payload);
            }
            let resp = req.send().await?;
            let status = resp.status();
            let body = resp.bytes().await?;
            if status == StatusCode::UNAUTHORIZED {
                self.invalidate_token().await;
                last_err = Some(anyhow!("relayway {path} failed: {status}: {}", String::from_utf8_lossy(&body).trim()));
                continue;
            }
            if !status.is_success() {
                return Err(anyhow!("relayway {path} failed: {status}: {}", String::from_utf8_lossy(&body).trim()));
            }
            if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(&body) {
                if envelope.ok == Some(false) && !envelope.error.is_empty() {
                    let err = anyhow!("relayway {path} returned error: {}", envelope.error);
                    let lower = envelope.error.to_ascii_lowercase();
                    if lower.contains("token") || lower.contains("unauthorized") {
                        self.invalidate_token().await;
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
            return Ok(serde_json::from_slice(&body)?);
        }
        Err(last_err.unwrap_or_else(|| anyhow!("relayway {path} failed")))
    }

    async fn consumer_query_value(&self, action: &str, params: Value) -> Value {
        let payload = json!({ "action": action, "params": params });
        match self.do_relayway_json::<Value>(Method::POST, "/consumer/query", Some(payload.clone())).await {
            Ok(value) => value,
            Err(err) if is_session_error(&err.to_string()) => {
                self.invalidate_token().await;
                let _ = self.get_token().await;
                match self.do_relayway_json::<Value>(Method::POST, "/consumer/query", Some(payload)).await {
                    Ok(value) => value,
                    Err(err) => json!({ "ok": false, "action": action, "error": err.to_string() }),
                }
            }
            Err(err) => json!({ "ok": false, "action": action, "error": err.to_string() }),
        }
    }

    async fn consumer_subscription_value(&self) -> Value {
        match self.do_relayway_json::<Value>(Method::GET, "/consumer/subscription", None).await {
            Ok(value) => value,
            Err(err) if is_session_error(&err.to_string()) => {
                self.invalidate_token().await;
                let _ = self.get_token().await;
                self.do_relayway_json::<Value>(Method::GET, "/consumer/subscription", None).await
                    .unwrap_or_else(|err| json!({ "ok": false, "error": err.to_string() }))
            }
            Err(err) => json!({ "ok": false, "error": err.to_string() }),
        }
    }

    async fn consumer_user_value(&self) -> Value {
        if let Some(user) = self.state.lock().await.consumer_user.clone() {
            return user;
        }
        match self.refresh_token_locked().await {
            Ok(_) => self.state.lock().await.consumer_user.clone().unwrap_or_else(|| json!({ "ok": false, "error": "unlock did not return user" })),
            Err(err) => json!({ "ok": false, "error": err.to_string() }),
        }
    }

    async fn get_token(&self) -> Result<String> {
        let token = self.state.lock().await.token.clone();
        if !token.is_empty() {
            return Ok(token);
        }
        let _guard = self.token_lock.lock().await;
        let token = self.state.lock().await.token.clone();
        if !token.is_empty() {
            return Ok(token);
        }
        if let Ok(data) = fs::read_to_string(&self.cfg.relayway_token_file).await {
            let token = data.trim().to_string();
            if !token.is_empty() {
                self.state.lock().await.token = token.clone();
                return Ok(token);
            }
        }
        self.refresh_token_locked().await
    }

    async fn invalidate_token(&self) {
        let mut state = self.state.lock().await;
        state.token.clear();
        state.session_cookie.clear();
        state.consumer_user = None;
        drop(state);
        let _ = fs::remove_file(&self.cfg.relayway_token_file).await;
    }

    async fn refresh_token_locked(&self) -> Result<String> {
        if self.cfg.relayway_admin_pass.is_empty() {
            return Err(anyhow!("RELAYWAY_ADMIN_PASSWORD is required to refresh token"));
        }
        let url = format!("{}/consumer/unlock", self.cfg.relayway_base_url);
        let response = self.http.post(url)
            .header("Accept", "application/json")
            .json(&json!({ "admin_password": self.cfg.relayway_admin_pass }))
            .send().await?
            .error_for_status()?;
        let session_cookie = response.headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .filter_map(|value| value.split(';').next())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("; ");
        let response: UnlockResponse = response.json().await?;
        if !response.ok || response.token.is_empty() {
            return Err(anyhow!("relayway unlock did not return a token"));
        }
        if let Some(parent) = self.cfg.relayway_token_file.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        let _ = fs::write(&self.cfg.relayway_token_file, &response.token).await;
        let mut state = self.state.lock().await;
        state.token = response.token.clone();
        state.session_cookie = session_cookie;
        state.consumer_user = response.user.clone();
        state.last_token_refresh = Some(SystemTime::now());
        info!("relayway token refreshed");
        Ok(response.token)
    }

    async fn relay_health(&self) -> Result<RelayHealth> {
        let url = format!("{}/api/v1/health", self.cfg.relayway_base_url);
        let health: RelayHealth = self.do_json(Method::GET, url, None).await?;
        if !health.ok {
            return Err(anyhow!("relayway health returned not ok"));
        }
        Ok(health)
    }

    async fn bootstrap(&self) -> Result<BootstrapInfo> {
        let url = format!("{}/mw/api/v2/bootstrap", self.cfg.relayway_base_url);
        let mut last_err = None;
        for attempt in 0..3 {
            if attempt > 0 { sleep(Duration::from_millis(attempt * 200)).await; }
            match self.http.get(&url)
                .header("Accept", "application/json")
                .header("User-Agent", format!("RelayWay/{} relayway-proxy", self.cfg.client_version))
                .header("x-airport-id", &self.cfg.airport_id)
                .header("x-client-version", &self.cfg.client_version)
                .header("x-device-id", "")
                .send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.bytes().await?;
                    if !status.is_success() {
                        last_err = Some(anyhow!("bootstrap failed: {status}: {}", String::from_utf8_lossy(&body)));
                        continue;
                    }
                    let bootstrap: BootstrapResponse = serde_json::from_slice(&body)?;
                    return bootstrap.data.ok_or_else(|| anyhow!("bootstrap failed code={} message={}", bootstrap.code, bootstrap.message));
                }
                Err(err) => last_err = Some(err.into()),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("bootstrap failed")))
    }

    async fn subscription(&self) -> Result<Subscription> {
        {
            let state = self.state.lock().await;
            if let (Some(cache), Some(cached_at)) = (&state.subscription_cache, state.subscription_cached) {
                if cached_at.elapsed() < self.cfg.subscription_ttl {
                    return Ok(cache.clone());
                }
            }
        }
        let sub: Subscription = self.do_relayway_json(Method::GET, "/consumer/subscription", None).await?;
        let mut state = self.state.lock().await;
        state.subscription_cache = Some(sub.clone());
        state.subscription_cached = Some(Instant::now());
        Ok(sub)
    }

    async fn probe_rtt(&self, node_id: &str) -> Result<ProbeResult> {
        let _guard = self.route_lock.lock().await;
        let _ = self.subscription().await?;
        self.do_relayway_json(Method::POST, "/consumer/probe-rtt", Some(json!({ "id": node_id }))).await
    }

    async fn connect_node(&self, node_id: &str) -> Result<()> {
        let _guard = self.route_lock.lock().await;
        self.connect_node_locked(node_id).await
    }

    async fn connect_node_locked(&self, node_id: &str) -> Result<()> {
        let _ = self.subscription().await?;
        {
            let state = self.state.lock().await;
            if !self.cfg.connect_ttl.is_zero() && state.active_node == node_id && state.last_switch_mono.map(|t| t.elapsed() < self.cfg.connect_ttl).unwrap_or(false) {
                return Ok(());
            }
        }
        let connect = self.do_relayway_json::<Value>(Method::POST, "/consumer/connect", Some(json!({ "id": node_id, "mode": self.cfg.relayway_mode }))).await;
        if let Err(err) = connect {
            self.record_error("switch", &err).await;
            return Err(anyhow!("relayway connect {node_id} failed: {err}"));
        }
    let mut state = self.state.lock().await;
    state.active_node = node_id.to_string();
    state.last_switch_at = Some(SystemTime::now());
    state.last_switch_mono = Some(Instant::now());
    state.last_error.clear();
    self.switches.fetch_add(1, Ordering::Relaxed);
    drop(state);
        if !self.cfg.switch_delay.is_zero() {
            sleep(self.cfg.switch_delay).await;
        }
        info!("active node: {node_id}");
        Ok(())
    }

    async fn record_error(&self, kind: &str, err: &anyhow::Error) {
        match kind {
            "switch" => { self.failed_switches.fetch_add(1, Ordering::Relaxed); }
            "dial" => { self.failed_dials.fetch_add(1, Ordering::Relaxed); }
            "handshake" => { self.failed_handshakes.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
        self.state.lock().await.last_error = err.to_string();
    }

    async fn should_log_proxy(&self) -> bool { self.state.lock().await.debug_proxy_logs }
    async fn should_log_health_checks(&self) -> bool { self.state.lock().await.debug_health_checks }
}

async fn wait_for_relayway(app: SharedApp) -> Result<()> {
    if app.cfg.startup_timeout.is_zero() {
        return reconcile_relayway(app, ReconcileOpts::full()).await;
    }
    let deadline = Instant::now() + app.cfg.startup_timeout;
    loop {
        match timeout(Duration::from_secs(10), reconcile_relayway(app.clone(), ReconcileOpts::startup())).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(err)) => {
                if Instant::now() > deadline {
                    return Err(err);
                }
                warn!("waiting for relayway: {err:#}");
            }
            Err(_) => {
                if Instant::now() > deadline {
                    return Err(anyhow!("relayway startup wait timed out"));
                }
                warn!("waiting for relayway: reconcile exceeded 10s (relayway bootstrap/status may be slow)");
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn watch_relayway(app: SharedApp) {
    if app.cfg.watchdog_interval.is_zero() { return; }
    loop {
        sleep(app.cfg.watchdog_interval).await;
        if let Err(err) = timeout(Duration::from_secs(20), reconcile_relayway(app.clone(), ReconcileOpts::full())).await.unwrap_or_else(|_| Err(anyhow!("watchdog timed out"))) {
            warn!("relayway watchdog failed: {err}");
        }
    }
}

async fn reconcile_relayway(app: SharedApp, opts: ReconcileOpts) -> Result<()> {
    let health = match app.relay_health().await {
        Ok(health) => health,
        Err(err) => {
            record_watchdog(&app, false, None, false, None, None, Some(err.to_string())).await;
            return Err(err);
        }
    };
    let socks_ok = check_relayway_socks(&app.cfg.relayway_socks, Duration::from_secs(2)).await;
    let bootstrap = if opts.include_bootstrap {
        match app.bootstrap().await {
            Ok(bootstrap) => Some(bootstrap),
            Err(err) => {
                warn!("relayway bootstrap check failed: {err:#}");
                None
            }
        }
    } else {
        None
    };
    let status: NfCloudStatus = match app.do_relayway_json(Method::GET, "/consumer/status", None).await {
        Ok(status) => status,
        Err(err) => {
            record_watchdog(&app, false, Some(health), socks_ok, None, bootstrap, Some(err.to_string())).await;
            return Err(err);
        }
    };
    if !app.cfg.relayway_mode.trim().is_empty() && !status.active_mode.is_empty() && status.active_mode != app.cfg.relayway_mode {
        if !status.active_node_id.is_empty() {
            warn!("relayway mode drift detected: active_mode={} desired={} active_node={}; reconciling", status.active_mode, app.cfg.relayway_mode, status.active_node_id);
            app.connect_node(&status.active_node_id).await?;
        }
    }
    let desired_node = app.state.lock().await.active_node.clone();
    if !desired_node.is_empty() && !status.active_node_id.is_empty() && status.active_node_id != desired_node {
        warn!(
            "relayway active node drift detected: active_node={} desired_node={}; reconciling",
            status.active_node_id,
            desired_node,
        );
        app.connect_node(&desired_node).await?;
    }
    let ready = health.ok && socks_ok;
    record_watchdog(&app, ready, Some(health), socks_ok, Some(status), bootstrap, None).await;
    Ok(())
}

struct ReconcileOpts {
    include_bootstrap: bool,
}

impl ReconcileOpts {
    fn full() -> Self {
        Self { include_bootstrap: true }
    }
    fn startup() -> Self {
        Self { include_bootstrap: false }
    }
}

async fn record_watchdog(app: &App, ready: bool, health: Option<RelayHealth>, socks_ok: bool, status: Option<NfCloudStatus>, bootstrap: Option<BootstrapInfo>, err: Option<String>) {
    app.watchdog_checks.fetch_add(1, Ordering::Relaxed);
    if err.is_some() { app.watchdog_failures.fetch_add(1, Ordering::Relaxed); }
    let mut state = app.state.lock().await;
    state.relay_ready = ready;
    state.relay_health_ok = health.as_ref().map(|h| h.ok).unwrap_or(false);
    state.relay_socks_ok = socks_ok;
    state.last_watchdog_at = Some(SystemTime::now());
    state.last_relay_health = health;
    state.last_relay_status = status.map(with_human_bytes);
    state.last_bootstrap = bootstrap;
    state.last_error = err.unwrap_or_default();
}

async fn check_relayway_socks(addr: &str, dur: Duration) -> bool {
    timeout(dur, TcpStream::connect(addr)).await.map(|r| r.is_ok()).unwrap_or(false)
}

async fn serve_socks5(app: SharedApp) -> Result<()> {
    let listener = TcpListener::bind(&app.cfg.socks_addr).await?;
    info!("socks5 listening on {}", app.cfg.socks_addr);
    loop {
        let (conn, _) = listener.accept().await?;
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_socks5(app.clone(), conn).await {
                debug!("socks connection ended: {err:#}");
            }
        });
    }
}

struct ActivePermit {
    app: SharedApp,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ActivePermit {
    fn drop(&mut self) {
        self.app.active_conns.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_active(app: &SharedApp) -> Option<ActivePermit> {
    match app.active_sem.clone().try_acquire_owned() {
        Ok(permit) => {
            let active = app.active_conns.fetch_add(1, Ordering::Relaxed) + 1;
            loop {
                let peak = app.peak_conns.load(Ordering::Relaxed);
                if active <= peak || app.peak_conns.compare_exchange(peak, active, Ordering::Relaxed, Ordering::Relaxed).is_ok() { break; }
            }
            Some(ActivePermit { app: app.clone(), _permit: permit })
        }
        Err(_) => {
            app.rejected_conns.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

async fn handle_socks5(app: SharedApp, conn: TcpStream) -> Result<()> {
    let _ = conn.set_nodelay(true);
    let handshake = async {
        let mut br = BufReader::new(conn);
        let node_id = socks_auth(&mut br, &app.cfg.provider_password).await.context("socks auth failed")?;
        let target = match socks_request(&mut br).await {
            Ok(target) => target,
            Err(SocksError::UnsupportedCommand(command)) => {
                send_socks_reply(br.get_mut(), 0x07).await?;
                return Err(anyhow!("unsupported SOCKS command 0x{command:02x}"));
            }
            Err(err) => return Err(anyhow!(err.to_string())),
        };
        Ok::<_, anyhow::Error>((br, node_id, target))
    };
    let (br, node_id, target) = if !app.cfg.handshake_timeout.is_zero() {
        timeout(app.cfg.handshake_timeout, handshake).await??
    } else {
        handshake.await?
    };
    relay_socks5_after_handshake(app, br, node_id, target).await
}

async fn relay_socks5_after_handshake(app: SharedApp, mut br: BufReader<TcpStream>, node_id: String, target: String) -> Result<()> {
    let mut initial_client_bytes = Vec::new();
    let mut socks_reply_sent = false;
    match maybe_handle_health_check(app.clone(), &mut br, &node_id, &target).await? {
        HealthCheckDecision::Pass => {}
        HealthCheckDecision::Handled => return Ok(()),
        HealthCheckDecision::Proxy { buffered_headers } => {
            initial_client_bytes = buffered_headers;
            socks_reply_sent = true;
        }
    }
    let mut conn = br.into_inner();
    let route_guard = app.route_lock.lock().await;
    if let Err(err) = timeout(app.cfg.connect_timeout, app.connect_node_locked(&node_id)).await.unwrap_or_else(|_| Err(anyhow!("connect timeout"))) {
        warn!("switch failed node={} target={}: {err}", node_id, target);
        send_socks_reply(&mut conn, 0x01).await?;
        return Ok(());
    }
    let _permit = match acquire_active(&app).await {
        Some(permit) => permit,
        None => {
            warn!("socks overloaded node={} target={} active_limit={}", node_id, target, app.cfg.max_active_conns);
            send_socks_reply(&mut conn, 0x01).await?;
            return Ok(());
        }
    };
    let mut upstream = match dial_socks5(&app.cfg.relayway_socks, &target, app.cfg.dial_timeout).await {
        Ok(stream) => stream,
        Err(err) => {
            app.record_error("dial", &err).await;
            warn!("relayway socks dial failed node={} target={} upstream={}: {err}", node_id, target, app.cfg.relayway_socks);
            send_socks_reply(&mut conn, 0x05).await?;
            return Ok(());
        }
    };
    drop(route_guard);
    if !socks_reply_sent {
        send_socks_reply(&mut conn, 0x00).await?;
    }
    if !initial_client_bytes.is_empty() {
        upstream.write_all(&initial_client_bytes).await?;
    }
    app.connections.fetch_add(1, Ordering::Relaxed);
    let log_proxy = app.should_log_proxy().await;
    if log_proxy { info!("proxy node={} target={}", node_id, target); }
    let result = relay_with_idle_timeout(conn, upstream, app.cfg.idle_timeout).await;
    match result {
        Ok((client_to_up, up_to_client)) => {
            let client_to_up = client_to_up + initial_client_bytes.len() as u64;
            app.client_to_up_bytes.fetch_add(client_to_up, Ordering::Relaxed);
            app.up_to_client_bytes.fetch_add(up_to_client, Ordering::Relaxed);
            if log_proxy { info!("proxy done node={} target={} client_to_upstream={} upstream_to_client={}", node_id, target, client_to_up, up_to_client); }
        }
        Err(err) => debug!("proxy relay ended node={} target={}: {err}", node_id, target),
    }
    Ok(())
}

async fn relay_with_idle_timeout(mut a: TcpStream, mut b: TcpStream, idle_timeout: Duration) -> io::Result<(u64, u64)> {
    let mut a_to_b = 0u64;
    let mut b_to_a = 0u64;
    let mut buf_a = vec![0u8; 32 * 1024];
    let mut buf_b = vec![0u8; 32 * 1024];
    let mut last_activity = Instant::now();
    let mut a_open = true;
    let mut b_open = true;
    loop {
        if !a_open && !b_open {
            return Ok((a_to_b, b_to_a));
        }
        if !idle_timeout.is_zero() && last_activity.elapsed() >= idle_timeout {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout"));
        }
        tokio::select! {
            read = a.read(&mut buf_a), if a_open => {
                let n = read?;
                if n == 0 {
                    a_open = false;
                    let _ = b.shutdown().await;
                    continue;
                }
                b.write_all(&buf_a[..n]).await?;
                a_to_b += n as u64;
                last_activity = Instant::now();
            }
            read = b.read(&mut buf_b), if b_open => {
                let n = read?;
                if n == 0 {
                    b_open = false;
                    let _ = a.shutdown().await;
                    continue;
                }
                a.write_all(&buf_b[..n]).await?;
                b_to_a += n as u64;
                last_activity = Instant::now();
            }
            _ = sleep(Duration::from_millis(200)), if !idle_timeout.is_zero() => {}
        }
    }
}

enum HealthCheckDecision {
    Pass,
    Handled,
    Proxy { buffered_headers: Vec<u8> },
}

async fn maybe_handle_health_check(app: SharedApp, br: &mut BufReader<TcpStream>, node_id: &str, target: &str) -> Result<HealthCheckDecision> {
    let exact_health_target = health_target_host_matches(&app.cfg.health_check_urls, target);
    let possible_resolved_health_target = is_possible_resolved_health_check_target(target);
    let possible_tls_health_target = is_possible_tls_health_check_target(&app.cfg.health_check_urls, target);
    if app.cfg.health_check_urls.is_empty() || (!exact_health_target && !possible_resolved_health_target && !possible_tls_health_target) {
        return Ok(HealthCheckDecision::Pass);
    }
    send_socks_reply(br.get_mut(), 0x00).await?;
    if possible_tls_health_target {
        let record = match timeout(Duration::from_secs(15), read_tls_client_hello_record(br, 16 * 1024)).await {
            Ok(Ok(record)) => record,
            Ok(Err(err)) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(HealthCheckDecision::Handled),
            Ok(Err(err)) => {
                debug!("TLS health-check sniff failed node={} target={}: {err}", node_id, target);
                return Ok(HealthCheckDecision::Handled);
            }
            Err(_) => return Ok(HealthCheckDecision::Handled),
        };
        if tls_sni_matches_health_check(&app.cfg.health_check_urls, target, &record) {
            match timeout(Duration::from_secs(20), app.probe_rtt(node_id)).await {
                Ok(Ok(probe)) if probe.ok => {
                    if app.should_log_health_checks().await {
                        info!("TLS health-check probe ok node={} target={} sni={:?} rtt_ms={:?} warm_ms={:?}", node_id, target, tls_client_hello_sni(&record), probe.rtt_ms, probe.warm_ms);
                    }
                    return Ok(HealthCheckDecision::Handled);
                }
                Ok(Ok(probe)) => {
                    warn!("TLS health-check probe unhealthy node={} target={} error={}", node_id, target, probe.error);
                    return Ok(HealthCheckDecision::Handled);
                }
                Ok(Err(err)) => {
                    warn!("TLS health-check probe failed node={} target={}: {err}", node_id, target);
                    return Ok(HealthCheckDecision::Handled);
                }
                Err(_) => return Ok(HealthCheckDecision::Handled),
            }
        }
        debug!("TLS request is not a health-check; proxying node={} target={}", node_id, target);
        return Ok(HealthCheckDecision::Proxy { buffered_headers: record });
    }
    for request_number in 1u64.. {
        let headers = match timeout(Duration::from_secs(15), read_http_headers(br, 8192)).await {
            Ok(Ok(headers)) => headers,
            Ok(Err(err)) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(HealthCheckDecision::Handled),
            Ok(Err(err)) => {
                warn!("health-check header read failed node={} target={} request={}: {err}", node_id, target, request_number);
                br.get_mut().write_all(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
                return Ok(HealthCheckDecision::Handled);
            }
            Err(_) => return Ok(HealthCheckDecision::Handled),
        };
        if !is_health_check_request(&app.cfg.health_check_urls, target, &headers) {
            if possible_resolved_health_target && !exact_health_target {
                debug!("resolved HTTP request is not a health-check; proxying node={} target={}", node_id, target);
                return Ok(HealthCheckDecision::Proxy { buffered_headers: headers });
            }
            warn!("plain HTTP request did not match health-check list node={} target={}", node_id, target);
            br.get_mut().write_all(b"HTTP/1.1 421 Misdirected Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
            return Ok(HealthCheckDecision::Handled);
        }
        match timeout(Duration::from_secs(20), app.probe_rtt(node_id)).await {
            Ok(Ok(probe)) if probe.ok => {
                if app.should_log_health_checks().await {
                    info!("health-check probe ok node={} target={} request={} rtt_ms={:?} warm_ms={:?}", node_id, target, request_number, probe.rtt_ms, probe.warm_ms);
                }
                br.get_mut().write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n").await?;
            }
            Ok(Ok(probe)) => {
                warn!("health-check probe unhealthy node={} target={} request={} error={}", node_id, target, request_number, probe.error);
                br.get_mut().write_all(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
                return Ok(HealthCheckDecision::Handled);
            }
            Ok(Err(err)) => {
                warn!("health-check probe failed node={} target={} request={}: {err}", node_id, target, request_number);
                br.get_mut().write_all(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await?;
                return Ok(HealthCheckDecision::Handled);
            }
            Err(_) => return Ok(HealthCheckDecision::Handled),
        }
    }
    Ok(HealthCheckDecision::Handled)
}

async fn socks_auth(br: &mut BufReader<TcpStream>, password: &str) -> Result<String> {
    let header = read_exact_vec(br, 2).await?;
    if header[0] != 0x05 { return Err(anyhow!("unsupported socks version")); }
    let methods = read_exact_vec(br, header[1] as usize).await?;
    if !methods.contains(&0x02) {
        br.get_mut().write_all(&[0x05, 0xff]).await?;
        return Err(anyhow!("username/password auth required"));
    }
    br.get_mut().write_all(&[0x05, 0x02]).await?;
    let auth_header = read_exact_vec(br, 2).await?;
    if auth_header[0] != 0x01 { return Err(anyhow!("unsupported auth version")); }
    let user = read_exact_vec(br, auth_header[1] as usize).await?;
    let pass_len = br.read_u8().await? as usize;
    let pass = read_exact_vec(br, pass_len).await?;
    if pass != password.as_bytes() || user.is_empty() {
        br.get_mut().write_all(&[0x01, 0x01]).await?;
        return Err(anyhow!("invalid credentials"));
    }
    br.get_mut().write_all(&[0x01, 0x00]).await?;
    Ok(String::from_utf8_lossy(&user).to_string())
}

#[derive(Debug)]
enum SocksError { Io(io::Error), UnsupportedCommand(u8), Invalid(String) }
impl std::fmt::Display for SocksError { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { match self { SocksError::Io(e) => write!(f, "{e}"), SocksError::UnsupportedCommand(c) => write!(f, "unsupported SOCKS command 0x{c:02x}"), SocksError::Invalid(e) => write!(f, "{e}") } } }
impl From<io::Error> for SocksError { fn from(value: io::Error) -> Self { SocksError::Io(value) } }

async fn socks_request(br: &mut BufReader<TcpStream>) -> Result<String, SocksError> {
    let header = read_exact_vec(br, 4).await?;
    if header[0] != 0x05 { return Err(SocksError::Invalid("unsupported SOCKS version".to_string())); }
    if header[1] != 0x01 { return Err(SocksError::UnsupportedCommand(header[1])); }
    let host = match header[3] {
        0x01 => IpAddr::from(<[u8; 4]>::try_from(read_exact_vec(br, 4).await?.as_slice()).unwrap()).to_string(),
        0x03 => { let len = br.read_u8().await? as usize; String::from_utf8_lossy(&read_exact_vec(br, len).await?).to_string() }
        0x04 => IpAddr::from(<[u8; 16]>::try_from(read_exact_vec(br, 16).await?.as_slice()).unwrap()).to_string(),
        _ => return Err(SocksError::Invalid("unsupported address type".to_string())),
    };
    let port = br.read_u16().await?;
    Ok(format_host_port(&host, port))
}

async fn dial_socks5(proxy_addr: &str, target: &str, dur: Duration) -> Result<TcpStream> {
    timeout(dur, async {
        let mut conn = TcpStream::connect(proxy_addr).await?;
        conn.write_all(&[0x05, 0x01, 0x00]).await?;
        let method = read_exact_stream(&mut conn, 2).await?;
        if method != [0x05, 0x00] { return Err(anyhow!("relayway SOCKS auth method rejected: {method:x?}")); }
        conn.write_all(&socks_connect_request(target)?).await?;
        let response = read_exact_stream(&mut conn, 4).await?;
        if response[0] != 0x05 || response[1] != 0x00 { return Err(anyhow!("relayway SOCKS connect rejected rep=0x{:02x}", response[1])); }
        let addr_len = match response[3] { 0x01 => 4, 0x03 => read_exact_stream(&mut conn, 1).await?[0] as usize, 0x04 => 16, other => return Err(anyhow!("relayway SOCKS response unsupported atyp=0x{other:02x}")) };
        let _ = read_exact_stream(&mut conn, addr_len + 2).await?;
        Ok(conn)
    }).await?
}

fn socks_connect_request(target: &str) -> Result<Vec<u8>> {
    let (host, port) = split_host_port(target)?;
    let mut request = vec![0x05, 0x01, 0x00];
    if let Ok(ip) = host.parse::<IpAddr>() {
        match ip { IpAddr::V4(ip) => { request.push(0x01); request.extend(ip.octets()); } IpAddr::V6(ip) => { request.push(0x04); request.extend(ip.octets()); } }
    } else {
        if host.len() > 255 { return Err(anyhow!("target host too long: {host}")); }
        request.push(0x03); request.push(host.len() as u8); request.extend(host.as_bytes());
    }
    request.extend(port.to_be_bytes());
    Ok(request)
}

async fn send_socks_reply(conn: &mut TcpStream, rep: u8) -> io::Result<()> {
    conn.write_all(&[0x05, rep, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await
}

async fn read_exact_vec<R: AsyncReadExt + Unpin>(reader: &mut R, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; n];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_exact_stream(stream: &mut TcpStream, n: usize) -> io::Result<Vec<u8>> { read_exact_vec(stream, n).await }

async fn read_http_headers(br: &mut BufReader<TcpStream>, limit: usize) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while buf.len() < limit {
        let byte = br.read_u8().await?;
        buf.push(byte);
        if buf.ends_with(b"\r\n\r\n") || buf.ends_with(b"\n\n") { return Ok(buf); }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "http headers too large"))
}

async fn read_tls_client_hello_record(br: &mut BufReader<TcpStream>, limit: usize) -> io::Result<Vec<u8>> {
    let mut record = read_exact_vec(br, 5).await?;
    let len = u16::from_be_bytes([record[3], record[4]]) as usize;
    if record[0] != 0x16 || len == 0 || len > limit {
        return Ok(record);
    }
    record.extend(read_exact_vec(br, len).await?);
    Ok(record)
}

fn health_target_host_matches(items: &[HealthCheckUrl], target: &str) -> bool {
    let target = target.to_ascii_lowercase();
    items.iter().any(|item| item.host_port == target)
}

fn is_possible_resolved_health_check_target(target: &str) -> bool {
    let Ok((host, port)) = split_host_port(target) else { return false; };
    port == 80 && host.parse::<IpAddr>().is_ok()
}

fn is_possible_tls_health_check_target(items: &[HealthCheckUrl], target: &str) -> bool {
    let Ok((host, port)) = split_host_port(target) else { return false; };
    if port != 443 || !items.iter().any(|item| item.host_port.ends_with(":443")) {
        return false;
    }
    host.parse::<IpAddr>().is_ok() || items.iter().any(|item| item.host_port == format!("{}:443", host.to_ascii_lowercase()))
}

fn tls_sni_matches_health_check(items: &[HealthCheckUrl], target: &str, record: &[u8]) -> bool {
    let Some(sni) = tls_client_hello_sni(record) else { return false; };
    let sni_host_port = format!("{}:443", sni.to_ascii_lowercase());
    let target_matches = split_host_port(target).ok().is_some_and(|(host, port)| {
        port == 443 && (host.parse::<IpAddr>().is_ok() || host.eq_ignore_ascii_case(&sni))
    });
    target_matches && items.iter().any(|item| item.host_port == sni_host_port)
}

fn tls_client_hello_sni(record: &[u8]) -> Option<String> {
    if record.len() < 5 || record[0] != 0x16 {
        return None;
    }
    let record_len = u16::from_be_bytes([record[3], record[4]]) as usize;
    if record.len() < 5 + record_len || record.get(5).copied()? != 0x01 {
        return None;
    }
    let body_len = ((record.get(6).copied()? as usize) << 16) | ((record.get(7).copied()? as usize) << 8) | record.get(8).copied()? as usize;
    let mut pos = 9;
    let end = (9 + body_len).min(record.len());
    if pos + 34 > end {
        return None;
    }
    pos += 34;
    let session_len = *record.get(pos)? as usize;
    pos += 1 + session_len;
    if pos + 2 > end {
        return None;
    }
    let cipher_len = u16::from_be_bytes([record[pos], record[pos + 1]]) as usize;
    pos += 2 + cipher_len;
    if pos >= end {
        return None;
    }
    let compression_len = *record.get(pos)? as usize;
    pos += 1 + compression_len;
    if pos + 2 > end {
        return None;
    }
    let ext_len = u16::from_be_bytes([record[pos], record[pos + 1]]) as usize;
    pos += 2;
    let ext_end = (pos + ext_len).min(end);
    while pos + 4 <= ext_end {
        let ext_type = u16::from_be_bytes([record[pos], record[pos + 1]]);
        let len = u16::from_be_bytes([record[pos + 2], record[pos + 3]]) as usize;
        pos += 4;
        if pos + len > ext_end {
            return None;
        }
        if ext_type == 0x0000 {
            return parse_tls_sni_extension(&record[pos..pos + len]);
        }
        pos += len;
    }
    None
}

fn parse_tls_sni_extension(data: &[u8]) -> Option<String> {
    if data.len() < 5 {
        return None;
    }
    let list_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut pos = 2;
    let end = (2 + list_len).min(data.len());
    while pos + 3 <= end {
        let name_type = data[pos];
        let len = u16::from_be_bytes([data[pos + 1], data[pos + 2]]) as usize;
        pos += 3;
        if pos + len > end {
            return None;
        }
        if name_type == 0 {
            return std::str::from_utf8(&data[pos..pos + len]).ok().map(|host| host.to_ascii_lowercase());
        }
        pos += len;
    }
    None
}

fn is_health_check_request(items: &[HealthCheckUrl], target: &str, headers: &[u8]) -> bool {
    let (request_line, mut host) = parse_http_request_meta(headers);
    let parts: Vec<_> = request_line.split_whitespace().collect();
    if parts.len() < 2 { return false; }
    let method = parts[0].to_ascii_uppercase();
    if method != "GET" && method != "HEAD" { return false; }
    let (path_host, path) = normalize_http_url_path(parts[1]);
    if host.is_empty() { host = path_host.unwrap_or_else(|| target.to_string()); }
    if !host.contains(':') { host = format!("{host}:80"); }
    let host = host.to_ascii_lowercase();
    items.iter().any(|item| item.host_port == host && item.path == path)
}

fn normalize_http_url_path(value: &str) -> (Option<String>, String) {
    if !value.starts_with("http://") { return (None, value.to_string()); }
    let without = &value[7..];
    match without.split_once('/') { Some((host, path)) => (Some(host.to_string()), format!("/{path}")), None => (Some(without.to_string()), "/".to_string()) }
}

fn parse_http_request_meta(headers: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(headers).replace("\r\n", "\n");
    let mut lines = text.lines();
    let request_line = lines.next().unwrap_or_default().trim().to_string();
    let host = lines.find_map(|line| line.split_once(':').and_then(|(key, value)| key.eq_ignore_ascii_case("host").then(|| value.trim().to_string()))).unwrap_or_default();
    (request_line, host)
}

fn split_host_port(target: &str) -> Result<(String, u16)> {
    if let Ok(addr) = target.parse::<SocketAddr>() {
        return Ok((addr.ip().to_string(), addr.port()));
    }
    let idx = target.rfind(':').ok_or_else(|| anyhow!("missing port in target {target}"))?;
    let host = target[..idx].trim_matches(['[', ']']).to_string();
    let port = target[idx + 1..].parse::<u16>()?;
    Ok((host, port))
}

fn format_host_port(host: &str, port: u16) -> String { if host.contains(':') && !host.starts_with('[') { format!("[{host}]:{port}") } else { format!("{host}:{port}") } }

fn with_human_bytes(mut status: NfCloudStatus) -> NfCloudStatus {
    status.up_human = Some(format_bytes(status.up_bytes.max(0) as u64));
    status.down_human = Some(format_bytes(status.down_bytes.max(0) as u64));
    status
}

fn format_bytes(value: u64) -> String {
    const UNIT: f64 = 1024.0;
    if value < 1024 { return format!("{value} B"); }
    let mut f = value as f64;
    for suffix in ["KiB", "MiB", "GiB", "TiB", "PiB"] {
        f /= UNIT;
        if f < UNIT { return format!("{f:.2} {suffix}"); }
    }
    format!("{:.2} EiB", f / UNIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_health_check_url() {
        let urls = parse_health_check_urls("http://cp.cloudflare.com/generate_204,https://www.gstatic.com/generate_204");
        assert_eq!(urls[0].host_port, "cp.cloudflare.com:80");
        assert_eq!(urls[0].path, "/generate_204");
        assert_eq!(urls[1].host_port, "www.gstatic.com:443");
        assert_eq!(urls[1].path, "/generate_204");
    }

    #[test]
    fn builds_socks_request_for_domain() {
        let request = socks_connect_request("example.com:443").unwrap();
        assert_eq!(&request[..5], &[0x05, 0x01, 0x00, 0x03, 11]);
    }

    #[test]
    fn matches_http_health_check() {
        let urls = parse_health_check_urls("http://cp.cloudflare.com/generate_204");
        let headers = b"GET /generate_204 HTTP/1.1\r\nHost: cp.cloudflare.com\r\n\r\n";
        assert!(is_health_check_request(&urls, "cp.cloudflare.com:80", headers));
    }

    #[test]
    fn matches_resolved_health_check_connect_target() {
        assert!(is_possible_resolved_health_check_target("104.18.10.242:80"));
        assert!(!is_possible_resolved_health_check_target("cp.cloudflare.com:80"));
        assert!(!is_possible_resolved_health_check_target("104.18.10.242:443"));
    }

    #[test]
    fn matches_tls_health_check_sni() {
        let urls = parse_health_check_urls("https://www.gstatic.com/generate_204");
        let record = tls_client_hello_with_sni("www.gstatic.com");
        assert!(is_possible_tls_health_check_target(&urls, "www.gstatic.com:443"));
        assert!(is_possible_tls_health_check_target(&urls, "142.250.72.163:443"));
        assert!(tls_sni_matches_health_check(&urls, "www.gstatic.com:443", &record));
        assert!(tls_sni_matches_health_check(&urls, "142.250.72.163:443", &record));
        assert!(!tls_sni_matches_health_check(&urls, "www.apple.com:443", &record));
    }

    fn tls_client_hello_with_sni(host: &str) -> Vec<u8> {
        let host = host.as_bytes();
        let mut sni = Vec::new();
        sni.extend(((host.len() + 3) as u16).to_be_bytes());
        sni.push(0);
        sni.extend((host.len() as u16).to_be_bytes());
        sni.extend(host);

        let mut extensions = Vec::new();
        extensions.extend(0u16.to_be_bytes());
        extensions.extend((sni.len() as u16).to_be_bytes());
        extensions.extend(sni);

        let mut body = Vec::new();
        body.extend([0x03, 0x03]);
        body.extend([0u8; 32]);
        body.push(0);
        body.extend(2u16.to_be_bytes());
        body.extend(0x1301u16.to_be_bytes());
        body.push(1);
        body.push(0);
        body.extend((extensions.len() as u16).to_be_bytes());
        body.extend(extensions);

        let mut record = Vec::new();
        record.push(0x16);
        record.extend([0x03, 0x01]);
        record.extend(((body.len() + 4) as u16).to_be_bytes());
        record.push(0x01);
        record.extend([((body.len() >> 16) & 0xff) as u8, ((body.len() >> 8) & 0xff) as u8, (body.len() & 0xff) as u8]);
        record.extend(body);
        record
    }
}
