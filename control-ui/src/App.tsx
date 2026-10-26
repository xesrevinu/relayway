import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type Provider = {
  id: string;
  name: string;
  api: string;
  native: string;
  socks: number;
  apiPort: number;
};

type RelayHealth = {
  ok?: boolean;
  service?: string;
  version?: string;
};

type RelayStatus = {
  connected?: boolean;
  active_node_id?: string;
  active_node_name?: string;
  active_mode?: string;
  up_human?: string;
  down_human?: string;
};

type BootstrapInfo = {
  service_id?: string;
  service_name?: string;
  panel_type?: string;
  api_domains?: string[];
};

type ProviderStatus = {
  active_node?: string;
  active_connections?: number;
  bootstrap?: BootstrapInfo;
  client_to_up_human?: string;
  connections?: number;
  debug_health_checks?: boolean;
  debug_proxy_logs?: boolean;
  failed_dials?: number;
  failed_handshakes?: number;
  failed_switches?: number;
  has_token?: boolean;
  last_error?: string;
  last_switch_at?: unknown;
  last_watchdog_at?: unknown;
  peak_connections?: number;
  relay_health?: RelayHealth;
  relay_ready?: boolean;
  relay_socks_ok?: boolean;
  relayway_mode?: string;
  relayway_status?: RelayStatus;
  rejected_connections?: number;
  switches?: number;
  up_to_client_human?: string;
  uptime_seconds?: number;
  watchdog_checks?: number;
  watchdog_failures?: number;
};

type RelayNode = {
  id: string;
  name?: string;
  protocol?: string;
};

type ProbeResult = {
  id: string;
  name?: string;
  ok?: boolean;
  rtt_ms?: number | null;
  warm_ms?: number | null;
  error?: string;
};

type ProviderSnapshot = {
  status: ProviderStatus;
  relay: RelayStatus;
  nodes: RelayNode[];
  logs: string[];
  debug: DebugFlags;
  consumer: ConsumerOverview;
  error?: string;
};

type DebugFlags = {
  proxy_logs?: boolean;
  health_checks?: boolean;
};

type EgressInfo = {
  label: string;
  tone: "idle" | "ok" | "error" | "loading";
};

type JsonRecord = Record<string, unknown>;

type ConsumerOverview = {
  plans?: unknown;
  notices?: unknown;
  devices?: unknown;
  update?: unknown;
  user?: unknown;
  subscription?: unknown;
};

const providers: Provider[] = [
  {
    id: "fishport",
    name: "Fishport",
    api: "/api/fishport",
    native: import.meta.env.VITE_PROVIDER_URL || "http://127.0.0.1:7664",
    socks: Number(import.meta.env.VITE_PROVIDER_SOCKS_PORT || 10080),
    apiPort: Number(import.meta.env.VITE_PROVIDER_API_PORT || 18088),
  },
];

const emptySnapshot: ProviderSnapshot = {
  status: {},
  relay: {},
  nodes: [],
  logs: [],
  debug: {},
  consumer: {},
  error: "loading",
};

async function requestJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error((await response.text()) || response.statusText);
  return response.json() as Promise<T>;
}

function formatAge(value?: unknown) {
  if (!value) return "never";

  if (typeof value === "number") {
    const seconds = Math.max(0, Math.floor(Date.now() / 1000 - value));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  }

  if (typeof value === "object") {
    const record = asRecord(value);
    const secs = record.secs_since_epoch;
    if (typeof secs === "number") {
      const seconds = Math.max(0, Math.floor(Date.now() / 1000 - secs));
      if (seconds < 60) return `${seconds}s ago`;
      if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
      if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
      return `${Math.floor(seconds / 86400)}d ago`;
    }
    return "unknown";
  }

  const text = String(value);
  if (!text || text.startsWith("0001-")) return "never";
  const parsed = Number.isFinite(Date.parse(text)) ? Date.parse(text) : NaN;
  if (!Number.isFinite(parsed)) return text;
  const seconds = Math.max(0, Math.floor((Date.now() - parsed) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : {};
}

function asArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.map(asRecord) : [];
}

function unwrapConsumerData(value: unknown): unknown {
  const root = asRecord(value);
  const data = root.data ?? root.result ?? root.results;
  return data ?? value;
}

function listFromConsumer(value: unknown): JsonRecord[] {
  const data = unwrapConsumerData(value);
  if (Array.isArray(data)) return asArray(data);
  const record = asRecord(data);
  return asArray(
    record.list ??
      record.items ??
      record.records ??
      record.data ??
      record.devices ??
      record.notices ??
      record.plans,
  );
}

function valueText(value: unknown) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number") return Number.isFinite(value) ? value.toLocaleString() : "-";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function plainText(value: unknown) {
  return valueText(value)
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function numberValue(value: unknown) {
  if (typeof value === "number") return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function bytesText(bytes?: number) {
  if (bytes === undefined) return "-";
  const gib = bytes / 1024 / 1024 / 1024;
  return `${gib.toFixed(gib >= 10 ? 1 : 2)}G`;
}

function dateText(value: unknown) {
  const seconds = numberValue(value);
  if (seconds && seconds > 0) return new Date(seconds * 1000).toLocaleDateString();
  return valueText(value);
}

function pick(record: JsonRecord, keys: string[]) {
  for (const key of keys) {
    if (record[key] !== undefined && record[key] !== null && record[key] !== "") return record[key];
  }
  return undefined;
}

function consumerError(value: unknown) {
  const record = asRecord(value);
  return record.ok === false ? valueText(record.error || "provider returned an error") : "";
}

function cleanAnsi(line: string) {
  return line.replace(new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g"), "");
}

function parseLogLine(raw: string) {
  const clean = cleanAnsi(raw);
  const match = clean.match(/^(\S+)\s+(ERROR|WARN|INFO|DEBUG|TRACE)\s+(.*)$/i);
  if (!match) return { time: "", level: "LOG", message: clean, clean };
  const date = new Date(match[1]);
  const time = Number.isFinite(date.getTime()) ? date.toLocaleTimeString() : match[1];
  return { time, level: match[2].toUpperCase(), message: match[3], clean };
}

function logLevelAnsi(level: string) {
  if (level === "ERROR") return "\x1b[31m";
  if (level === "WARN") return "\x1b[33m";
  if (level === "INFO") return "\x1b[32m";
  return "\x1b[37m";
}

function statusTone(snapshot?: ProviderSnapshot) {
  if (!snapshot || snapshot.error === "loading") return "loading";
  if (snapshot.error || !snapshot.status.relay_ready || !snapshot.status.relay_socks_ok)
    return "down";
  if ((snapshot.status.watchdog_failures || 0) > 0 || snapshot.status.last_error) return "warn";
  return "ready";
}

async function loadProvider(provider: Provider): Promise<ProviderSnapshot> {
  const [statusResult, relayResult, nodesResult, logsResult, debugResult, consumerResult] =
    await Promise.allSettled([
      requestJson<ProviderStatus>(`${provider.api}/status`),
      requestJson<RelayStatus>(`${provider.api}/relayway/status`),
      requestJson<{ nodes?: RelayNode[] }>(`${provider.api}/nodes`),
      requestJson<{ lines?: string[] }>(`${provider.api}/logs`),
      requestJson<DebugFlags>(`${provider.api}/debug`),
      requestJson<ConsumerOverview>(`${provider.api}/consumer/overview`),
    ]);

  const status = statusResult.status === "fulfilled" ? statusResult.value : {};
  const relay =
    relayResult.status === "fulfilled" ? relayResult.value : status.relayway_status || {};
  const nodes = nodesResult.status === "fulfilled" ? nodesResult.value.nodes || [] : [];
  const logs = logsResult.status === "fulfilled" ? logsResult.value.lines || [] : [];
  const debug = debugResult.status === "fulfilled" ? debugResult.value : {};
  const consumer = consumerResult.status === "fulfilled" ? consumerResult.value : {};

  return {
    status,
    relay,
    nodes,
    logs,
    debug,
    consumer,
    error: statusResult.status === "rejected" ? statusResult.reason.message : undefined,
  };
}

function Metric({ label, value, hint }: { label: string; value?: string | number; hint?: string }) {
  return (
    <div className="rounded-xl border border-slate-700/70 bg-slate-950/45 p-3 shadow-inner shadow-black/15">
      <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
        {label}
      </div>
      <div className="mt-2 break-words text-xl font-black text-slate-50">{value || "-"}</div>
      {hint && <div className="mt-1 text-[11px] text-slate-500">{hint}</div>}
    </div>
  );
}

function ProviderCard({
  provider,
  snapshot,
  active,
  onSelect,
  egress,
  onEgress,
}: {
  provider: Provider;
  snapshot?: ProviderSnapshot;
  active: boolean;
  onSelect: () => void;
  egress?: EgressInfo;
  onEgress: () => void;
}) {
  const tone = statusTone(snapshot);
  const data = snapshot || emptySnapshot;
  const activeNode =
    data.relay.active_node_name || data.relay.active_node_id || data.status.active_node || "none";
  const toneClass = {
    ready: "border-emerald-300/30 bg-emerald-300/10 text-emerald-200",
    warn: "border-amber-300/30 bg-amber-300/10 text-amber-200",
    down: "border-red-300/30 bg-red-300/10 text-red-200",
    loading: "border-stone-300/20 bg-slate-300/10 text-slate-300",
  }[tone];
  const egressTone =
    egress?.tone === "error"
      ? "text-red-300"
      : egress?.tone === "ok"
        ? "text-emerald-200"
        : "text-slate-300";

  return (
    <section
      className={`group relative overflow-hidden rounded-2xl border p-4 text-left shadow-xl shadow-black/25 transition hover:-translate-y-0.5 ${
        active
          ? "border-cyan-200/45 bg-cyan-300/10"
          : "border-slate-700/70 bg-slate-800/50 hover:border-slate-500/80"
      }`}
      onClick={onSelect}
      role="button"
      tabIndex={0}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") onSelect();
      }}
    >
      <div className="absolute right-0 top-0 h-24 w-24 rounded-full bg-cyan-200/10 blur-2xl transition group-hover:bg-cyan-200/20" />
      <div className="relative flex items-start justify-between gap-4">
        <div>
          <div className="text-xs font-black uppercase tracking-[0.32em] text-slate-400">
            Provider
          </div>
          <h2 className="mt-2 text-3xl font-black tracking-tight text-white">{provider.name}</h2>
          <p className="mt-2 text-sm text-slate-400">
            SOCKS :{provider.socks} / API :{provider.apiPort}
          </p>
        </div>
        <span
          className={`rounded-full border px-3 py-2 text-xs font-black uppercase tracking-[0.18em] ${toneClass}`}
        >
          {tone}
        </span>
      </div>
      <div className="relative mt-6 grid gap-3 sm:grid-cols-3">
        <Metric label="Active" value={activeNode} />
        <Metric label="Nodes" value={data.nodes.length} />
        <Metric
          label="Traffic"
          value={`${data.relay.up_human || "0 B"} / ${data.relay.down_human || "0 B"}`}
        />
      </div>
      <div
        className="relative mt-4 rounded-xl border border-slate-700/70 bg-black/20 p-4"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">Actions</div>
        <div className="mt-3 flex flex-wrap gap-3">
          <a className="control-button" href={provider.native} rel="noreferrer" target="_blank">
            Native panel
          </a>
          <a
            className="control-button"
            href={`${provider.api}/provider.yaml`}
            rel="noreferrer"
            target="_blank"
          >
            Provider YAML
          </a>
          <button className="control-button-primary" onClick={onEgress} type="button">
            Check egress
          </button>
        </div>
        <div className="mt-3 text-sm text-slate-400">
          <span className="font-semibold text-slate-200">Egress</span>
          <span className={`ml-3 break-words ${egressTone}`}>{egress?.label || "not queried"}</span>
        </div>
      </div>
    </section>
  );
}

function NodeWorkspace({
  provider,
  snapshot,
  probes,
  scanning,
  onProbe,
  onProbeAll,
}: {
  provider: Provider;
  snapshot: ProviderSnapshot;
  probes: Record<string, ProbeResult>;
  scanning: boolean;
  onProbe: (node: RelayNode) => void;
  onProbeAll: () => void;
}) {
  const [query, setQuery] = useState("");
  const activeNode = snapshot.relay.active_node_id || snapshot.status.active_node;
  const filtered = snapshot.nodes.filter((node) =>
    `${node.name || ""} ${node.id}`.toLowerCase().includes(query.toLowerCase()),
  );

  return (
    <section className="rounded-2xl border border-slate-700/70 bg-slate-900/65 p-4 shadow-xl shadow-black/25 backdrop-blur-xl">
      <div className="flex flex-col justify-between gap-3 lg:flex-row lg:items-end">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.24em] text-cyan-300/80">
            Node operations
          </div>
          <h3 className="mt-1 text-2xl font-black tracking-tight text-white">
            Switch and scan {provider.name}
          </h3>
          <p className="mt-1 text-xs text-slate-400">
            Compact node cards with latency checks and one-tap switching.
          </p>
        </div>
        <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:flex-wrap">
          <input
            className="w-full rounded-lg border border-slate-700/80 bg-slate-950/60 px-3 py-2 text-xs text-white outline-none transition placeholder:text-slate-500 focus:border-cyan-300/60 sm:w-64"
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search node name or id"
            value={query}
          />
          <button
            className="control-button-primary"
            disabled={scanning || snapshot.nodes.length === 0}
            onClick={onProbeAll}
            type="button"
          >
            {scanning ? "Scanning" : "Scan latency"}
          </button>
        </div>
      </div>

      <div className="mt-4 grid max-h-[26rem] gap-2 overflow-auto overscroll-contain pr-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
        {filtered.map((node) => {
          const probe = probes[node.id];
          const isActive = node.id === activeNode;
          const latency = probe
            ? probe.ok
              ? `${probe.rtt_ms ?? probe.warm_ms} ms`
              : probe.error || "failed"
            : "not scanned";
          return (
            <article
              className={`group rounded-xl border p-3 transition hover:border-cyan-300/40 focus-within:border-cyan-300/40 ${
                isActive
                  ? "border-emerald-300/40 bg-emerald-300/10"
                  : "border-slate-700/70 bg-slate-950/35"
              }`}
              key={node.id}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div
                    className="truncate text-sm font-black text-slate-100"
                    title={node.name || node.id}
                  >
                    {node.name || node.id}
                  </div>
                  <div className="mt-1 font-mono text-[10px] uppercase tracking-[0.12em] text-slate-500">
                    {node.id} / {node.protocol || "node"}
                  </div>
                </div>
                <span
                  className={`shrink-0 rounded-md px-2 py-1 text-[10px] font-black uppercase ${isActive ? "bg-emerald-300/15 text-emerald-200" : "bg-slate-800 text-slate-400"}`}
                >
                  {isActive ? "active" : "idle"}
                </span>
              </div>
              <div className="mt-3 flex items-center justify-between gap-2">
                <div
                  className={`truncate font-mono text-xs ${probe?.ok ? "text-emerald-200" : probe ? "text-red-300" : "text-slate-500"}`}
                  title={latency}
                >
                  {latency}
                </div>
                <button
                  className="mini-button opacity-0 transition group-hover:opacity-100 group-focus-within:opacity-100"
                  onClick={() => onProbe(node)}
                  type="button"
                >
                  Probe
                </button>
              </div>
            </article>
          );
        })}
        {!filtered.length && (
          <div className="rounded-xl border border-slate-700/70 bg-slate-950/35 p-4 text-sm text-slate-500 sm:col-span-2 lg:col-span-3">
            No nodes match your search.
          </div>
        )}
      </div>
    </section>
  );
}

function ConsumerPanel({ snapshot }: { snapshot: ProviderSnapshot }) {
  const [showCatalog, setShowCatalog] = useState(false);
  const planContainer = asRecord(unwrapConsumerData(snapshot.consumer.plans));
  const planData = asRecord(
    planContainer.data ?? planContainer.result ?? planContainer.results ?? planContainer,
  );
  const plans = listFromConsumer(snapshot.consumer.plans);

  const noticeContainer = asRecord(unwrapConsumerData(snapshot.consumer.notices));
  const noticeData = asRecord(
    noticeContainer.data ?? noticeContainer.result ?? noticeContainer.results ?? noticeContainer,
  );
  const notices = asArray(
    noticeData.items ?? noticeData.list ?? noticeData.records ?? noticeData.data,
  );

  const deviceContainer = asRecord(unwrapConsumerData(snapshot.consumer.devices));
  const deviceData = asRecord(
    deviceContainer.data ?? deviceContainer.result ?? deviceContainer.results ?? deviceContainer,
  );
  const devices = asArray(
    deviceData.devices ?? deviceData.items ?? deviceData.records ?? deviceData.data,
  );

  const updateContainer = asRecord(unwrapConsumerData(snapshot.consumer.update));
  const updateData = asRecord(
    updateContainer.data ?? updateContainer.result ?? updateContainer.results ?? updateContainer,
  );
  const user = asRecord(unwrapConsumerData(snapshot.consumer.user));
  const subscription = asRecord(unwrapConsumerData(snapshot.consumer.subscription));

  const currentDevices = pick(deviceData, ["current_devices", "currentDevices"]);
  const maxDevices = pick(deviceData, ["max_devices", "maxDevices"]);
  const totalDevices = pick(deviceData, ["total_devices", "totalDevices"]);
  const unlimitedDevices = pick(deviceData, ["unlimited"]);

  const latestVersion = pick(updateData, ["latest_version", "version", "version_name", "name"]);
  const hasUpdate = pick(updateData, ["has_update", "need_update", "available", "updatable"]);
  const downloadUrls = asArray(updateData.download_urls).length;
  const noticesError = consumerError(snapshot.consumer.notices);

  const currentPlanId = numberValue(
    pick(user, ["plan_id", "planId"]) ?? pick(subscription, ["plan_id", "planId"]),
  );
  const userPlanName = pick(user, ["plan", "plan_name", "planName"]);
  const userClass = numberValue(pick(user, ["user_class", "class", "group"]));
  const userConnector = numberValue(pick(user, ["node_connector", "connector"]));
  const currentPlan = asRecord(
    plans.find(
      (plan) => currentPlanId !== undefined && numberValue(pick(plan, ["id"])) === currentPlanId,
    ) ||
      plans.find(
        (plan) =>
          userPlanName &&
          valueText(pick(plan, ["name", "title", "plan_name"])).includes(valueText(userPlanName)),
      ) ||
      plans.find(
        (plan) =>
          userClass !== undefined &&
          numberValue(pick(plan, ["group"])) === userClass &&
          (userConnector === undefined || numberValue(pick(plan, ["connector"])) === userConnector),
      ) ||
      {},
  );
  const currentPlanName = valueText(
    userPlanName || pick(currentPlan, ["name", "title", "plan_name"]),
  );
  const uploadBytes = numberValue(
    pick(user, ["u", "upload"]) ?? pick(subscription, ["u", "upload"]),
  );
  const downloadBytes = numberValue(
    pick(user, ["d", "download"]) ?? pick(subscription, ["d", "download"]),
  );
  const totalBytes = numberValue(
    pick(user, ["transfer_enable", "total"]) ?? pick(subscription, ["transfer_enable", "total"]),
  );
  const usedBytes = downloadBytes ?? 0;
  const usagePercent =
    totalBytes && totalBytes > 0 ? Math.min(100, (usedBytes / totalBytes) * 100) : 0;
  const expiresAt =
    pick(user, ["expired_at", "expires"]) ?? pick(subscription, ["expired_at", "expires"]);

  const catalog = plans.map((plan) => ({
    name: valueText(pick(plan, ["name", "title", "plan_name", "package_name", "product_name"])),
    transfer: valueText(pick(plan, ["transfer"])),
    price: valueText(pick(plan, ["price", "month", "quarter", "half_year", "year", "onetime"])),
    reset: valueText(pick(plan, ["reset", "reset_day", "reset_at"])),
    group: valueText(pick(plan, ["group"])),
    connector: valueText(pick(plan, ["connector"])),
    content: plainText(pick(plan, ["content"])),
  }));

  const onlineDevices = devices.filter((device) =>
    Boolean(pick(device, ["online", "is_online", "connected", "alive"])),
  ).length;

  return (
    <section className="rounded-2xl border border-slate-700/70 bg-slate-900/65 p-4 shadow-xl shadow-black/25 backdrop-blur-xl">
      <div className="flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
        <div>
          <div className="text-xs font-black uppercase tracking-[0.3em] text-cyan-200/80">
            Consumer overview
          </div>
          <h3 className="mt-2 text-3xl font-black tracking-tight text-white">
            Plan catalog, notices and devices
          </h3>
          <p className="mt-2 text-sm text-slate-400">
            Provider data, normalized from the raw API shapes we verified directly.
          </p>
        </div>
        <span className="rounded-full border border-slate-700/70 bg-slate-800/60 px-3 py-2 text-xs font-black uppercase tracking-[0.18em] text-slate-300">
          {catalog.length} plans / {devices.length} devices / {notices.length} notices
        </span>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric
          label="Current plan"
          value={currentPlanName}
          hint={
            currentPlanId
              ? `plan id ${currentPlanId}`
              : userClass
                ? `class ${userClass}`
                : "from user profile"
          }
        />
        <Metric
          label="Usage"
          value={`${bytesText(usedBytes)} / ${bytesText(totalBytes)}`}
          hint={`${usagePercent.toFixed(1)}% used`}
        />
        <Metric label="Expires" value={dateText(expiresAt)} hint="subscription expiry" />
        <Metric label="Speed limit" value="unlimited" hint="provider reports no cap" />
      </div>

      <div className="mt-4 rounded-xl border border-slate-700/70 bg-black/20 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">
              Traffic usage
            </div>
            <div className="mt-1 text-sm text-slate-500">
              Used is provider-reported download. Upload {bytesText(uploadBytes)} / Download{" "}
              {bytesText(downloadBytes)}
            </div>
          </div>
          <div className="text-sm font-bold text-slate-200">{usagePercent.toFixed(1)}%</div>
        </div>
        <div className="mt-4 h-4 overflow-hidden rounded-full bg-slate-950/70">
          <div
            className="h-full rounded-full bg-gradient-to-r from-teal-200 to-emerald-300"
            style={{ width: `${usagePercent}%` }}
          />
        </div>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric
          label="Catalog plans"
          value={catalog.length}
          hint={valueText(planData.items ? `items: ${asArray(planData.items).length}` : "catalog")}
        />
        <Metric
          label="Devices"
          value={devices.length}
          hint={`${valueText(currentDevices)} current / ${valueText(totalDevices)} total`}
        />
        <Metric
          label="Online"
          value={onlineDevices}
          hint={
            unlimitedDevices === true ? "unlimited device quota" : `max ${valueText(maxDevices)}`
          }
        />
        <Metric
          label="Update"
          value={valueText(latestVersion)}
          hint={`${valueText(hasUpdate)} / downloads ${downloadUrls}`}
        />
      </div>

      <div className="mt-4 rounded-xl border border-slate-700/70 bg-black/20 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">
              Plan catalog
            </div>
            <div className="mt-1 text-sm text-slate-500">
              All available package rows from `plan.list`
            </div>
          </div>
          <button
            className="control-button"
            onClick={() => setShowCatalog((value) => !value)}
            type="button"
          >
            {showCatalog ? "Hide catalog" : `Show catalog (${catalog.length})`}
          </button>
        </div>
        {showCatalog && (
          <div className="mt-4 overflow-auto overscroll-contain rounded-xl border border-slate-700/70 bg-slate-950/30">
            <table className="w-full min-w-[1100px] text-left text-sm">
              <thead className="sticky top-0 bg-slate-950 text-xs uppercase tracking-[0.18em] text-slate-400">
                <tr>
                  <th className="px-4 py-3 font-black">Name</th>
                  <th className="px-4 py-3 font-black">Transfer</th>
                  <th className="px-4 py-3 font-black">Price</th>
                  <th className="px-4 py-3 font-black">Reset</th>
                  <th className="px-4 py-3 font-black">Group / Connector</th>
                  <th className="px-4 py-3 font-black">Content</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/10 text-slate-300">
                {catalog.map((plan, index) => (
                  <tr key={index}>
                    <td className="px-4 py-3 font-bold text-slate-100">{plan.name}</td>
                    <td className="px-4 py-3">{plan.transfer}</td>
                    <td className="px-4 py-3">{plan.price}</td>
                    <td className="px-4 py-3">{plan.reset}</td>
                    <td className="px-4 py-3">
                      {plan.group} / {plan.connector}
                    </td>
                    <td className="px-4 py-3 whitespace-pre-wrap break-words text-slate-300">
                      {plan.content || "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="mt-4">
        <div className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">Notices</div>
        <div className="mt-3 space-y-3">
          {noticesError ? (
            <div className="rounded-2xl border border-amber-300/30 bg-amber-300/10 p-4 text-sm text-amber-200">
              {noticesError}
            </div>
          ) : notices.length ? (
            notices.map((notice, index) => {
              const title = valueText(
                pick(notice, ["title", "name", "subject"]) || `Notice ${index + 1}`,
              );
              const body = plainText(
                pick(notice, ["content", "message", "body", "text", "description"]),
              );
              const createdAt = pick(notice, ["created_at", "createdAt", "time", "date"]);
              return (
                <div className="rounded-2xl border border-slate-700/70 bg-white/5 p-4" key={index}>
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="font-bold text-slate-100">{title}</div>
                    <div className="text-xs font-mono text-slate-500">{formatAge(createdAt)}</div>
                  </div>
                  <p className="mt-3 whitespace-pre-wrap break-words text-sm leading-7 text-slate-300">
                    {body}
                  </p>
                </div>
              );
            })
          ) : (
            <div className="rounded-2xl border border-slate-700/70 bg-white/5 p-4 text-sm text-slate-500">
              No notices returned by provider.
            </div>
          )}
        </div>
      </div>

      <div className="mt-4">
        <div className="flex items-end justify-between gap-3">
          <div className="text-xs font-black uppercase tracking-[0.22em] text-slate-400">
            Devices
          </div>
          <div className="text-xs text-slate-500">Showing all device records inline</div>
        </div>
        <div className="mt-3 overflow-auto overscroll-contain rounded-xl border border-slate-700/70 bg-slate-950/30">
          <table className="w-full min-w-[900px] text-left text-sm">
            <thead className="sticky top-0 bg-slate-950 text-xs uppercase tracking-[0.18em] text-slate-400">
              <tr>
                <th className="px-4 py-3 font-black">Device</th>
                <th className="px-4 py-3 font-black">Type</th>
                <th className="px-4 py-3 font-black">Status</th>
                <th className="px-4 py-3 font-black">Last active</th>
                <th className="px-4 py-3 font-black">ID</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10 text-slate-300">
              {devices.length ? (
                devices.map((device, index) => (
                  <tr key={index}>
                    <td className="px-4 py-3 font-bold text-slate-100">
                      {valueText(
                        pick(device, ["name", "device_name", "hostname", "id"]) ||
                          `Device ${index + 1}`,
                      )}
                    </td>
                    <td className="px-4 py-3">
                      {valueText(pick(device, ["device_type", "type", "platform", "os", "model"]))}
                    </td>
                    <td className="px-4 py-3">
                      {valueText(pick(device, ["online", "is_online", "status", "connected"]))}
                    </td>
                    <td className="px-4 py-3">
                      {formatAge(
                        pick(device, ["last_active", "lastActive", "updated_at", "updatedAt"]),
                      )}
                    </td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-500">
                      {valueText(pick(device, ["device_id", "id"]))}
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td className="px-4 py-4 text-slate-500" colSpan={5}>
                    No device records returned by provider.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function LogsPanel({
  snapshot,
  onToggleLogs,
}: {
  snapshot: ProviderSnapshot;
  onToggleLogs: (key: keyof DebugFlags) => void;
}) {
  const [query, setQuery] = useState("");
  const [level, setLevel] = useState("all");
  const [newestFirst, setNewestFirst] = useState(true);
  const [paused, setPaused] = useState(false);
  const [frozenLogs, setFrozenLogs] = useState<string[]>([]);
  const terminalRef = useRef<HTMLDivElement | null>(null);
  const terminalInstance = useRef<Terminal | null>(null);
  const fitAddon = useRef<FitAddon | null>(null);

  useEffect(() => {
    if (!terminalRef.current || terminalInstance.current) return;
    const terminal = new Terminal({
      convertEol: true,
      cursorBlink: false,
      disableStdin: true,
      fontFamily: "JetBrains Mono, SFMono-Regular, Menlo, monospace",
      fontSize: 11,
      lineHeight: 1.35,
      scrollback: 1000,
      theme: {
        background: "#060b10",
        foreground: "#d6dee8",
        black: "#0f172a",
        red: "#f87171",
        green: "#34d399",
        yellow: "#fbbf24",
        blue: "#60a5fa",
        magenta: "#c084fc",
        cyan: "#22d3ee",
        white: "#e2e8f0",
      },
    });
    const fit = new FitAddon();
    terminal.loadAddon(fit);
    terminal.open(terminalRef.current);
    fit.fit();
    terminalInstance.current = terminal;
    fitAddon.current = fit;
    const onResize = () => fit.fit();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      terminal.dispose();
      terminalInstance.current = null;
      fitAddon.current = null;
    };
  }, []);

  useEffect(() => {
    if (!paused) setFrozenLogs(snapshot.logs);
  }, [paused, snapshot.logs]);

  const sourceLogs = paused ? frozenLogs : snapshot.logs;
  const logs = sourceLogs
    .slice(-220)
    .map(parseLogLine)
    .filter((line) => (level === "all" ? true : line.level.toLowerCase() === level))
    .filter((line) => line.clean.toLowerCase().includes(query.toLowerCase()));
  const visibleLogs = newestFirst ? [...logs].reverse() : logs;

  useEffect(() => {
    const terminal = terminalInstance.current;
    if (!terminal) return;
    terminal.clear();
    visibleLogs.forEach((line) => {
      terminal.writeln(
        `\x1b[90m${line.time.padEnd(10)}\x1b[0m ${logLevelAnsi(line.level)}${line.level.padEnd(5)}\x1b[0m ${line.message}`,
      );
    });
    fitAddon.current?.fit();
  }, [visibleLogs]);

  return (
    <section className="rounded-2xl border border-slate-700/70 bg-slate-900/75 p-4 shadow-xl shadow-black/20 backdrop-blur-xl">
      <div className="flex flex-col justify-between gap-3 lg:flex-row lg:items-end">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.24em] text-cyan-300/80">
            Logs
          </div>
          <h3 className="mt-1 text-2xl font-black tracking-tight text-white">Live event console</h3>
          <p className="mt-1 text-xs text-slate-400">
            ANSI cleaned, newest-first by default, with structured timestamp and level columns.
          </p>
        </div>
        <div className="flex flex-col gap-2 lg:items-end">
          <div className="flex flex-wrap gap-2">
            <button
              className={
                (snapshot.debug.proxy_logs ?? snapshot.status.debug_proxy_logs)
                  ? "control-button-active"
                  : "control-button"
              }
              onClick={() => onToggleLogs("proxy_logs")}
              type="button"
            >
              Proxy logs
            </button>
            <button
              className={
                (snapshot.debug.health_checks ?? snapshot.status.debug_health_checks)
                  ? "control-button-active"
                  : "control-button"
              }
              onClick={() => onToggleLogs("health_checks")}
              type="button"
            >
              Health logs
            </button>
          </div>
          <div className="flex flex-wrap gap-2">
            {[
              ["all", "All"],
              ["info", "Info"],
              ["warn", "Warn"],
              ["error", "Error"],
            ].map(([value, label]) => (
              <button
                className={level === value ? "control-button-active" : "control-button"}
                key={value}
                onClick={() => setLevel(value)}
                type="button"
              >
                {label}
              </button>
            ))}
            <button
              className={newestFirst ? "control-button-active" : "control-button"}
              onClick={() => setNewestFirst((value) => !value)}
              type="button"
            >
              Newest first
            </button>
            <button
              className={paused ? "control-button-active" : "control-button"}
              onClick={() => setPaused((value) => !value)}
              type="button"
            >
              {paused ? "Paused" : "Live"}
            </button>
            <input
              className="rounded-lg border border-slate-700/80 bg-slate-950/60 px-3 py-2 text-xs text-white outline-none placeholder:text-slate-500 focus:border-cyan-300/60"
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Filter logs"
              value={query}
            />
          </div>
        </div>
      </div>

      <div className="mt-4 overflow-hidden rounded-xl border border-slate-800 bg-[#060b10] shadow-inner shadow-black/30">
        <div className="flex items-center justify-between border-b border-slate-800 bg-slate-950/85 px-3 py-2 text-[10px] uppercase tracking-[0.18em] text-slate-500">
          <span>{visibleLogs.length} lines</span>
          <span>{paused ? "frozen" : "streaming"}</span>
        </div>
        <div className="h-[32rem] overflow-hidden" ref={terminalRef} />
      </div>
    </section>
  );
}

export default function App() {
  const [snapshots, setSnapshots] = useState<Record<string, ProviderSnapshot>>({});
  const [selectedProviderId, setSelectedProviderId] = useState(providers[0].id);
  const [egress, setEgress] = useState<Record<string, EgressInfo>>({});
  const [probes, setProbes] = useState<Record<string, Record<string, ProbeResult>>>({});
  const [refreshing, setRefreshing] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [notice, setNotice] = useState("");

  const selectedProvider =
    providers.find((provider) => provider.id === selectedProviderId) || providers[0];
  const selectedSnapshot = snapshots[selectedProvider.id] || emptySnapshot;

  const refreshAll = useCallback(async () => {
    setRefreshing(true);
    try {
      await Promise.all(
        providers.map(async (provider) => {
          const snapshot = await loadProvider(provider);
          setSnapshots((current) => ({ ...current, [provider.id]: snapshot }));
        }),
      );
      setLastUpdated(new Date());
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => void refreshAll(), 5000);
    return () => window.clearInterval(timer);
  }, [refreshAll]);

  useEffect(() => {
    if (!notice) return;
    const timer = window.setTimeout(() => setNotice(""), 3200);
    return () => window.clearTimeout(timer);
  }, [notice]);

  const overview = useMemo(() => {
    const ready = providers.filter(
      (provider) => statusTone(snapshots[provider.id]) === "ready",
    ).length;
    const totalNodes = providers.reduce(
      (sum, provider) => sum + (snapshots[provider.id]?.nodes.length || 0),
      0,
    );
    const activeConnections = providers.reduce(
      (sum, provider) => sum + (snapshots[provider.id]?.status.active_connections || 0),
      0,
    );
    return { ready, total: providers.length, totalNodes, activeConnections };
  }, [snapshots]);

  async function queryEgress(provider: Provider) {
    setEgress((current) => ({
      ...current,
      [provider.id]: { label: "querying...", tone: "loading" },
    }));
    try {
      const data = await requestJson<{ ipinfo?: { ip?: string; country?: string; org?: string } }>(
        `${provider.api}/egress`,
      );
      const ip = data.ipinfo || {};
      setEgress((current) => ({
        ...current,
        [provider.id]: {
          label: [ip.ip, ip.country, ip.org].filter(Boolean).join(" / ") || "unknown",
          tone: "ok",
        },
      }));
    } catch (error) {
      setEgress((current) => ({
        ...current,
        [provider.id]: {
          label: error instanceof Error ? error.message : String(error),
          tone: "error",
        },
      }));
    }
  }

  async function toggleLogs(provider: Provider, key: keyof DebugFlags) {
    const current = await requestJson<DebugFlags>(`${provider.api}/debug`);
    const nextValue = !current[key];
    setSnapshots((snapshots) => {
      const snapshot = snapshots[provider.id];
      if (!snapshot) return snapshots;
      return {
        ...snapshots,
        [provider.id]: {
          ...snapshot,
          debug: { ...snapshot.debug, [key]: nextValue },
          status: {
            ...snapshot.status,
            ...(key === "proxy_logs"
              ? { debug_proxy_logs: nextValue }
              : { debug_health_checks: nextValue }),
          },
        },
      };
    });
    await requestJson(`${provider.api}/debug`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ [key]: nextValue }),
    });
    setNotice(`${provider.name} ${key.replace("_", " ")} ${nextValue ? "enabled" : "disabled"}`);
  }

  async function probeNode(provider: Provider, node: RelayNode) {
    setProbes((current) => ({
      ...current,
      [provider.id]: {
        ...current[provider.id],
        [node.id]: { id: node.id, name: node.name, error: "probing" },
      },
    }));
    const result = await requestJson<ProbeResult>(
      `${provider.api}/probe/${encodeURIComponent(node.id)}`,
    );
    setProbes((current) => ({
      ...current,
      [provider.id]: {
        ...current[provider.id],
        [node.id]: { ...result, name: node.name || result.name },
      },
    }));
  }

  async function probeAll(provider: Provider) {
    setScanning(true);
    try {
      const data = await requestJson<{ results?: ProbeResult[] }>(`${provider.api}/probes`);
      setProbes((current) => ({
        ...current,
        [provider.id]: Object.fromEntries(
          (data.results || []).map((result) => [result.id, result]),
        ),
      }));
      setNotice(`${provider.name} latency scan completed`);
    } finally {
      setScanning(false);
    }
  }

  return (
    <main className="min-h-screen overflow-hidden bg-[#071018] text-slate-100">
      <div className="fixed inset-0 -z-10 bg-[radial-gradient(circle_at_10%_0%,rgba(45,212,191,0.22),transparent_32%),radial-gradient(circle_at_86%_8%,rgba(74,222,128,0.15),transparent_26%),linear-gradient(145deg,#07100d_0%,#101914_54%,#0b100d_100%)]" />
      <div className="fixed inset-0 -z-10 opacity-[0.08] [background-image:linear-gradient(rgba(255,255,255,0.6)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.6)_1px,transparent_1px)] [background-size:44px_44px]" />

      {notice && (
        <div className="fixed right-5 top-4 z-20 rounded-2xl border border-cyan-200/30 bg-slate-950/90 px-4 py-3 text-sm font-bold text-cyan-100 shadow-xl shadow-black/30 backdrop-blur">
          {notice}
        </div>
      )}

      <header className="mx-auto flex w-full max-w-[1540px] flex-col gap-8 px-5 py-6 sm:px-8 lg:px-10">
        <div className="flex flex-col justify-between gap-6 lg:flex-row lg:items-end">
          <div className="max-w-4xl">
            <div className="mb-5 inline-flex rounded-full border border-cyan-200/20 bg-cyan-200/10 px-4 py-2 text-xs font-black uppercase tracking-[0.3em] text-cyan-100">
              RelayWay Control
            </div>
            <h1 className="text-5xl font-black tracking-[-0.06em] text-white sm:text-7xl">
              Provider command deck
            </h1>
            <p className="mt-4 max-w-2xl text-lg text-slate-300">
              Modern operations for RelayWay providers: live health, node switching, latency scans,
              debug toggles and filtered logs in one surface.
            </p>
          </div>

          <div className="flex flex-wrap gap-3">
            <button
              className="control-button-primary"
              disabled={refreshing}
              onClick={() => void refreshAll()}
              type="button"
            >
              {refreshing ? "Refreshing" : "Refresh all"}
            </button>
          </div>
        </div>

        <div className="grid gap-4 sm:grid-cols-4">
          <div className="hero-stat">
            <span>Ready providers</span>
            <strong>
              {overview.ready}/{overview.total}
            </strong>
          </div>
          <div className="hero-stat">
            <span>Total nodes</span>
            <strong>{overview.totalNodes}</strong>
          </div>
          <div className="hero-stat">
            <span>Active conns</span>
            <strong>{overview.activeConnections}</strong>
          </div>
          <div className="hero-stat">
            <span>Last updated</span>
            <strong>{lastUpdated ? lastUpdated.toLocaleTimeString() : "pending"}</strong>
          </div>
        </div>
      </header>

      <section className="mx-auto grid w-full max-w-[1540px] gap-4 px-5 sm:px-8 lg:grid-cols-2 lg:px-10">
        {providers.map((provider) => (
          <ProviderCard
            active={provider.id === selectedProvider.id}
            egress={egress[provider.id]}
            key={provider.id}
            onEgress={() => void queryEgress(provider)}
            onSelect={() => setSelectedProviderId(provider.id)}
            provider={provider}
            snapshot={snapshots[provider.id]}
          />
        ))}
      </section>

      <section className="mx-auto flex w-full max-w-[1540px] flex-col gap-6 px-5 py-6 sm:px-8 lg:px-10">
        <ConsumerPanel snapshot={selectedSnapshot} />
        <NodeWorkspace
          onProbe={(node) => void probeNode(selectedProvider, node)}
          onProbeAll={() => void probeAll(selectedProvider)}
          probes={probes[selectedProvider.id] || {}}
          provider={selectedProvider}
          scanning={scanning}
          snapshot={selectedSnapshot}
        />
        <LogsPanel
          onToggleLogs={(key) => void toggleLogs(selectedProvider, key)}
          snapshot={selectedSnapshot}
        />
      </section>
    </main>
  );
}
