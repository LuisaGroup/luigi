"use strict";
(() => {
  function id(key) { return document.getElementById(key); }
  function node(tag, text, className) {
    const result = document.createElement(tag);
    if (text !== undefined) result.textContent = text;
    if (className) result.className = className;
    return result;
  }
  function reason(text) {
    const labels = {
      blocked_by_fifo: "Waiting for FIFO head",
      waiting_for_other_queue: "Waiting for another required queue",
      reserved_by_a_running_job: "Reserved by a running job",
      timed_out: "Timed out",
      timeout: "Timed out"
    };
    return text && text !== "unknown"
      ? text.split(",").map(item => labels[item] || item.replace(/_/g, " ")).join(" · ")
      : "Unknown";
  }
  function time(value) {
    return value ? new Date(value).toLocaleString() : "Unknown";
  }
  function age(value) {
    const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
    if (seconds < 60) return seconds + "s";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m";
    return Math.floor(seconds / 3600) + "h";
  }
  function display(value) {
    return value === null ? "–" : value === true ? "Yes" : value === false ? "No" : String(value);
  }
  function empty(target, text) { target.replaceChildren(node("p", text, "empty")); }
  function connection(text, css) {
    id("connection").textContent = text;
    id("connection").className = "badge " + css;
  }
  const token = location.hash.slice(1);
  if (location.hash) history.replaceState(null, "", location.pathname);
  if (!/^[A-Za-z0-9_-]{43}$/.test(token)) {
    connection("No viewing session", "disconnected");
    id("observation").textContent = "Reopen the dashboard from the local Luigi launcher.";
    return;
  }
  function showQueues(items) {
    const target = id("queues");
    if (!items.length) return empty(target, "No queues reported.");
    target.replaceChildren(...items.map(item => {
      const card = node("article", undefined, "card");
      card.append(node("h3", item.name));
      const list = node("dl");
      for (const [label, value] of [["Pending", item.queued], ["Active", item.active], ["Held", item.held]]) {
        const itemNode = node("div");
        itemNode.append(node("dt", label), node("dd", display(value)));
        list.append(itemNode);
      }
      card.append(list, node("p", "Gate: " + reason(item.hold_reason), "note"));
      if (item.hold_since) card.append(node("p", "Held for " + age(item.hold_since), "note"));
      if (item.oldest_queued_at) card.append(node("p", "Oldest pending: " + age(item.oldest_queued_at), "note"));
      return card;
    }));
  }
  function showTelemetry(data) {
    const gpu = data.gpu.length ? data.gpu.map(item => [item.queue, item])
      : [["GPU", {percent: null, observed_at: null, reason: "no_sample"}]];
    const rows = [["CPU", data.cpu], ...gpu];
    id("telemetry").replaceChildren(...rows.map(([label, item]) => {
      const card = node("article", undefined, "card");
      const bad = /(stale|unavailable|no_sample)/.test(item.reason);
      const value = item.percent === null ? "Unknown" : item.percent + "%" + (bad ? " · last seen" : "");
      card.append(node("h3", label), node("p", value, "value"),
                  node("p", "Observed: " + time(item.observed_at), "note"),
                  node("p", "State: " + reason(item.reason), "note"));
      return card;
    }));
  }
  function showJobs(target, jobs, when, fallback) {
    if (!jobs.length) return empty(target, fallback);
    const list = node("ul", undefined, "jobs");
    for (const item of jobs) {
      const box = node("li", undefined, "job");
      const label = item.phase === "draining" ? "Waiting for child processes" : reason(item.status);
      const stamp = when === "started_at" && !item[when] ? "Not started" : time(item[when]);
      box.append(node("p", (item.queues.join(" + ") || "Unknown queue") + " · " + label),
                 node("code", item.id), node("small", stamp));
      if (item.never_started) box.append(node("small", "Never started"));
      list.append(box);
    }
    target.replaceChildren(list);
  }
  let inFlight = false;
  let last = null;
  async function refresh() {
    if (inFlight) return;
    inFlight = true;
    id("refresh").disabled = true;
    const controller = new AbortController();
    const deadline = setTimeout(() => controller.abort(), 3500);
    try {
      const response = await fetch("/api/snapshot", {
        method: "GET", headers: {"Authorization": "Bearer " + token},
        credentials: "omit", cache: "no-store", redirect: "error", signal: controller.signal
      });
      if (response.status === 401) {
        for (const key of ["queues", "telemetry", "active", "recent"]) empty(id(key), "No authorized snapshot.");
        last = null;
        throw new Error("auth");
      }
      if (!response.ok) throw new Error("unavailable");
      const data = await response.json();
      showQueues(data.queues);
      showTelemetry(data.telemetry);
      showJobs(id("active"), data.active, "started_at", "No active jobs.");
      showJobs(id("recent"), data.recent, "finished_at", "No recent results.");
      last = new Date().toLocaleString();
      id("observation").textContent = "Last snapshot: " + last;
      connection("Live", "live");
    } catch (_) {
      connection("Disconnected", "disconnected");
      id("observation").textContent = last
        ? "Showing the last snapshot from " + last + ". Data may be stale."
        : "No snapshot available. Verify the local broker and viewing session.";
    } finally {
      clearTimeout(deadline);
      id("refresh").disabled = false;
      inFlight = false;
    }
  }
  id("refresh").addEventListener("click", refresh);
  refresh();
  setInterval(refresh, 5000);
})();
