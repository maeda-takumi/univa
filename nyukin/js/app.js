(() => {
  const root = document.querySelector("[data-app]");
  if (!root) return;

  const data = JSON.parse(document.getElementById("app-data").textContent);
  const yen = new Intl.NumberFormat("ja-JP");
  const fmt = (n) => `${yen.format(n)}\u5186`;
  const byId = (id) => document.getElementById(id);
  const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const diffClass = (n) => (n > 0 ? "diff-pos" : n < 0 ? "diff-neg" : "diff-zero");
  const nameKey = (r) => String(r?.name || "").replace(/[\s\u3000]/g, "").toLowerCase();
  const sortName = (a, b) => nameKey(a).localeCompare(nameKey(b), "ja") || a.amount - b.amount || a.line - b.line;
  const t = {
    allPeriod: "\u5168\u671f\u9593",
    noDaily: "\u8a72\u5f53\u3059\u308b\u65e5\u5225\u30c7\u30fc\u30bf\u306f\u3042\u308a\u307e\u305b\u3093\u3002",
    none: "\u306a\u3057",
    detail: "\u8a73\u7d30\u3092\u898b\u308b",
    detailTitle: "\u306e\u5165\u91d1\u8a73\u7d30",
    db: "\u81ea\u793eDB",
    diff: "\u5dee\u984d",
    count: "\u4ef6",
    day: "\u65e5",
    noMatch: "\u8a72\u5f53\u306a\u3057",
    exact: "\u5b8c\u5168\u4e00\u81f4",
    destMismatch: "\u5165\u91d1\u5148\u9055\u3044",
    sameMail: "\u540c\u30e1\u30fc\u30eb\u8981\u78ba\u8a8d",
    sameNameAmount: "\u540c\u540d\u30fb\u540c\u984d\u8981\u78ba\u8a8d",
    noDb: "\u81ea\u793eDB\u306a\u3057",
    noTx: "UnivaPay\u306a\u3057",
    noCandidate: "\u3053\u306e\u6708\u306b\u8a72\u5f53\u3059\u308b\u5019\u88dc\u306f\u3042\u308a\u307e\u305b\u3093\u3002",
    reason: "\u7406\u7531",
    dbCandidate: "\u81ea\u793eDB\u5019\u88dc",
    sales: "\u30bb\u30fc\u30eb\u30b9\u62c5\u5f53",
  };

  if (root.dataset.app === "daily") initDaily();
  if (root.dataset.app === "mistake") initMistake();

  function initDaily() {
    const months = [...new Set(data.daily.map((r) => r.date.slice(0, 7)))].sort().reverse();
    byId("period").innerHTML = `<option value="all">${t.allPeriod}</option>${months.map((m) => `<option value="${m}">${m}</option>`).join("")}`;
    if (months.includes("2026-05")) byId("period").value = "2026-05";
    ["period", "dateSearch", "diffFilter"].forEach((id) => byId(id).addEventListener("input", renderDaily));
    byId("detailClose").addEventListener("click", () => byId("detailDialog").close());
    renderDaily();
  }

  function filteredDaily() {
    const period = byId("period").value;
    const query = byId("dateSearch").value.trim();
    const filter = byId("diffFilter").value;
    return data.daily.filter((r) => {
      if (period !== "all" && !r.date.startsWith(period)) return false;
      if (query && !r.date.includes(query)) return false;
      if (filter === "nonzero" && r.diff === 0) return false;
      if (filter === "positive" && r.diff <= 0) return false;
      if (filter === "negative" && r.diff >= 0) return false;
      return true;
    }).sort((a, b) => b.date.localeCompare(a.date));
  }

  function renderDaily() {
    const rows = filteredDaily();
    const dbTotal = rows.reduce((sum, r) => sum + r.dbTotal, 0);
    const txTotal = rows.reduce((sum, r) => sum + r.txTotal, 0);
    byId("dbTotal").textContent = fmt(dbTotal);
    byId("txTotal").textContent = fmt(txTotal);
    byId("diffTotal").textContent = fmt(txTotal - dbTotal);
    byId("diffTotal").className = diffClass(txTotal - dbTotal);
    byId("dbCount").textContent = `${rows.reduce((sum, r) => sum + r.dbCount, 0)}${t.count}`;
    byId("txCount").textContent = `${rows.reduce((sum, r) => sum + r.txCount, 0)}${t.count}`;
    byId("diffDays").textContent = `${rows.filter((r) => r.diff !== 0).length}${t.day}`;
    byId("rangeText").textContent = rows.length ? `${rows[rows.length - 1].date} - ${rows[0].date}` : t.noMatch;
    byId("dailyBody").innerHTML = rows.length ? rows.map((r) => `
      <tr class="clickable" data-date="${r.date}">
        <td><span class="pill ok">${r.date}</span></td>
        <td class="num">${fmt(r.dbTotal)}</td>
        <td class="num">${r.dbCount}</td>
        <td class="num">${fmt(r.txTotal)}</td>
        <td class="num">${r.txCount}</td>
        <td class="num ${diffClass(r.diff)}">${fmt(r.diff)}</td>
        <td><button type="button">${t.detail}</button></td>
      </tr>`).join("") : `<tr><td colspan="7" class="empty">${t.noDaily}</td></tr>`;
    document.querySelectorAll("#dailyBody tr[data-date]").forEach((row) => row.addEventListener("click", () => openDetail(row.dataset.date)));
  }

  function detailText(item, type) {
    if (!item) return t.noMatch;
    if (type === "tx") return `\u884c${item.line} / ${item.name || "-"} / ${item.email} / ${fmt(item.amount)} / ${item.method} / ${item.expectedDest}`;
    return `\u884c${item.line} / ${item.name || "-"} / ${item.email} / ${fmt(item.amount)} / ${item.dest} / ${item.status || "-"}`;
  }

  function buildLinks(txs, dbs) {
    const usedTx = new Set();
    const usedDb = new Set();
    const links = [];
    const add = (status, level, tx, db) => {
      if (tx) usedTx.add(tx.line);
      if (db) usedDb.add(db.line);
      links.push({ status, level, tx, db, sort: nameKey(tx || db) });
    };
    const findDb = (tx, pred) => dbs.find((db) => !usedDb.has(db.line) && pred(db, tx));
    txs.forEach((tx) => {
      const db = findDb(tx, (d, x) => d.email === x.email && d.amount === x.amount && d.dest === x.expectedDest);
      if (db) add(t.exact, "ok", tx, db);
    });
    txs.filter((tx) => !usedTx.has(tx.line)).forEach((tx) => {
      let db = findDb(tx, (d, x) => d.email === x.email && d.amount === x.amount);
      if (db) return add(t.destMismatch, "warn", tx, db);
      db = findDb(tx, (d, x) => d.email === x.email);
      if (db) return add(t.sameMail, "warn", tx, db);
      db = findDb(tx, (d, x) => nameKey(d) === nameKey(x) && d.amount === x.amount);
      if (db) return add(t.sameNameAmount, "warn", tx, db);
    });
    txs.filter((tx) => !usedTx.has(tx.line)).forEach((tx) => add(t.noDb, "bad", tx, null));
    dbs.filter((db) => !usedDb.has(db.line)).forEach((db) => add(t.noTx, "bad", null, db));
    return links.sort((a, b) => a.sort.localeCompare(b.sort, "ja"));
  }

  function openDetail(date) {
    const daily = data.daily.find((r) => r.date === date) || { dbTotal: 0, dbCount: 0, txTotal: 0, txCount: 0, diff: 0 };
    const txs = data.txEntries.filter((r) => r.date === date).sort(sortName);
    const dbs = data.dbEntries.filter((r) => r.date === date).sort(sortName);
    byId("detailTitle").textContent = `${date} ${t.detailTitle}`;
    byId("detailSummary").innerHTML = `<span class="pill ok">${t.db} ${fmt(daily.dbTotal)} / ${daily.dbCount}${t.count}</span><span class="pill ok">UnivaPay ${fmt(daily.txTotal)} / ${daily.txCount}${t.count}</span><span class="pill ${daily.diff === 0 ? "ok" : "bad"}">${t.diff} ${fmt(daily.diff)}</span>`;
    byId("detailLinkBody").innerHTML = buildLinks(txs, dbs).map((r) => `<tr><td><span class="status status-${r.level}">${r.status}</span></td><td>${esc(detailText(r.tx, "tx"))}</td><td>${esc(detailText(r.db, "db"))}</td></tr>`).join("");
    byId("detailTxBody").innerHTML = txs.length ? txs.map((r) => `<tr><td>${r.line}</td><td>${esc(r.name || "-")}</td><td>${esc(r.email)}</td><td class="num">${fmt(r.amount)}</td><td>${esc(r.method)}</td></tr>`).join("") : `<tr><td colspan="5" class="empty">${t.none}</td></tr>`;
    byId("detailDbBody").innerHTML = dbs.length ? dbs.map((r) => `<tr><td>${r.line}</td><td>${esc(r.name || "-")}</td><td>${esc(r.email)}</td><td class="num">${fmt(r.amount)}</td><td>${esc(r.dest)}</td></tr>`).join("") : `<tr><td colspan="5" class="empty">${t.none}</td></tr>`;
    byId("detailDialog").showModal();
  }

  function initMistake() {
    const months = data.months || [];
    byId("monthFilter").innerHTML = months.map((m) => `<option value="${m}">${m}</option>`).join("");
    if (months.includes("2026-05")) byId("monthFilter").value = "2026-05";
    const kinds = [...new Set(data.suggestions.filter((x) => x.tx).map((x) => x.kind))];
    byId("kindFilter").innerHTML += kinds.map((k) => `<option value="${esc(k)}">${esc(k)}</option>`).join("");
    ["monthFilter", "kindFilter", "confidenceFilter", "search"].forEach((id) => byId(id).addEventListener("input", renderMistakes));
    renderMistakes();
  }

  function label(row, source) {
    return row ? `${source}\u884c${row.line} / ${row.date} / ${row.name || "-"} / ${fmt(row.amount)} / ${row.email || ""} / ${row.method || row.dest || ""} / ${row.sales || ""}` : t.noMatch;
  }

  function candidateDetail(row, source) {
    if (!row) return `<div class="candidate-empty">${t.noMatch}</div>`;
    return `
      <div class="candidate-line">${esc(source)}\u884c${row.line}</div>
      <div class="candidate-main">
        <strong>${esc(row.name || "-")}</strong>
        <span>${esc(row.date || "-")}</span>
        <span class="candidate-amount">${fmt(row.amount || 0)}</span>
      </div>
      <div class="candidate-meta">
        <span>${esc(row.email || "-")}</span>
        <span>${esc(row.method || row.dest || "-")}</span>
        ${row.sales ? `<span class="candidate-sales">${t.sales}: ${esc(row.sales)}</span>` : ""}
      </div>`;
  }

  function filteredMistakes() {
    const month = byId("monthFilter").value;
    const kind = byId("kindFilter").value;
    const confidence = byId("confidenceFilter").value;
    const q = byId("search").value.trim().toLowerCase();
    return data.suggestions.filter((x) => x.tx && x.tx.date.startsWith(month)).filter((x) => {
      if (kind !== "all" && x.kind !== kind) return false;
      if (confidence !== "all" && x.confidence !== confidence) return false;
      if (q && !`${x.kind} ${x.fix} ${x.why} ${label(x.tx, "UnivaPay")} ${label(x.db, t.db)}`.toLowerCase().includes(q)) return false;
      return true;
    });
  }

  function renderMistakes() {
    const rows = filteredMistakes();
    byId("mistakeList").innerHTML = rows.length ? rows.map((x) => `
      <article class="candidate">
        <div class="candidate-head">
          <div class="candidate-kind">${esc(x.kind)}</div>
          <span class="status ${x.confidence === "\u9ad8" ? "status-bad" : x.confidence === "\u4e2d" ? "status-warn" : "status-ok"}">${esc(x.confidence)}</span>
          <div class="candidate-fix">${esc(x.fix)}</div>
        </div>
        <div class="candidate-body">
          <div class="candidate-side"><div class="candidate-title">UnivaPay</div>${candidateDetail(x.tx, "UnivaPay")}</div>
          <div class="candidate-side"><div class="candidate-title">${t.dbCandidate}</div>${candidateDetail(x.db, t.db)}</div>
        </div>
        <div class="candidate-why">${t.reason}: ${esc(x.why)}</div>
      </article>`).join("") : `<div class="empty">${t.noCandidate}</div>`;
  }
})();
