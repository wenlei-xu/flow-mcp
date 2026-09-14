import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";

const NAV = [
  ["overview", "◌", "概览"],
  ["accounts", "◎", "账号管理"],
  ["logs", "≡", "请求日志"],
  ["test", "＋", "测试页面"],
  ["settings", "⚙", "系统设置"],
];
const META = {
  overview: ["CONTROL ROOM", "概览", "账号池、服务和最近请求的统一状态。"],
  accounts: [
    "PROFILES",
    "账号管理",
    "启用且已登录的 Profile 会自动加入账号池。",
  ],
  logs: ["REQUEST LOGS", "请求日志", "查看每次请求的状态、结果和错误详情。"],
  test: ["LIVE TEST", "测试页面", "默认由账号池自动选择空闲账号。"],
  settings: ["CONTROL", "系统设置", "队列、超时、结果链接和统一入口配置。"],
};
function isOmniModel(modelId) {
  return String(modelId || "").toLowerCase().replaceAll("-", "_") === "omni_flash";
}

function durationOptionsForModel(modelId) {
  return isOmniModel(modelId) ? [4, 6, 8, 10] : [];
}

function AspectDurationControl({ model, value, onChange }) {
  if (!isOmniModel(model)) return <div className="sub">Veo：由 Flow 使用默认 8 秒</div>;
  return (
    <select id="testAspect" value={value} onChange={onChange}>
      {durationOptionsForModel(model).flatMap((seconds) =>
        ["9:16", "16:9"].map((ratio) => (
          <option key={`${ratio}|${seconds}`} value={`${ratio}|${seconds}`}>
            {ratio} · {seconds} 秒
          </option>
        )),
      )}
    </select>
  );
}

const FALLBACK_MODELS = {
  image: [
    { id: "nano2", name: "Nano Banana 2" },
    { id: "nano-pro", name: "Nano Banana Pro" },
    { id: "image4", name: "Imagen 4" },
  ],
  video: [
    { id: "veo_lite", name: "Veo Lite" },
    { id: "veo_fast", name: "Veo Fast" },
    { id: "veo_quality", name: "Veo Quality" },
    { id: "omni_flash", name: "Veo Omni Flash" },
  ],
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    ...options,
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok)
    throw new Error(
      body.detail || body.message || `请求失败 (${response.status})`,
    );
  return body;
}

function replaceIfChanged(current, next) {
  return JSON.stringify(current) === JSON.stringify(next) ? current : next;
}

const timeLabel = (value) =>
  value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
function statusInfo(status) {
  if (["succeeded", "completed"].includes(status)) return ["done", "已完成"];
  if (status === "failed") return ["fail", "失败"];
  if (status === "timed_out") return ["fail", "超时"];
  if (status === "cancelled") return ["fail", "已取消"];
  if (["indeterminate", "interrupted"].includes(status))
    return ["fail", "需核对"];
  if (status === "running") return ["run", "生成中"];
  if (status === "cancelling") return ["wait", "取消中"];
  return ["wait", "排队中"];
}
function Empty({ children }) {
  return <p className="muted empty-state">{children}</p>;
}
function Section({ title, action, children, className = "" }) {
  return (
    <section className={`section ${className}`}>
      {(title || action) && (
        <div className="section-head">
          <h2>{title}</h2>
          {action}
        </div>
      )}
      {children}
    </section>
  );
}
function Button({ children, className = "", ...props }) {
  return (
    <button className={`btn ${className}`} {...props}>
      {children}
    </button>
  );
}

export default function App() {
  const queryClient = useQueryClient();
  const [view, setView] = useState("overview");
  const [live, setLive] = useState(false);
  const [healthError, setHealthError] = useState("");
  const [authRequired, setAuthRequired] = useState(false);
  const [authenticated, setAuthenticated] = useState(true);
  const [loginForm, setLoginForm] = useState({
    username: "admin",
    password: "",
  });
  const [profiles, setProfiles] = useState([]);
  const [selectedProfile, setSelectedProfile] = useState("auto");
  const [profileInfo, setProfileInfo] = useState({});
  const [stats, setStats] = useState({
    profiles: 0,
    enabled_profiles: 0,
    available_profiles: 0,
    busy_profiles: 0,
    queued: 0,
    running: 0,
    completed: 0,
    failed: 0,
  });
  const [logs, setLogs] = useState([]);
  const [logFilter, setLogFilter] = useState("all");
  const [logDetail, setLogDetail] = useState(null);
  const [projects, setProjects] = useState([]);
  const [models, setModels] = useState(FALLBACK_MODELS);
  const [toast, setToast] = useState("");
  const [kind, setKind] = useState("video");
  const [mode, setMode] = useState("t2v");
  const [prompt, setPrompt] = useState(
    "一只奶油色小狗在温暖的客厅地垫上发现旧玩具碎片，随后好奇地看向镜头。",
  );
  const [model, setModel] = useState("veo_fast");
  const [aspectDuration, setAspectDuration] = useState("9:16|8");
  const [projectId, setProjectId] = useState("");
  const [uploaded, setUploaded] = useState([]);
  const [submitting, setSubmitting] = useState(false);
  const [lastTestId, setLastTestId] = useState("");
  const [lastTest, setLastTest] = useState(null);
  const [projectManager, setProjectManager] = useState(null);
  const [projectPool, setProjectPool] = useState([]);
  const [projectCatalog, setProjectCatalog] = useState([]);
  const [projectBusy, setProjectBusy] = useState(false);
  const [manualProjectId, setManualProjectId] = useState("");
  const [manualProjectTitle, setManualProjectTitle] = useState("");
  const [config, setConfig] = useState({
    queue_workers: 4,
    image_workers: 8,
    video_workers: 4,
    generation_timeout_seconds: 1800,
    media_url_ttl_seconds: 86400,
    asset_retention_seconds: 604800,
    generation_rate_capacity: 8,
    generation_rate_refill_seconds: 20,
    public_base_url: "",
    mcp_upstream_url: "",
  });

  const authQuery = useQuery({
    queryKey: ["studio", "auth"],
    queryFn: () => api("/api/auth/status"),
  });
  const authenticatedQuery = !authQuery.data?.required || Boolean(authQuery.data?.authenticated);
  const profilesQuery = useQuery({
    queryKey: ["studio", "profiles"],
    queryFn: () => api("/api/profiles"),
    enabled: authenticatedQuery,
  });
  const statsQuery = useQuery({
    queryKey: ["studio", "stats"],
    queryFn: () => api("/api/stats"),
    enabled: authenticatedQuery,
  });
  const logsQuery = useQuery({
    queryKey: ["studio", "logs"],
    queryFn: () => api("/api/logs?limit=100"),
    enabled: authenticatedQuery,
  });
  const modelsQuery = useQuery({
    queryKey: ["studio", "models"],
    queryFn: () => api("/api/models"),
    enabled: authenticatedQuery,
    staleTime: 30000,
  });
  const configQuery = useQuery({
    queryKey: ["studio", "config"],
    queryFn: () => api("/api/config"),
    enabled: authenticatedQuery,
    staleTime: 30000,
  });
  const healthQuery = useQuery({
    queryKey: ["studio", "health"],
    queryFn: () => api("/api/health"),
  });
  const fileRef = useRef(null);
  const toastTimer = useRef(null);
  const notify = useCallback((message) => {
    setToast(message);
    clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(() => setToast(""), 2800);
  }, []);

  const loadProfiles = useCallback(async () => {
    const payload = await api("/api/profiles");
    const next = payload.profiles || [];
    setProfiles((current) => replaceIfChanged(current, next));
    setSelectedProfile((current) =>
      current === "auto" || next.some((item) => item.name === current)
        ? current
        : "auto",
    );
  }, []);
  const loadStats = useCallback(
    async () => {
      const next = await api("/api/stats");
      setStats((current) => replaceIfChanged(current, next));
    },
    [],
  );
  const loadLogs = useCallback(
    async () => {
      const next = (await api("/api/logs?limit=100")).logs || [];
      setLogs((current) => replaceIfChanged(current, next));
    },
    [],
  );
  const loadModels = useCallback(async () => {
    try {
      const result = await api("/api/models");
      setModels({
        image: result.image?.length ? result.image : FALLBACK_MODELS.image,
        video: result.video?.length ? result.video : FALLBACK_MODELS.video,
      });
    } catch {
      /* fallback models are enough while the adapter starts */
    }
  }, []);
  const loadConfig = useCallback(async () => {
    try {
      const payload = await api("/api/config");
      setConfig((current) => ({ ...current, ...payload }));
    } catch {
      /* auth gate or cold start */
    }
  }, []);
  const refreshProfile = useCallback(async (name) => {
    if (!name || name === "auto") return;
    const credits = await Promise.allSettled([
      api(`/api/profiles/${encodeURIComponent(name)}/credits`),
    ]);
    setProfileInfo((current) => ({
      ...current,
      [name]: {
        credits: credits[0].status === "fulfilled" ? credits[0].value.result : null,
        checkedAt: new Date().toISOString(),
      },
    }));
  }, []);
  const loadProjects = useCallback(async (name) => {
    if (!name || name === "auto") return setProjects([]);
    try {
      setProjects(
        (await api(`/api/projects?profile=${encodeURIComponent(name)}`)).result
          ?.projects || [],
      );
    } catch {
      setProjects([]);
    }
  }, []);

  useEffect(() => {
    if (!authQuery.data) return;
    setAuthRequired(Boolean(authQuery.data.required));
    setAuthenticated(Boolean(authQuery.data.authenticated));
  }, [authQuery.data]);

  useEffect(() => {
    if (healthQuery.data) {
      setLive(true);
      setHealthError("");
    }
    if (healthQuery.error) {
      setLive(false);
      setHealthError(healthQuery.error.message);
    }
  }, [healthQuery.data, healthQuery.error]);

  useEffect(() => {
    const next = profilesQuery.data?.profiles;
    if (!next) return;
    setProfiles((current) => replaceIfChanged(current, next));
    setSelectedProfile((current) =>
      current === "auto" || next.some((item) => item.name === current)
        ? current
        : "auto",
    );
  }, [profilesQuery.data]);

  useEffect(() => {
    if (statsQuery.data) setStats((current) => replaceIfChanged(current, statsQuery.data));
  }, [statsQuery.data]);

  useEffect(() => {
    const next = logsQuery.data?.logs || [];
    if (logsQuery.data) setLogs((current) => replaceIfChanged(current, next));
  }, [logsQuery.data]);

  useEffect(() => {
    const result = modelsQuery.data;
    if (!result) return;
    setModels({
      image: result.image?.length ? result.image : FALLBACK_MODELS.image,
      video: result.video?.length ? result.video : FALLBACK_MODELS.video,
    });
  }, [modelsQuery.data]);

  useEffect(() => {
    if (configQuery.data) setConfig((current) => ({ ...current, ...configQuery.data }));
  }, [configQuery.data]);
  useEffect(() => {
    setProjectId("");
    loadProjects(selectedProfile);
    refreshProfile(selectedProfile);
  }, [loadProjects, refreshProfile, selectedProfile]);

  const activeProfiles = useMemo(
    () =>
      profiles.filter(
        (item) => item.enabled && !item.pending_login && item.cookies_present,
      ),
    [profiles],
  );
  const currentProfile = useMemo(
    () => profiles.find((item) => item.name === selectedProfile),
    [profiles, selectedProfile],
  );
  const modelOptions = models[kind] || [];
  const selectedModel = modelOptions.find((item) => item.id === model);
  const durationOptions = durationOptionsForModel(model);
  const [aspect, duration] = aspectDuration.split("|");
  useEffect(() => {
    if (kind !== "video" || !isOmniModel(model) || durationOptions.includes(Number(duration))) return;
    setAspectDuration(`${aspect}|${durationOptions[0]}`);
  }, [aspect, duration, durationOptions, kind, model]);
  const visibleLogs = useMemo(
    () =>
      logFilter === "all"
        ? logs
        : logs.filter((item) => item.status === logFilter),
    [logFilter, logs],
  );
  const switchView = (next) => {
    setView(next);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };
  const switchKind = (next) => {
    setKind(next);
    setModel((models[next] || FALLBACK_MODELS[next])[0]?.id || "");
    if (next === "image") setMode("t2v");
  };

  async function addFiles(fileList) {
    for (const file of [...(fileList || [])]) {
      const form = new FormData();
      form.append("file", file);
      try {
        const item = await api("/api/assets", { method: "POST", body: form });
        setUploaded((current) => [
          ...current,
          { asset_id: item.asset_id, filename: item.filename },
        ]);
        notify(`${file.name} 已上传，提交时只引用 asset_id`);
      } catch (error) {
        notify(`${file.name} 上传失败：${error.message}`);
      }
    }
  }
  async function submitTest(event) {
    event.preventDefault();
    if (selectedProfile !== "auto" && !currentProfile?.enabled)
      return notify("当前账号已禁用，请先启用");
    if (!prompt.trim()) return notify("请先填写提示词");
    if (kind === "video" && mode !== "t2v" && !uploaded.length)
      return notify("图生视频需要至少一张参考图");
    if (kind === "video" && isOmniModel(model) && !durationOptions.includes(Number(duration)))
      return notify(`${selectedModel?.name || model} 当前只支持 ${durationOptions.join("、")} 秒`);
    setSubmitting(true);
    setLastTest(null);
    try {
      const result = await api("/api/generations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kind,
          prompt: prompt.trim(),
          profile: selectedProfile === "auto" ? null : selectedProfile,
          project: selectedProfile === "auto" ? null : projectId || null,
          model,
          requested_model: model,
          model_label: selectedModel?.name || model,
          aspect,
          ...(isOmniModel(model) ? { duration: Number(duration) } : {}),
          count: 1,
          mode: kind === "image" ? "t2v" : uploaded.length ? mode : "t2v",
          input_asset_ids: uploaded.map((item) => item.asset_id),
          reference_images: [],
          wait: false,
        }),
      });
      setLastTestId(result.id);
      setLastTest(result);
      notify(`测试任务已提交：${result.id.slice(0, 8)}`);
      await loadLogs();
    } catch (error) {
      notify(`测试提交失败：${error.message}`);
    } finally {
      setSubmitting(false);
    }
  }
  async function showDetail(id) {
    try {
      setLogDetail(
        (await api(`/api/logs/${encodeURIComponent(id)}`)).log || null,
      );
    } catch (error) {
      notify(`日志详情读取失败：${error.message}`);
    }
  }
  async function clearLogs() {
    if (
      !window.confirm("确认清除本地请求日志吗？gflow-cli 原始目录不会被删除。")
    )
      return;
    try {
      await api("/api/logs", { method: "DELETE" });
      await loadLogs();
      notify("本地请求日志已清除");
    } catch (error) {
      notify(`清除日志失败：${error.message}`);
    }
  }
  async function login(event) {
    event.preventDefault();
    try {
      await api("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(loginForm),
      });
      setAuthenticated(true);
      setHealthError("");
      notify("已登录管理后台");
      await Promise.all([
        loadProfiles(),
        loadStats(),
        loadLogs(),
        loadModels(),
        loadConfig(),
      ]);
    } catch (error) {
      notify(`登录失败：${error.message}`);
    }
  }
  async function cancelTask(id) {
    try {
      await api(`/api/generations/${encodeURIComponent(id)}/cancel`, {
        method: "POST",
      });
      await loadLogs();
      notify("任务已取消");
    } catch (error) {
      notify(`取消失败：${error.message}`);
    }
  }
  async function retryTask(id) {
    try {
      const task = await api(
        `/api/generations/${encodeURIComponent(id)}/retry`,
        { method: "POST" },
      );
      await loadLogs();
      notify(`已创建重试任务：${task.id.slice(0, 8)}`);
    } catch (error) {
      notify(`重试失败：${error.message}`);
    }
  }
  async function saveConfig(event) {
    event.preventDefault();
    try {
      const updated = await api("/api/config", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      setConfig(updated);
      notify("系统设置已保存");
    } catch (error) {
      notify(`设置保存失败：${error.message}`);
    }
  }
  async function addProfile() {
    const name = window.prompt("输入新的 Profile 名称");
    if (!name) return;
    const remark = window.prompt("输入备注（可选）") || "";
    try {
      await api("/api/profiles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, remark }),
      });
      notify("Chrome 登录窗口已启动，请完成 Google 登录");
      setTimeout(loadProfiles, 1800);
    } catch (error) {
      notify(`添加账号失败：${error.message}`);
    }
  }
  async function relogin(name) {
    try {
      await api(`/api/profiles/${encodeURIComponent(name)}/login`, {
        method: "POST",
      });
      notify(`${name} 的 Chrome 登录窗口已启动`);
      setTimeout(loadProfiles, 1800);
    } catch (error) {
      notify(`重新登录失败：${error.message}`);
    }
  }
  async function updateProfile(name, changes, message) {
    try {
      await api(`/api/profiles/${encodeURIComponent(name)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(changes),
      });
      await loadProfiles();
      notify(message);
    } catch (error) {
      notify(`账号更新失败：${error.message}`);
    }
  }
  async function editProfile(item) {
    const name = window.prompt("Profile 名称", item.name);
    if (!name) return;
    const remark =
      (window.prompt("备注", item.remark || "") ?? item.remark) || "";
    await updateProfile(
      item.name,
      { new_name: name, remark },
      "账号信息已更新",
    );
  }
  async function deleteProfile(item) {
    if (
      !window.confirm(
        `确认删除 Profile「${item.name}」吗？这会删除本地 Chrome 登录数据。`,
      )
    )
      return;
    try {
      await api(`/api/profiles/${encodeURIComponent(item.name)}`, {
        method: "DELETE",
      });
      if (selectedProfile === item.name) setSelectedProfile("auto");
      await loadProfiles();
      notify("Profile 已删除");
    } catch (error) {
      notify(`删除失败：${error.message}`);
    }
  }
  async function refreshCredits(name) {
    await refreshProfile(name);
    await loadProfiles();
    notify(`${name} 额度已刷新；账号将在真实任务中验证`);
  }
  async function setDefault(name) {
    try {
      await api(`/api/profiles/${encodeURIComponent(name)}/default`, {
        method: "POST",
      });
      await loadProfiles();
      notify(`${name} 已设为默认账号`);
    } catch (error) {
      notify(`设置默认账号失败：${error.message}`);
    }
  }
  async function openProjects(item) {
    setProjectManager(item);
    setProjectBusy(true);
    try {
      const payload = await api(
        `/api/profiles/${encodeURIComponent(item.name)}/projects`,
      );
      setProjectPool(payload.projects || []);
      setProjectCatalog(payload.catalog || []);
    } catch (error) {
      setProjectPool([]);
      setProjectCatalog([]);
      notify(`项目池读取失败：${error.message}`);
    } finally {
      setProjectBusy(false);
    }
  }
  async function updateProject(projectId, changes, message) {
    if (!projectManager) return;
    try {
      await api(
        `/api/profiles/${encodeURIComponent(projectManager.name)}/projects/${encodeURIComponent(projectId)}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(changes),
        },
      );
      await openProjects(projectManager);
      notify(message);
    } catch (error) {
      notify(`项目池更新失败：${error.message}`);
    }
  }
  async function addProjectToPool(project) {
    if (!projectManager) return;
    try {
      await api(
        `/api/profiles/${encodeURIComponent(projectManager.name)}/projects`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            project_id: project.project_id,
            title: project.title || project.project_id,
            enabled: true,
            is_default: projectPool.length === 0,
          }),
        },
      );
      await openProjects(projectManager);
      notify("项目已加入账号池");
    } catch (error) {
      notify(`加入项目池失败：${error.message}`);
    }
  }
  async function addManualProject() {
    const projectId = manualProjectId.trim();
    if (!projectManager || !projectId) {
      notify("请输入 Flow 项目 ID");
      return;
    }
    await addProjectToPool({
      project_id: projectId,
      title: manualProjectTitle.trim() || projectId,
    });
    setManualProjectId("");
    setManualProjectTitle("");
  }

  async function createProjectForProfile() {
    if (!projectManager) return;
    const title = window.prompt(
      "输入新的 Flow 项目名称",
      `${projectManager.name} project`,
    );
    if (!title) return;
    try {
      await api("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ profile: projectManager.name, title }),
      });
      await openProjects(projectManager);
      notify("Flow 项目已创建并加入项目池");
    } catch (error) {
      notify(`创建项目失败：${error.message}`);
    }
  }
  async function removeProject(projectId) {
    if (
      !projectManager ||
      !window.confirm("确认从账号池移除这个项目吗？不会删除 Flow 中的项目。")
    )
      return;
    try {
      await api(
        `/api/profiles/${encodeURIComponent(projectManager.name)}/projects/${encodeURIComponent(projectId)}`,
        { method: "DELETE" },
      );
      await openProjects(projectManager);
      notify("项目已移出账号池");
    } catch (error) {
      notify(`移除项目失败：${error.message}`);
    }
  }

  function topbar() {
    function refreshAll() {
      void queryClient.invalidateQueries({ queryKey: ["studio"] });
      notify("数据已刷新");
    }
    return (
      <header className="topbar">
        <div className="crumb">
          gflow studio / <b>{META[view][1]}</b>
        </div>
        <div className="top-actions">
          <span className={`health ${healthError ? "health-error" : ""}`}>
            <i className="dot" />
            {live ? "适配层在线" : healthError ? "适配层离线" : "连接中"}
          </span>
          <span className="pool-chip">
            账号池 · {stats.available_profiles ?? activeProfiles.length} 可用 /{" "}
            {stats.busy_profiles ?? 0} 忙碌
          </span>
          <Button className="ghost refresh-button" onClick={refreshAll} title="刷新工作台数据">
            ↻ 刷新
          </Button>
          <div className="avatar">AP</div>
        </div>
      </header>
    );
  }
  function Overview() {
    return (
      <>
        <PageHead
          page="overview"
          action={
            <Button className="primary" onClick={() => switchView("test")}>
              ＋ 开始测试
            </Button>
          }
        />
        <div className="stats">
          <Stat
            label="账号池可用"
            value={stats.available_profiles ?? activeProfiles.length}
            note={`启用 ${stats.enabled_profiles ?? activeProfiles.length} · 忙碌 ${stats.busy_profiles ?? 0} · 实际并发 图 ${stats.image_account_capacity ?? 0} / 视 ${stats.video_account_capacity ?? 0}`}
          />
          <Stat
            label="运行中 / 排队"
            value={(stats.running || 0) + (stats.queued || 0)}
            note={`运行中 ${stats.running || 0} · 排队 ${stats.queued || 0}`}
          />
          <Stat
            label="已完成"
            value={stats.completed || 0}
            note="本地任务记录"
          />
          <Stat
            label="失败 / 需核对"
            value={stats.failed || 0}
            note="查看请求日志"
          />
        </div>
        <div className="layout-2">
          <Section
            title="最近请求"
            action={
              <button className="link" onClick={() => switchView("logs")}>
                查看全部 →
              </button>
            }
          >
            <div className="task-list">
              {logs.slice(0, 5).map((item) => (
                <TaskRow item={item} key={item.id} />
              ))}
              {!logs.length && <Empty>暂无请求记录。</Empty>}
            </div>
          </Section>
          <Section
            title="账号池"
            action={
              <button className="link" onClick={() => switchView("accounts")}>
                管理账号 →
              </button>
            }
          >
            <div className="section-body">
              <div className="setting-list">
                <div className="setting">
                  <div>
                    <strong>自动调度</strong>
                    <p>普通请求从启用且已登录的 Profile 中选择</p>
                  </div>
                  <span className="status done">
                    {stats.available_profiles ?? activeProfiles.length} 可用
                  </span>
                </div>
                <div className="setting">
                  <div>
                    <strong>并行方式</strong>
                    <p>不同 Profile 可并行，同一 Profile 内部串行</p>
                  </div>
                  <strong>{stats.busy_profiles ?? 0} 忙碌</strong>
                </div>
                <div className="setting">
                  <div>
                    <strong>成员状态</strong>
                    <p>
                      待登录{" "}
                      {profiles.filter((item) => item.pending_login).length} 个
                    </p>
                  </div>
                  <button
                    className="link"
                    onClick={() => switchView("accounts")}
                  >
                    查看 →
                  </button>
                </div>
              </div>
            </div>
          </Section>
        </div>
      </>
    );
  }
  function Accounts() {
    return (
      <>
        <PageHead
          page="accounts"
          action={
            <Button className="primary" onClick={addProfile}>
              ＋ 添加账号
            </Button>
          }
        />
        <Section
          title="账号列表"
          action={<span className="muted">{profiles.length} 个 Profile</span>}
        >
          {profiles.map((item) => {
            const credits =
              profileInfo[item.name]?.credits?.credits ??
              profileInfo[item.name]?.credits?.balance ??
              profileInfo[item.name]?.credits?.remaining_credits;
            const authLabel = item.cookies_present
              ? "真实任务时验证"
              : "等待登录";
            return (
              <div
                className={`account-row ${!item.enabled ? "disabled-row" : ""}`}
                key={item.name}
              >
                <div className="person">
                  <div className="mini-avatar">
                    {item.name.slice(0, 2).toUpperCase()}
                  </div>
                  <div>
                    <strong>{item.name}</strong>
                    <div className="sub">
                      {item.google_account ||
                        (item.pending_login
                          ? "等待完成 Google 登录"
                          : "Google 账号未识别")}
                      {item.remark ? ` · ${item.remark}` : ""}
                    </div>
                    <div className="sub">
                      {item.last_used_at
                        ? `最近使用 ${timeLabel(item.last_used_at)}`
                        : "尚未使用"}{" "}
                      · {authLabel}
                    </div>
                  </div>
                </div>
                <div>
                  <strong>{credits ?? "—"}</strong>
                  <div className="sub">可用额度</div>
                </div>
                <div>
                  <span
                    className={`status ${item.enabled ? (item.cookies_present ? "done" : "run") : "fail"}`}
                  >
                    {item.enabled
                      ? item.cookies_present
                        ? "账号池成员"
                        : "待登录"
                      : "已禁用"}
                  </span>
                  <div className="sub">
                    {item.is_default ? "默认账号" : "可切换"}
                  </div>
                </div>
                <div>
                  <strong>
                    图 {item.image_inflight ?? 0}/{item.effective_image_concurrency ?? 1}
                    {" · "}
                    视 {item.video_inflight ?? 0}/{item.effective_video_concurrency ?? 1}
                  </strong>
                  <div className="sub">账号并发 · 排队 {item.queued_tasks ?? 0}</div>
                </div>
                <div className="account-actions">
                  <Button onClick={() => refreshCredits(item.name)}>刷新额度</Button>
                  <Button onClick={() => relogin(item.name)}>重登</Button>
                  <Button onClick={() => relogin(item.name)}>打开 Chrome</Button>
                   <Button onClick={() => openProjects(item)}>项目池</Button>
                   <Button onClick={() => openProjects(item)}>项目池</Button>
                  <Button onClick={() => editProfile(item)}>编辑</Button>
                  <Button
                    onClick={() =>
                      updateProfile(
                        item.name,
                        { enabled: !item.enabled },
                        item.enabled ? "账号已禁用" : "账号已启用",
                      )
                    }
                  >
                    {item.enabled ? "禁用" : "启用"}
                  </Button>
                  <Button onClick={() => deleteProfile(item)}>删除</Button>
                  {!item.is_default && item.enabled && !item.pending_login && (
                    <Button onClick={() => setDefault(item.name)}>
                      设为默认
                    </Button>
                  )}
                </div>
              </div>
            );
          })}
          {!profiles.length && <Empty>暂无 Profile，请先添加账号。</Empty>}
        </Section>
        {projectManager && (
          <div
            className="modal-backdrop"
            onClick={() => setProjectManager(null)}
          >
            <div
              className="modal-panel project-modal"
              onClick={(event) => event.stopPropagation()}
            >
              <div className="section-head">
                <div>
                  <h2>{projectManager.name} · 项目池</h2>
                  <p className="muted">
                    自动任务只会使用启用项目；默认项目优先，其余项目按使用次数轮换。
                  </p>
                </div>
                <div className="project-head-actions">
                  <Button onClick={createProjectForProfile}>＋ 新建项目</Button>
                  <button
                    className="link"
                    onClick={() => setProjectManager(null)}
                  >
                    关闭
                  </button>
                </div>
              </div>
              <div className="section-body">
                {projectBusy ? (
                  <Empty>正在读取 Flow 项目…</Empty>
                ) : (
                  <>
                                         <div className="form-grid project-manual-form">
                       <label>已有项目 ID <input value={manualProjectId} onChange={(event) => setManualProjectId(event.target.value)} placeholder="例如 6aa3ed09-8eff-486b-9674-b3f2329728c0" /></label>
                       <label>项目名称（可选） <input value={manualProjectTitle} onChange={(event) => setManualProjectTitle(event.target.value)} placeholder="迁移项目" /></label>
                       <Button className="primary" onClick={addManualProject}>加入已有项目</Button>
                     </div>

                                           <div className="project-list">
                       {projectPool.map((project) => (
                        <div className="project-row" key={project.project_id}>
                          <div>
                            <strong>
                              {project.title || project.project_id}
                            </strong>
                            <div className="sub">
                              {project.project_id} · 使用 {project.use_count} 次
                            </div>
                          </div>
                          <div className="project-actions">
                            <span
                              className={`status ${project.enabled ? "done" : "fail"}`}
                            >
                              {project.is_default
                                ? "默认"
                                : project.enabled
                                  ? "启用"
                                  : "停用"}
                            </span>
                            {project.enabled && !project.is_default && (
                              <Button
                                onClick={() =>
                                  updateProject(
                                    project.project_id,
                                    { is_default: true },
                                    "默认项目已更新",
                                  )
                                }
                              >
                                设为默认
                              </Button>
                            )}
                            {project.is_default && (
                              <Button
                                onClick={() =>
                                  updateProject(
                                    project.project_id,
                                    { is_default: false },
                                    "默认项目已取消",
                                  )
                                }
                              >
                                取消默认
                              </Button>
                            )}
                            <Button
                              onClick={() =>
                                updateProject(
                                  project.project_id,
                                  { enabled: !project.enabled },
                                  project.enabled ? "项目已停用" : "项目已启用",
                                )
                              }
                            >
                              {project.enabled ? "停用" : "启用"}
                            </Button>
                            <Button
                              onClick={() => removeProject(project.project_id)}
                            >
                              移除
                            </Button>
                          </div>
                        </div>
                      ))}
                      {!projectPool.length && <Empty>暂无项目池配置。</Empty>}
                    </div>
                    <div className="catalog-head">
                      <strong>Flow 项目目录</strong>
                      <span className="muted">
                        加入项目池不会删除 Flow 项目
                      </span>
                    </div>
                    <div className="project-list catalog-list">
                      {projectCatalog
                        .filter(
                          (project) =>
                            !projectPool.some(
                              (item) => item.project_id === project.project_id,
                            ),
                        )
                        .map((project) => (
                          <div className="project-row" key={project.project_id}>
                            <div>
                              <strong>
                                {project.title || project.project_id}
                              </strong>
                              <div className="sub">{project.project_id}</div>
                            </div>
                            <Button onClick={() => addProjectToPool(project)}>
                              加入池
                            </Button>
                          </div>
                        ))}
                      {!projectCatalog.length && (
                        <Empty>没有读取到 Flow 项目。</Empty>
                      )}
                    </div>
                  </>
                )}
              </div>
            </div>
          </div>
        )}
      </>
    );
  }
  function Logs() {
    return (
      <>
        <PageHead
          page="logs"
          action={
            <div className="form-actions">
              <Button onClick={loadLogs}>刷新</Button>
              <Button
                onClick={() =>
                  window.open("/api/logs/export?format=csv", "_blank")
                }
              >
                导出 CSV
              </Button>
              <Button onClick={clearLogs}>清除本地日志</Button>
            </div>
          }
        />
        <Section
          title="请求记录"
          action={
            <select
              value={logFilter}
              onChange={(event) => setLogFilter(event.target.value)}
            >
              <option value="all">全部状态</option>
              <option value="queued">排队中</option>
              <option value="running">生成中</option>
              <option value="succeeded">已完成</option>
              <option value="failed">失败</option>
              <option value="timed_out">超时</option>
              <option value="cancelled">已取消</option>
              <option value="indeterminate">需核对</option>
            </select>
          }
        >
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>账号</th>
                  <th>类型 / 模型</th>
                  <th>状态</th>
                  <th>错误</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {visibleLogs.map((item) => {
                  const state = statusInfo(item.status);
                  const canRetry =
                    [
                      "failed",
                      "timed_out",
                      "cancelled",
                      "interrupted",
                      "indeterminate",
                    ].includes(item.status) && item.source === "gflow-studio";
                  const canCancel =
                    ["queued", "running"].includes(item.status) &&
                    item.source === "gflow-studio";
                  return (
                    <tr key={item.id}>
                      <td className="muted">{timeLabel(item.created_at)}</td>
                      <td>{item.profile || "账号池待分配"}</td>
                      <td>
                        {item.kind === "image" ? "图片" : "视频"}
                        <div className="sub">{item.model || "—"}</div>
                      </td>
                      <td>
                        <span className={`status ${state[0]}`}>{state[1]}</span>
                      </td>
                      <td className="error-cell">{item.error || "—"}</td>
                      <td>
                        <button
                          className="link"
                          onClick={() => showDetail(item.id)}
                        >
                          详情
                        </button>
                        {canRetry && (
                          <button
                            className="link result-link"
                            onClick={() => retryTask(item.id)}
                          >
                            重试
                          </button>
                        )}
                        {canCancel && (
                          <button
                            className="link result-link"
                            onClick={() => cancelTask(item.id)}
                          >
                            取消
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
                {!visibleLogs.length && (
                  <tr>
                    <td colSpan="6">
                      <Empty>暂无匹配的请求记录。</Empty>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Section>
        {logDetail && (
          <DetailModal item={logDetail} close={() => setLogDetail(null)} />
        )}
      </>
    );
  }
  function Test() {
    const result = lastTestId
      ? logs.find((item) => item.id === lastTestId) || lastTest
      : lastTest;
    const files = result?.result?.files || [];
    return (
      <>
        <PageHead
          page="test"
          action={
            <span className={`status ${live ? "done" : "fail"}`}>
              {live ? "适配层可用" : "适配层不可用"}
            </span>
          }
        />
        <div className="layout-2">
          <Section className="create-panel">
            <div className="section-body">
              <div className="mode-tabs">
                <button
                  type="button"
                  className={`mode-tab ${kind === "video" ? "active" : ""}`}
                  onClick={() => switchKind("video")}
                >
                  视频
                </button>
                <button
                  type="button"
                  className={`mode-tab ${kind === "image" ? "active" : ""}`}
                  onClick={() => switchKind("image")}
                >
                  图片
                </button>
              </div>
              <form onSubmit={submitTest}>
                <div className="form-grid">
                  <div className="field">
                    <label htmlFor="testProfile">账号来源</label>
                    <select
                      id="testProfile"
                      value={selectedProfile}
                      onChange={(event) =>
                        setSelectedProfile(event.target.value)
                      }
                    >
                      <option value="auto">自动选择（账号池）</option>
                      {activeProfiles.map((item) => (
                        <option key={item.name} value={item.name}>
                          指定：{item.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="field">
                    <label htmlFor="testProject">
                      Flow 项目（指定账号时可选）
                    </label>
                    <select
                      id="testProject"
                      value={projectId}
                      onChange={(event) => setProjectId(event.target.value)}
                      disabled={selectedProfile === "auto"}
                    >
                      <option value="">不绑定项目</option>
                      {projects.map((item) => (
                        <option key={item.project_id} value={item.project_id}>
                          {item.title || item.project_id}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="field full">
                    <label htmlFor="testPrompt">提示词</label>
                    <textarea
                      id="testPrompt"
                      value={prompt}
                      onChange={(event) => setPrompt(event.target.value)}
                    />
                  </div>
                  <div className="field">
                    <label htmlFor="testModel">模型</label>
                    <select
                      id="testModel"
                      value={model}
                      onChange={(event) => setModel(event.target.value)}
                    >
                      {modelOptions.map((item) => (
                        <option key={item.id} value={item.id}>
                          {item.name}
                        </option>
                      ))}
                    </select>
                     <div className="sub">实际提交：{model} · {selectedModel?.name || "未知模型"}</div>
                  </div>
                   <div className="field">
                     <label htmlFor="testAspect">画幅 / 时长</label>
                     <AspectDurationControl
                       model={model}
                       value={aspectDuration}
                       onChange={(event) => setAspectDuration(event.target.value)}
                     />
                   </div>
                  {kind === "video" && (
                    <div className="field">
                      <label htmlFor="testMode">视频方式</label>
                      <select
                        id="testMode"
                        value={mode}
                        onChange={(event) => setMode(event.target.value)}
                      >
                        <option value="t2v">T2V · 文生视频</option>
                        <option value="r2v">R2V · 参考生视频</option>
                        <option value="i2v">I2V · 图生视频</option>
                      </select>
                    </div>
                  )}
                  <div className="field full">
                    <label>参考图（可选）</label>
                    <div
                      className="dropzone"
                      onClick={() => fileRef.current?.click()}
                      onDragOver={(event) => event.preventDefault()}
                      onDrop={(event) => {
                        event.preventDefault();
                        addFiles(event.dataTransfer.files);
                      }}
                    >
                      <div>
                        <strong>拖拽图片到这里，或点击选择</strong>
                        <span>先上传到适配层，生成请求不携带 Base64</span>
                        <input
                          ref={fileRef}
                          type="file"
                          accept="image/png,image/jpeg,image/webp"
                          multiple
                          hidden
                          onChange={(event) => addFiles(event.target.files)}
                        />
                      </div>
                    </div>
                    <div className="asset-chips">
                      {uploaded.map((item) => (
                        <span className="chip" key={item.asset_id}>
                          {item.filename}
                          <button
                            type="button"
                            onClick={() =>
                              setUploaded((current) =>
                                current.filter(
                                  (entry) => entry.asset_id !== item.asset_id,
                                ),
                              )
                            }
                          >
                            ×
                          </button>
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
                <div className="form-actions">
                  <Button
                    type="submit"
                    className="primary"
                    disabled={
                      !live ||
                      !selectedProfile ||
                      (selectedProfile !== "auto" &&
                        !currentProfile?.enabled) ||
                      submitting
                    }
                  >
                    {submitting ? "提交中…" : "提交测试 →"}
                  </Button>
                </div>
              </form>
            </div>
          </Section>
          <Section title="测试结果">
            <div className="section-body">
              {result ? (
                <div className="result-panel">
                  <div className="result-line">
                    <strong>{result.id?.slice(0, 12)}</strong>
                    <span className={`status ${statusInfo(result.status)[0]}`}>
                      {statusInfo(result.status)[1]}
                    </span>
                  </div>
                  <p className="muted">
                    {result.error ||
                      `实际账号：${result.profile || "等待账号池分配"} · ${timeLabel(result.created_at)}`}
                  </p>
                  {files.length > 0 && (
                    <div className="result-files">
                      {files.map((file) => (
                        <a
                          className="result-file"
                          key={file.preview_url || file.name}
                          href={file.preview_url}
                          target="_blank"
                          rel="noreferrer"
                        >
                          打开结果：{file.name}
                        </a>
                      ))}
                    </div>
                  )}
                  <pre className="json-view">
                    {JSON.stringify(result, null, 2)}
                  </pre>
                  <button
                    className="link"
                    onClick={() => showDetail(result.id)}
                  >
                    查看完整日志 →
                  </button>
                </div>
              ) : (
                <Empty>
                  提交一次测试后，这里会显示任务状态、结果文件和原始响应。
                </Empty>
              )}
            </div>
          </Section>
        </div>
      </>
    );
  }
  function Settings() {
    return (
      <>
        <PageHead page="settings" />
        <div className="layout-2">
          <Section title="任务队列">
            <div className="section-body">
              <form className="form-grid" onSubmit={saveConfig}>
                <div className="field">
                  <label>队列并发 Worker</label>
                  <input
                    type="number"
                    min="1"
                    max="32"
                    value={config.queue_workers}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        queue_workers: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>图片并发 Worker</label>
                  <input
                    type="number"
                    min="1"
                    max="32"
                    value={config.image_workers}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        image_workers: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>视频并发 Worker</label>
                  <input
                    type="number"
                    min="1"
                    max="32"
                    value={config.video_workers}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        video_workers: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>单任务超时（秒）</label>
                  <input
                    type="number"
                    min="60"
                    max="86400"
                    value={config.generation_timeout_seconds}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        generation_timeout_seconds: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>结果链接有效期（秒）</label>
                  <input
                    type="number"
                    min="300"
                    max="2592000"
                    value={config.media_url_ttl_seconds}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        media_url_ttl_seconds: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>上传素材保留（秒）</label>
                  <input
                    type="number"
                    min="3600"
                    max="7776000"
                    value={config.asset_retention_seconds}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        asset_retention_seconds: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>MCP 生成突发容量</label>
                  <input
                    type="number"
                    min="1"
                    max="1000"
                    value={config.generation_rate_capacity}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        generation_rate_capacity: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field">
                  <label>MCP 每个令牌间隔（秒）</label>
                  <input
                    type="number"
                    min="0.1"
                    max="86400"
                    step="0.1"
                    value={config.generation_rate_refill_seconds}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        generation_rate_refill_seconds: Number(event.target.value),
                      })
                    }
                  />
                </div>
                <div className="field full">
                  <p className="hint">
                    这是本地提交节奏保护，不会增加 Google 配额，也不会提高单个 Chrome Profile 的安全并发。
                  </p>
                </div>
                <div className="field full">
                  <label>公网基地址（用于生成 HTTPS 结果链接）</label>
                  <input
                    placeholder="https://gflow.example.com"
                    value={config.public_base_url || ""}
                    onChange={(event) =>
                      setConfig({
                        ...config,
                        public_base_url: event.target.value,
                      })
                    }
                  />
                </div>
                <div className="form-actions full">
                  <Button className="primary" type="submit">
                    保存设置
                  </Button>
                </div>
              </form>
            </div>
          </Section>
          <Section title="统一入口">
            <div className="section-body">
              <div className="setting-list">
                <div className="setting">
                  <div>
                    <strong>MCP Streamable HTTP</strong>
                    <p>外部客户端统一使用当前站点的 /mcp</p>
                  </div>
                  <code>{config.mcp_endpoint || "/mcp"}</code>
                </div>
                <div className="setting">
                  <div>
                    <strong>OpenAI 兼容接口</strong>
                    <p>模型列表、图片生成、视频任务和 chat 兼容转接</p>
                  </div>
                  <code>/v1/models</code>
                </div>
                <div className="setting">
                  <div>
                    <strong>鉴权</strong>
                    <p>
                      公网部署必须配置 API Key 或管理员密码，并在反向代理启用
                      HTTPS
                    </p>
                  </div>
                  <span
                    className={`status ${config.auth_required ? "done" : "fail"}`}
                  >
                    {config.auth_required ? "已启用" : "仅本机安全"}
                  </span>
                </div>
              </div>
            </div>
          </Section>
        </div>
      </>
    );
  }
  const page = {
    overview: <Overview />,
    accounts: <Accounts />,
    logs: <Logs />,
    test: <Test />,
    settings: <Settings />,
  }[view];
  const loginOverlay =
    authRequired && !authenticated ? (
      <div className="modal-backdrop">
        <div className="modal-panel login-panel">
          <div className="section-head">
            <div>
              <h2>登录 gflow studio</h2>
              <p className="muted">公网访问需要管理员会话。</p>
            </div>
          </div>
          <div className="section-body">
            <form className="form-grid" onSubmit={login}>
              <div className="field full">
                <label>用户名</label>
                <input
                  value={loginForm.username}
                  onChange={(event) =>
                    setLoginForm({ ...loginForm, username: event.target.value })
                  }
                />
              </div>
              <div className="field full">
                <label>密码</label>
                <input
                  type="password"
                  value={loginForm.password}
                  onChange={(event) =>
                    setLoginForm({ ...loginForm, password: event.target.value })
                  }
                />
              </div>
              <div className="form-actions full">
                <Button className="primary" type="submit">
                  登录
                </Button>
              </div>
            </form>
          </div>
        </div>
      </div>
    ) : null;
  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">gf</div>
          <div>
            <span className="brand-name">gflow studio</span>
            <span className="brand-sub">local control plane</span>
          </div>
        </div>
        <div className="nav-label">工作区</div>
        <nav className="nav">
          {NAV.map((item) => (
            <button
              key={item[0]}
              className={view === item[0] ? "active" : ""}
              onClick={() => switchView(item[0])}
            >
              <span className="nav-icon">{item[1]}</span>
              <span>{item[2]}</span>
            </button>
          ))}
        </nav>
        <div className="sidebar-spacer" />
        <div className="connection">
          <div className="connection-head">
            <span>
              <i className="dot" />
              gflow-cli
            </span>
            <span>local</span>
          </div>
          <p>
            Chrome profiles · Flow
            <br />
            账号池自动调度
          </p>
        </div>
      </aside>
      <main className="main">
        {topbar()}
        <div className="content">{page}</div>
      </main>
      {toast && <div className="toast show">{toast}</div>}
      {loginOverlay}
    </div>
  );
}

function PageHead({ page, action }) {
  return (
    <div className="page-head">
      <div>
        <div className="eyebrow">{META[page][0]}</div>
        <h1>{META[page][1]}</h1>
        <p className="lead">{META[page][2]}</p>
      </div>
      {action}
    </div>
  );
}
function Stat({ label, value, note }) {
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{String(value).padStart(2, "0")}</div>
      <div className="stat-note">{note}</div>
    </div>
  );
}
function TaskRow({ item }) {
  const state = statusInfo(item.status);
  return (
    <div className="task-row">
      <div className="type-icon">{item.kind === "image" ? "I" : "V"}</div>
      <div>
        <div className="task-title">
          {item.prompt || item.model || "Flow 请求"}
        </div>
        <div className="task-meta">
          {item.profile || "账号池待分配"} · {item.model || "—"} ·{" "}
          {timeLabel(item.created_at)}
        </div>
      </div>
      <span className={`status ${state[0]}`}>{state[1]}</span>
    </div>
  );
}
function DetailModal({ item, close }) {
  return (
    <div className="modal-backdrop" onClick={close}>
      <div className="modal-panel" onClick={(event) => event.stopPropagation()}>
        <div className="section-head">
          <h2>日志详情</h2>
          <button className="link" onClick={close}>
            关闭
          </button>
        </div>
        <div className="section-body">
          <div className="detail-grid">
            <span>任务 ID</span>
            <strong>{item.id}</strong>
            <span>账号</span>
            <strong>{item.profile || "账号池待分配"}</strong>
            <span>状态</span>
            <strong>{statusInfo(item.status)[1]}</strong>
            <span>提交时间</span>
            <strong>{timeLabel(item.created_at)}</strong>
          </div>
          {item.error && <div className="error-box">{item.error}</div>}
          <pre className="json-view">{JSON.stringify(item, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}
