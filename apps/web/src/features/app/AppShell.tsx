import { useEffect, useRef, useState, type FormEvent } from 'react'
import { Link, NavLink, Navigate, Route, Routes, useSearchParams } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Activity,
  Archive,
  Ban,
  BarChart3,
  CheckCircle2,
  Clock3,
  Cog,
  Database,
  ExternalLink,
  FileSearch,
  Gauge,
  GitBranch,
  Image as ImageIcon,
  Languages,
  Loader2,
  Monitor,
  Moon,
  Play,
  RefreshCw,
  Search,
  Server,
  Settings as SettingsIcon,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Sun,
  Terminal,
  Upload,
  XCircle,
} from 'lucide-react'
import '../../App.css'
import { UiPreferenceProvider, useUiPreferences, type AppLanguage, type AppThemePreference } from './uiPreferences'
import {
  artifactFileUrl,
  cancelRun,
  createArchiveRun,
  createChartExportRun,
  createDiagnosticRun,
  createMarketDataRun,
  createPreselectRun,
  createReviewProviderRun,
  dryRunLegacyImport,
  getSettings,
  getStrategies,
  getStrategySummary,
  getArchiveRow,
  getCandidate,
  getHealth,
  getReview,
  getRun,
  getRunArtifacts,
  getRunEvents,
  importLegacyCandidateBatch,
  importLegacyHistorySnapshot,
  importLegacyReviewRun,
  listCandidateBatches,
  listArchiveRows,
  listArchiveSnapshots,
  listCandidates,
  listRuns,
  listReviews,
  putSettings,
  verifyLegacyImport,
  type ArchiveRow,
  type ArchiveRowFilters,
  type ArchiveSnapshot,
  type ArchiveStatus,
  type Artifact,
  type Candidate,
  type CandidateBatchSummary,
  type CandidateFilters,
  type JobEvent,
  type LegacyImportIssue,
  type LegacyImportSectionReport,
  type LegacyImportSummary,
  type LegacyImportVerifyReport,
  type LegacyImportVerifyScope,
  type MarketDataRunRequest,
  type PreselectRunRequest,
  type ProductPreferenceSettings,
  type ProductSettingsResponse,
  type RecommendationStatus,
  type Review,
  type ReviewFilters,
  type RunStatus,
  type RunSummary,
  type StrategyDefinition,
  type StrategyPreferenceId,
  type StrategySummaryFilters,
  type StrategySummaryRow,
} from '../../api'

const navItems = [
  { to: '/overview', label: 'Overview', icon: Gauge, state: 'active' },
  { to: '/runs', label: 'Run Center', icon: Activity, state: 'active' },
  { to: '/candidates', label: 'Candidates', icon: Search, state: 'active' },
  { to: '/reviews', label: 'Reviews', icon: FileSearch, state: 'active' },
  { to: '/archive', label: 'Archive', icon: Archive, state: 'active' },
  { to: '/analytics', label: 'Analytics', icon: BarChart3, state: 'active' },
  { to: '/migrations', label: 'Migrations', icon: Database, state: 'active' },
  { to: '/settings', label: 'Settings', icon: SettingsIcon, state: 'active' },
]

const statusLabels: Record<RunStatus, string> = {
  queued: 'Queued',
  running: 'Running',
  succeeded: 'Succeeded',
  failed: 'Failed',
  cancelling: 'Cancelling',
  cancelled: 'Cancelled',
}

const MARKET_DATA_DEFAULT_START_DATE = '2019-01-01'

const legacyMigrationScopes: { value: LegacyImportVerifyScope; label: string; description: string }[] = [
  { value: 'candidates', label: 'Candidates', description: 'Batch files and strategy counts' },
  { value: 'reviews', label: 'Reviews', description: 'Review runs and recommendations' },
  { value: 'history', label: 'History', description: 'Archive snapshots and rows' },
]

function App() {
  return (
    <UiPreferenceProvider>
      <ProductShell />
    </UiPreferenceProvider>
  )
}

function ProductShell() {
  const { language, theme, setLanguage, setTheme, t } = useUiPreferences()
  const ThemeIcon = theme === 'dark' ? Moon : theme === 'light' ? Sun : Monitor

  return (
    <div className="app-shell">
      <LiquidGlassLayer />
      <aside className="sidebar" aria-label={t('Product navigation')}>
        <div className="brand-lockup">
          <div className="brand-mark" aria-hidden="true">
            <Gauge size={22} />
          </div>
          <div>
            <p className="brand-name">StockTradebyZ</p>
            <p className="brand-meta">{t('Product refactor')}</p>
          </div>
        </div>

        <nav className="primary-nav">
          {navItems.map((item) => (
            <NavLink key={item.to} to={item.to} className={({ isActive }) => (isActive ? 'nav-item active' : 'nav-item')}>
              <item.icon size={18} aria-hidden="true" />
              <span>{t(item.label)}</span>
              {item.state === 'pending' ? <span className="nav-pill">{t('Soon')}</span> : null}
            </NavLink>
          ))}
        </nav>

        <div className="sidebar-controls" aria-label={t('Display preferences')}>
          <label className="preference-select">
            <span>
              <Languages size={15} aria-hidden="true" />
              {t('Language')}
            </span>
            <select value={language} onChange={(event) => setLanguage(event.target.value as AppLanguage)}>
              <option value="zh">{t('Chinese')}</option>
              <option value="en">{t('English')}</option>
            </select>
          </label>
          <label className="preference-select">
            <span>
              <ThemeIcon size={15} aria-hidden="true" />
              {t('Theme mode')}
            </span>
            <select value={theme} onChange={(event) => setTheme(event.target.value as AppThemePreference)}>
              <option value="system">{t('System')}</option>
              <option value="light">{t('Light')}</option>
              <option value="dark">{t('Dark')}</option>
            </select>
          </label>
        </div>
      </aside>

      <main className="main-surface">
        <Routes>
          <Route path="/" element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<OverviewView />} />
          <Route path="/runs" element={<RunsView />} />
          <Route path="/candidates" element={<CandidatesView />} />
          <Route path="/reviews" element={<ReviewsView />} />
          <Route path="/archive" element={<ArchiveView />} />
          <Route path="/analytics" element={<AnalyticsView />} />
          <Route path="/migrations" element={<MigrationsView />} />
          <Route path="/settings" element={<SettingsView />} />
        </Routes>
      </main>
    </div>
  )
}

function LiquidGlassLayer() {
  const layerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const layer = layerRef.current
    if (!layer) return undefined
    const currentLayer: HTMLDivElement = layer

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)')
    let frame = 0

    function hideLens() {
      currentLayer.dataset.active = 'false'
    }

    function updateLens(event: PointerEvent) {
      if (reducedMotion.matches || event.pointerType === 'touch') {
        hideLens()
        return
      }
      if (frame) window.cancelAnimationFrame(frame)
      frame = window.requestAnimationFrame(() => {
        currentLayer.style.setProperty('--glass-x', `${event.clientX}px`)
        currentLayer.style.setProperty('--glass-y', `${event.clientY}px`)
        currentLayer.dataset.active = 'true'
      })
    }

    window.addEventListener('pointermove', updateLens, { passive: true })
    window.addEventListener('pointerleave', hideLens)
    window.addEventListener('blur', hideLens)

    return () => {
      if (frame) window.cancelAnimationFrame(frame)
      window.removeEventListener('pointermove', updateLens)
      window.removeEventListener('pointerleave', hideLens)
      window.removeEventListener('blur', hideLens)
    }
  }, [])

  return <div ref={layerRef} className="liquid-glass-layer" aria-hidden="true" data-active="false" />
}

function OverviewView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const healthQuery = useQuery({ queryKey: ['health'], queryFn: getHealth, refetchInterval: 15_000 })
  const settingsQuery = useQuery({ queryKey: ['settings'], queryFn: getSettings })
  const strategiesQuery = useQuery({ queryKey: ['strategies'], queryFn: getStrategies })
  const analyticsQuery = useQuery({
    queryKey: ['strategy-summary', { limit: '5' }],
    queryFn: () => getStrategySummary({ limit: '5' }),
  })
  const runsQuery = useQuery({ queryKey: ['runs'], queryFn: listRuns, refetchInterval: 5_000 })

  const recentRuns = runsQuery.data?.runs.slice(0, 5) ?? []
  const defaultStrategies = settingsQuery.data?.product_preferences.preferences.default_strategy_ids ?? []
  const configuredIntegrations = settingsQuery.data?.external_integrations.filter((integration) => integration.configured).length ?? 0
  const healthState = healthQuery.isLoading ? 'checking' : healthQuery.isError ? 'offline' : 'online'

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Research workstation')}</p>
          <h1>{t('Overview')}</h1>
        </div>
        <button
          type="button"
          className="icon-button secondary"
          aria-label={t('Refresh overview')}
          onClick={() => {
            queryClient.invalidateQueries({ queryKey: ['health'] })
            queryClient.invalidateQueries({ queryKey: ['settings'] })
            queryClient.invalidateQueries({ queryKey: ['strategies'] })
            queryClient.invalidateQueries({ queryKey: ['strategy-summary'] })
            queryClient.invalidateQueries({ queryKey: ['runs'] })
          }}
        >
          <RefreshCw size={17} aria-hidden="true" />
        </button>
      </header>

      <section className="summary-strip" aria-label={t('Workstation summary')}>
        <Metric label="API" value={healthState === 'online' ? 'Online' : healthState === 'offline' ? 'Offline' : 'Checking'} />
        <Metric label="Settings" value={settingsQuery.data?.product_preferences.source === 'sqlite' ? 'Saved' : 'Defaults'} />
        <Metric label="Strategies" value={(strategiesQuery.data?.strategies.length ?? 0).toString()} />
        <Metric label="Analytics rows" value={(analyticsQuery.data?.totals.total ?? 0).toString()} />
      </section>

      {healthQuery.isError || settingsQuery.isError || strategiesQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(healthQuery.error ?? settingsQuery.error ?? strategiesQuery.error)}</span>
        </div>
      ) : null}

      <div className="overview-grid">
        <section className="overview-panel" aria-label={t('System state')}>
          <div className="panel-heading">
            <div>
              <h2>{t('System state')}</h2>
              <p>{settingsQuery.isLoading ? t('Loading product settings') : `${configuredIntegrations} ${t('integrations configured')}`}</p>
            </div>
            <Link className="artifact-open-link" to="/settings">
              <SettingsIcon size={15} aria-hidden="true" />
              <span>{t('Settings')}</span>
            </Link>
          </div>
          {settingsQuery.isLoading ? <RunSkeleton /> : null}
          {settingsQuery.data ? (
            <div className="overview-state-list">
              <DataPair label="SQLite" value={settingsQuery.data.local_state.sqlite_path} />
              <DataPair label="DuckDB" value={settingsQuery.data.local_state.duckdb_path ?? 'Disabled'} />
              <DataPair label="Preferences" value={settingsQuery.data.product_preferences.source === 'sqlite' ? 'Stored in SQLite' : 'Using defaults'} />
              <DataPair label="Simulated trading" value={settingsQuery.data.simulated_trading_in_scope ? 'In scope' : 'Out of scope'} />
            </div>
          ) : null}
        </section>

        <section className="overview-panel" aria-label={t('Strategy readiness')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Strategy readiness')}</h2>
              <p>{strategiesQuery.isLoading ? t('Loading strategies') : `${t('Default')}: ${defaultStrategies.join(', ') || t('none')}`}</p>
            </div>
            <Link className="artifact-open-link" to="/analytics">
              <BarChart3 size={15} aria-hidden="true" />
              <span>{t('Analytics')}</span>
            </Link>
          </div>
          {strategiesQuery.isLoading ? <RunSkeleton /> : null}
          <div className="strategy-card-grid">
            {strategiesQuery.data?.strategies.map((strategy) => (
              <StrategyCard key={strategy.id} strategy={strategy} selected={defaultStrategies.includes(strategy.id)} />
            ))}
          </div>
        </section>

        <section className="overview-panel" aria-label={t('Recent runs')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Recent runs')}</h2>
              <p>{runsQuery.isLoading ? t('Loading run history') : `${recentRuns.length} ${t('latest runs')}`}</p>
            </div>
            <Link className="artifact-open-link" to="/runs">
              <Activity size={15} aria-hidden="true" />
              <span>{t('Run center')}</span>
            </Link>
          </div>
          {runsQuery.isLoading ? <RunSkeleton /> : null}
          {runsQuery.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(runsQuery.error)}</span>
            </div>
          ) : null}
          {!runsQuery.isLoading && !runsQuery.isError && recentRuns.length === 0 ? (
            <div className="empty-state compact-empty">
              <Activity size={24} aria-hidden="true" />
              <h3>{t('No runs yet')}</h3>
            </div>
          ) : null}
          <div className="overview-run-list">
            {recentRuns.map((run) => (
              <Link key={run.id} className="overview-run-row" to={`/runs?run_id=${encodeURIComponent(run.id)}`}>
                <StatusBadge status={run.status} />
                <span>
                  <strong>{run.kind}</strong>
                  <small>{run.pick_date ?? t('No pick date')} / {formatDateTime(run.created_at)}</small>
                </span>
                <code>{run.id}</code>
              </Link>
            ))}
          </div>
        </section>

        <section className="overview-panel" aria-label={t('Strategy analytics')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Strategy summary')}</h2>
              <p>{analyticsQuery.isLoading ? t('Loading DuckDB summary') : `${analyticsQuery.data?.rows.length ?? 0} ${t('rows')}`}</p>
            </div>
          </div>
          {analyticsQuery.isLoading ? <RunSkeleton /> : null}
          {analyticsQuery.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(analyticsQuery.error)}</span>
            </div>
          ) : null}
          {!analyticsQuery.isLoading && !analyticsQuery.isError && (analyticsQuery.data?.rows.length ?? 0) === 0 ? (
            <div className="empty-state compact-empty">
              <BarChart3 size={24} aria-hidden="true" />
              <h3>{t('No strategy metrics yet')}</h3>
            </div>
          ) : null}
          <StrategySummaryList rows={analyticsQuery.data?.rows ?? []} />
        </section>
      </div>
    </div>
  )
}

function AnalyticsView() {
  const { t } = useUiPreferences()
  const [searchParams, setSearchParams] = useSearchParams()
  const filters = strategySummaryFiltersFromParams(searchParams)
  const normalizedFilters = {
    pick_date: filters.pick_date.trim(),
    run_id: filters.run_id.trim(),
    strategy: filters.strategy.trim(),
    limit: filters.limit.trim() || '100',
  }
  const hasFilters = Boolean(normalizedFilters.pick_date || normalizedFilters.run_id || normalizedFilters.strategy || normalizedFilters.limit !== '100')
  const summaryQuery = useQuery({
    queryKey: ['strategy-summary', normalizedFilters],
    queryFn: () => getStrategySummary(normalizedFilters),
  })

  function updateFilter(key: keyof Required<StrategySummaryFilters>, value: string) {
    const nextFilters = { ...filters, [key]: value }
    setSearchParams(strategySummaryFiltersToParams(nextFilters), { replace: true })
  }

  function resetFilters() {
    setSearchParams(strategySummaryFiltersToParams(emptyStrategySummaryFilters()), { replace: true })
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('DuckDB analysis')}</p>
          <h1>{t('Strategy analytics')}</h1>
        </div>
      </header>

      <form className="filter-bar analytics-filter-bar" onSubmit={(event) => event.preventDefault()} aria-label={t('Strategy analytics filters')}>
        <FilterInput
          label="Pick date"
          placeholder="2026-05-27"
          type="date"
          value={filters.pick_date}
          onChange={(value) => updateFilter('pick_date', value)}
        />
        <FilterInput label="Run id" placeholder="run id" value={filters.run_id} onChange={(value) => updateFilter('run_id', value)} />
        <FilterInput label="Strategy" placeholder="b1 / b2 / brick" value={filters.strategy} onChange={(value) => updateFilter('strategy', value)} />
        <FilterInput label="Limit" placeholder="100" value={filters.limit} onChange={(value) => updateFilter('limit', value)} />
        <button type="button" className="action-button secondary filter-reset" onClick={resetFilters} disabled={!hasFilters}>
          <XCircle size={17} aria-hidden="true" />
          <span>{t('Clear')}</span>
        </button>
      </form>

      {summaryQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(summaryQuery.error)}</span>
        </div>
      ) : null}

      <section className="summary-strip" aria-label={t('Analytics totals')}>
        <Metric label="Total" value={(summaryQuery.data?.totals.total ?? 0).toString()} />
        <Metric label="Reviewed" value={(summaryQuery.data?.totals.reviewed ?? 0).toString()} />
        <Metric label="Recommended" value={(summaryQuery.data?.totals.recommended ?? 0).toString()} />
        <Metric label="Recommended rate" value={formatPercent(summaryQuery.data?.totals.recommended_rate ?? 0)} />
      </section>

      <section className="analytics-panel" aria-label={t('Strategy summary table')}>
        <div className="panel-heading">
          <div>
            <h2>{t('Strategy/date/run comparison')}</h2>
            <p>{summaryQuery.isLoading ? t('Loading summary rows') : `${summaryQuery.data?.rows.length ?? 0} ${t('rows')}`}</p>
          </div>
        </div>
        {summaryQuery.isLoading ? <RunSkeleton /> : null}
        {!summaryQuery.isLoading && (summaryQuery.data?.rows.length ?? 0) === 0 ? (
          <div className="empty-state">
            <BarChart3 size={24} aria-hidden="true" />
            <h3>{hasFilters ? t('No strategy metrics match the filters') : t('No strategy metrics yet')}</h3>
          </div>
        ) : null}
        {(summaryQuery.data?.rows.length ?? 0) > 0 ? (
          <div className="analytics-table-wrap">
            <table className="data-table strategy-summary-table">
              <thead>
                <tr>
                  <th>{t('Pick date')}</th>
                  <th>{t('Strategy')}</th>
                  <th>{t('Total')}</th>
                  <th>{t('Reviewed')}</th>
                  <th>{t('Recommended')}</th>
                  <th>{t('Unreviewed')}</th>
                  <th>{t('Rate')}</th>
                  <th>{t('Run')}</th>
                </tr>
              </thead>
              <tbody>
                {summaryQuery.data?.rows.map((row) => (
                  <tr key={`${row.pick_date}-${row.run_id}-${row.strategy}`}>
                    <td>{row.pick_date}</td>
                    <td><span className="strategy-chip">{row.strategy}</span></td>
                    <td>{row.total}</td>
                    <td>{row.reviewed}</td>
                    <td>{row.recommended}</td>
                    <td>{row.unreviewed}</td>
                    <td>{formatPercent(row.recommended_rate)}</td>
                    <td><code>{row.run_id}</code></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  )
}

function SettingsView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const settingsQuery = useQuery({ queryKey: ['settings'], queryFn: getSettings })
  const strategiesQuery = useQuery({ queryKey: ['strategies'], queryFn: getStrategies })
  const [formDraft, setFormDraft] = useState<ProductPreferenceSettings | null>(null)
  const serverPreferences = settingsQuery.data?.product_preferences.preferences ?? null
  const form = formDraft ?? serverPreferences
  const settingsMutation = useMutation({
    mutationFn: (preferences: ProductPreferenceSettings) => putSettings({ preferences }),
    onSuccess: (data) => {
      queryClient.setQueryData(['settings'], data)
      setFormDraft(null)
    },
  })

  function updateForm<K extends keyof ProductPreferenceSettings>(key: K, value: ProductPreferenceSettings[K]) {
    setFormDraft((current) => {
      const base = current ?? serverPreferences
      return base ? { ...base, [key]: value } : current
    })
  }

  function toggleStrategy(strategyId: StrategyPreferenceId) {
    setFormDraft((current) => {
      const base = current ?? serverPreferences
      if (!base) return current
      const selected = base.default_strategy_ids.includes(strategyId)
      const next = selected
        ? base.default_strategy_ids.filter((id) => id !== strategyId)
        : [...base.default_strategy_ids, strategyId]
      return { ...base, default_strategy_ids: next }
    })
  }

  function submitSettings(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!form) return
    settingsMutation.mutate(form)
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Product configuration')}</p>
          <h1>{t('Settings')}</h1>
        </div>
        <button
          type="button"
          className="icon-button secondary"
          aria-label={t('Refresh settings')}
          onClick={() => {
            queryClient.invalidateQueries({ queryKey: ['settings'] })
            queryClient.invalidateQueries({ queryKey: ['strategies'] })
          }}
        >
          <RefreshCw size={17} aria-hidden="true" />
        </button>
      </header>

      {settingsQuery.isError || strategiesQuery.isError || settingsMutation.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(settingsQuery.error ?? strategiesQuery.error ?? settingsMutation.error)}</span>
        </div>
      ) : null}
      {settingsMutation.isSuccess ? (
        <div className="alert success-alert" role="status">
          <CheckCircle2 size={18} aria-hidden="true" />
          <span>{t('Product preferences saved to SQLite.')}</span>
        </div>
      ) : null}

      <div className="settings-grid">
        <section className="settings-panel" aria-label={t('Product preferences')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Product preferences')}</h2>
              <p>
                {settingsQuery.isLoading
                  ? t('Loading preferences')
                  : `${settingsQuery.data?.product_preferences.source === 'sqlite' ? t('SQLite saved') : t('Default values')} / ${formatDateTime(settingsQuery.data?.product_preferences.updated_at ?? null)}`}
              </p>
            </div>
          </div>
          {settingsQuery.isLoading || !form ? <RunSkeleton /> : null}
          {form ? (
            <form className="settings-form" onSubmit={submitSettings}>
              <div className="settings-form-grid">
                <label className="filter-field">
                  <span>{t('Timezone')}</span>
                  <input value={form.timezone} onChange={(event) => updateForm('timezone', event.target.value)} />
                </label>
                <label className="filter-field">
                  <span>{t('Theme')}</span>
                  <select value={form.theme} onChange={(event) => updateForm('theme', event.target.value as ProductPreferenceSettings['theme'])}>
                    <option value="system">{t('System')}</option>
                    <option value="light">{t('Light')}</option>
                    <option value="dark">{t('Dark')}</option>
                  </select>
                </label>
                <label className="filter-field">
                  <span>{t('Table density')}</span>
                  <select
                    value={form.table_density}
                    onChange={(event) => updateForm('table_density', event.target.value as ProductPreferenceSettings['table_density'])}
                  >
                    <option value="comfortable">{t('Comfortable')}</option>
                    <option value="compact">{t('Compact')}</option>
                  </select>
                </label>
                <NumberPreference
                  label="Analytics limit"
                  value={form.analytics_default_limit}
                  min={1}
                  max={500}
                  onChange={(value) => updateForm('analytics_default_limit', value)}
                />
                <NumberPreference
                  label="Candidate page size"
                  value={form.candidate_page_size}
                  min={10}
                  max={500}
                  onChange={(value) => updateForm('candidate_page_size', value)}
                />
                <NumberPreference
                  label="Review page size"
                  value={form.review_page_size}
                  min={10}
                  max={500}
                  onChange={(value) => updateForm('review_page_size', value)}
                />
                <NumberPreference
                  label="Archive page size"
                  value={form.archive_page_size}
                  min={10}
                  max={500}
                  onChange={(value) => updateForm('archive_page_size', value)}
                />
              </div>

              <fieldset className="strategy-selector">
                <legend>{t('Default strategies')}</legend>
                <div className="strategy-toggle-grid">
                  {(strategiesQuery.data?.strategies ?? []).map((strategy) => (
                    <label key={strategy.id} className="strategy-toggle">
                      <input
                        type="checkbox"
                        checked={form.default_strategy_ids.includes(strategy.id)}
                        onChange={() => toggleStrategy(strategy.id)}
                      />
                      <span>
                        <strong>{strategy.label}</strong>
                        <small>{strategy.description}</small>
                      </span>
                    </label>
                  ))}
                </div>
              </fieldset>

              <div className="settings-toggle-row">
                <label className="binary-toggle">
                  <input
                    type="checkbox"
                    checked={form.chart_export_enabled}
                    onChange={(event) => updateForm('chart_export_enabled', event.target.checked)}
                  />
                  <span>{t('Chart export enabled')}</span>
                </label>
                <label className="binary-toggle">
                  <input
                    type="checkbox"
                    checked={form.auto_archive_after_review}
                    onChange={(event) => updateForm('auto_archive_after_review', event.target.checked)}
                  />
                  <span>{t('Auto archive after review')}</span>
                </label>
              </div>

              <div className="button-row settings-actions">
                <button type="submit" className="action-button" disabled={settingsMutation.isPending || form.default_strategy_ids.length === 0}>
                  {settingsMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <SlidersHorizontal size={17} aria-hidden="true" />}
                  <span>{t('Save preferences')}</span>
                </button>
              </div>
            </form>
          ) : null}
        </section>

        <section className="settings-panel" aria-label={t('Local state and integrations')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Local state')}</h2>
              <p>{t('SQLite owns product state; DuckDB owns analytics.')}</p>
            </div>
          </div>
          {settingsQuery.isLoading ? <RunSkeleton /> : null}
          {settingsQuery.data ? <SettingsInventory settings={settingsQuery.data} /> : null}
        </section>
      </div>
    </div>
  )
}

function CandidatesView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const filters = candidateFiltersFromParams(searchParams)
  const [selectedCandidateId, setSelectedCandidateId] = useState<number | null>(null)

  const normalizedFilters = {
    batch_id: filters.batch_id.trim(),
    pick_date: filters.pick_date.trim(),
    run_id: filters.run_id.trim(),
    strategy: filters.strategy.trim(),
    code: filters.code.trim(),
  }

  const candidatesQuery = useQuery({
    queryKey: ['candidates', normalizedFilters],
    queryFn: () => listCandidates(normalizedFilters),
  })
  const candidateBatchesQuery = useQuery({
    queryKey: ['candidate-batches', normalizedFilters.pick_date, normalizedFilters.run_id],
    queryFn: () =>
      listCandidateBatches({
        pick_date: normalizedFilters.pick_date,
        run_id: normalizedFilters.run_id,
      }),
  })
  const candidates = candidatesQuery.data?.candidates ?? []
  const candidateBatches = candidateBatchesQuery.data?.batches ?? []
  const activeBatchId = normalizedFilters.batch_id
  const activeBatch = candidateBatches.find((batch) => batch.id === activeBatchId) ?? null
  const selectedStillVisible = selectedCandidateId !== null && candidates.some((candidate) => candidate.id === selectedCandidateId)
  const activeCandidateId = selectedStillVisible ? selectedCandidateId : candidates[0]?.id
  const activeCandidate = candidates.find((candidate) => candidate.id === activeCandidateId) ?? null

  const detailQuery = useQuery({
    queryKey: ['candidate', activeCandidateId],
    queryFn: () => getCandidate(activeCandidateId as number),
    enabled: typeof activeCandidateId === 'number',
  })
  const archiveBatchMutation = useMutation({
    mutationFn: (batch: CandidateBatchSummary) => {
      if (!batch.latest_review_run_id) {
        throw new Error(t('Selected batch has no review run to archive'))
      }
      return createArchiveRun({
        candidate_batch_id: batch.id,
        review_run_id: batch.latest_review_run_id,
      })
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['candidate-batches'] })
      queryClient.invalidateQueries({ queryKey: ['archive-snapshots'] })
      queryClient.invalidateQueries({ queryKey: ['archive-rows'] })
    },
  })
  const chartExportMutation = useMutation({
    mutationFn: (batch: CandidateBatchSummary) =>
      createChartExportRun({
        candidate_batch_id: batch.id,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['candidate-batches'] })
    },
  })
  const reviewBatchMutation = useMutation({
    mutationFn: (batch: CandidateBatchSummary) =>
      createReviewProviderRun({
        candidate_batch_id: batch.id,
        provider: 'gemini-cli',
        require_charts: true,
        provider_config: {
          skip_existing: true,
        },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['candidate-batches'] })
      queryClient.invalidateQueries({ queryKey: ['reviews'] })
      queryClient.invalidateQueries({ queryKey: ['review'] })
    },
  })

  const hasFilters = Object.values(filters).some((value) => value.trim())

  function updateFilter(key: keyof CandidateFilters, value: string) {
    const nextFilters = { ...filters, [key]: value }
    archiveBatchMutation.reset()
    chartExportMutation.reset()
    reviewBatchMutation.reset()
    setSearchParams(candidateFiltersToParams(nextFilters), { replace: true })
    setSelectedCandidateId(null)
  }

  function resetFilters() {
    const nextFilters = emptyCandidateFilters()
    archiveBatchMutation.reset()
    chartExportMutation.reset()
    reviewBatchMutation.reset()
    setSearchParams(candidateFiltersToParams(nextFilters), { replace: true })
    setSelectedCandidateId(null)
  }

  function selectBatch(batch: CandidateBatchSummary) {
    archiveBatchMutation.reset()
    chartExportMutation.reset()
    reviewBatchMutation.reset()
    const nextFilters = {
      ...filters,
      batch_id: batch.id,
      pick_date: '',
      run_id: '',
    }
    setSearchParams(candidateFiltersToParams(nextFilters), { replace: true })
    setSelectedCandidateId(null)
  }

  function runGeminiReview(batch: CandidateBatchSummary) {
    const confirmed = window.confirm(
      languageConfirmText(t, batch.pick_date, batch.candidate_count)
    )
    if (!confirmed) return
    reviewBatchMutation.mutate(batch)
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Selection evidence')}</p>
          <h1>{t('Candidates')}</h1>
        </div>
        <button
          type="button"
          className="icon-button secondary"
          aria-label={t('Refresh candidates')}
          onClick={() => {
            queryClient.invalidateQueries({ queryKey: ['candidates'] })
            queryClient.invalidateQueries({ queryKey: ['candidate-batches'] })
            queryClient.invalidateQueries({ queryKey: ['candidate'] })
          }}
        >
          <RefreshCw size={17} aria-hidden="true" />
        </button>
      </header>

      <section className="candidate-batch-panel" aria-label={t('Historical candidate batches')}>
        <div className="panel-heading">
          <div>
            <h2>{t('Candidate batches')}</h2>
            <p>
              {candidateBatchesQuery.isLoading
                ? t('Loading historical batches')
                : `${candidateBatchesQuery.data?.total ?? 0} ${t('batches')}${activeBatch ? `, ${t('selected')} ${activeBatch.pick_date}` : ''}`}
            </p>
          </div>
          <div className="button-row">
            {activeBatch ? (
              <>
                <button
                  type="button"
                  className="action-button"
                  onClick={() => chartExportMutation.mutate(activeBatch)}
                  disabled={activeBatch.candidate_count === 0 || chartExportMutation.isPending}
                >
                  {chartExportMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <ImageIcon size={17} aria-hidden="true" />}
                  <span>{t('Export charts')}</span>
                </button>
                <button
                  type="button"
                  className="action-button secondary"
                  onClick={() => runGeminiReview(activeBatch)}
                  disabled={activeBatch.candidate_count === 0 || reviewBatchMutation.isPending}
                >
                  {reviewBatchMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Play size={17} aria-hidden="true" />}
                  <span>{t('Gemini review')}</span>
                </button>
                <button
                  type="button"
                  className="action-button secondary"
                  onClick={() => archiveBatchMutation.mutate(activeBatch)}
                  disabled={!activeBatch.latest_review_run_id || archiveBatchMutation.isPending}
                >
                  {archiveBatchMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Archive size={17} aria-hidden="true" />}
                  <span>{t('Archive selected')}</span>
                </button>
              </>
            ) : null}
            {activeBatchId ? (
              <button type="button" className="action-button secondary" onClick={() => updateFilter('batch_id', '')}>
                <XCircle size={17} aria-hidden="true" />
                <span>{t('Clear batch')}</span>
              </button>
            ) : null}
          </div>
        </div>

        {candidateBatchesQuery.isError ? (
          <div className="alert compact-alert" role="alert">
            <ShieldAlert size={18} aria-hidden="true" />
            <span>{errorText(candidateBatchesQuery.error)}</span>
          </div>
        ) : null}
        {archiveBatchMutation.isError ? (
          <div className="alert compact-alert" role="alert">
            <ShieldAlert size={18} aria-hidden="true" />
            <span>{errorText(archiveBatchMutation.error)}</span>
          </div>
        ) : null}
        {chartExportMutation.isError ? (
          <div className="alert compact-alert" role="alert">
            <ShieldAlert size={18} aria-hidden="true" />
            <span>{errorText(chartExportMutation.error)}</span>
          </div>
        ) : null}
        {reviewBatchMutation.isError ? (
          <div className="alert compact-alert" role="alert">
            <ShieldAlert size={18} aria-hidden="true" />
            <span>{errorText(reviewBatchMutation.error)}</span>
          </div>
        ) : null}
        {archiveBatchMutation.isSuccess ? (
          <div className="alert success-alert compact-alert" role="status">
            <CheckCircle2 size={18} aria-hidden="true" />
            <span>
              {t('Archive run')} {archiveBatchMutation.data.run.id} {t('created from')} {archiveBatchMutation.data.snapshot.review_run_id}.
            </span>
          </div>
        ) : null}
        {chartExportMutation.isSuccess ? (
          <div className="alert success-alert compact-alert chart-export-alert" role="status">
            <CheckCircle2 size={18} aria-hidden="true" />
            <span>
              {t('Chart run')} {chartExportMutation.data.run.id} {t('exported')} {chartExportMutation.data.artifacts.length} {t('artifacts')}.
            </span>
            {chartExportMutation.data.artifacts[0] ? (
              <a
                className="artifact-open-link"
                href={artifactFileUrl(chartExportMutation.data.artifacts[0].id)}
                target="_blank"
                rel="noreferrer"
                aria-label={t('Open first chart artifact')}
              >
                <ExternalLink size={15} aria-hidden="true" />
                <span>{t('Open first chart')}</span>
              </a>
            ) : null}
          </div>
        ) : null}
        {reviewBatchMutation.isSuccess ? (
          <div className="alert success-alert compact-alert chart-export-alert" role="status">
            <CheckCircle2 size={18} aria-hidden="true" />
            <span>
              {t('Review run')} {reviewBatchMutation.data.review_run.id} {t('recorded')} {reviewBatchMutation.data.reviews.length} {t('reviews')}.
            </span>
            <Link
              className="artifact-open-link"
              to={`/reviews?candidate_batch_id=${encodeURIComponent(reviewBatchMutation.data.review_run.candidate_batch_id ?? activeBatchId)}`}
            >
              <FileSearch size={15} aria-hidden="true" />
              <span>{t('Open reviews')}</span>
            </Link>
          </div>
        ) : null}

        {candidateBatchesQuery.isLoading ? <RunSkeleton /> : null}
        {!candidateBatchesQuery.isLoading && candidateBatches.length === 0 ? (
          <div className="empty-state batch-empty">
            <Database size={24} aria-hidden="true" />
            <h3>{normalizedFilters.pick_date || normalizedFilters.run_id ? t('No batches match the filters') : t('No candidate batches yet')}</h3>
          </div>
        ) : null}

        <div className="candidate-batch-grid">
          {candidateBatches.map((batch) => (
            <article
              key={batch.id}
              className={batch.id === activeBatchId ? 'candidate-batch-row selected' : 'candidate-batch-row'}
            >
              <button
                type="button"
                className="candidate-batch-main"
                onClick={() => selectBatch(batch)}
                aria-label={`Use candidate batch ${batch.id} from ${batch.pick_date}`}
              >
                <span className="batch-row-head">
                  <strong>{batch.pick_date}</strong>
                  <span className="strategy-chip">{batch.source}</span>
                </span>
                <span className="batch-id-line">{batch.id}</span>
                <span className="batch-metrics">
                  <span>{batch.candidate_count} {t('candidates')}</span>
                  <span>{batch.latest_reviewed_count} {t('reviewed')}</span>
                  <span>{batch.latest_recommended_count} {t('rec')}</span>
                  <span>{batch.archive_snapshot_count} {t('archives')}</span>
                </span>
                <BatchEvidenceFlow batch={batch} />
                <span className="batch-lineage">
                  <code>{batch.run_id}</code>
                  <code>{batch.latest_review_run_id ?? t('no review run')}</code>
                </span>
              </button>
              <span className="batch-actions">
                <Link className="artifact-open-link" to={`/reviews?candidate_batch_id=${encodeURIComponent(batch.id)}`}>
                  <FileSearch size={15} aria-hidden="true" />
                  <span>{t('Reviews')}</span>
                </Link>
                <Link className="artifact-open-link" to={`/archive?pick_date=${encodeURIComponent(batch.pick_date)}`}>
                  <Archive size={15} aria-hidden="true" />
                  <span>{t('Archive')}</span>
                </Link>
              </span>
            </article>
          ))}
        </div>
      </section>

      <form className="filter-bar candidate-filter-bar" onSubmit={(event) => event.preventDefault()} aria-label={t('Candidate filters')}>
        <FilterInput
          label="Batch"
          placeholder="candidate batch"
          value={filters.batch_id}
          onChange={(value) => updateFilter('batch_id', value)}
        />
        <FilterInput
          label="Pick date"
          placeholder="2026-05-27"
          type="date"
          value={filters.pick_date}
          onChange={(value) => updateFilter('pick_date', value)}
        />
        <FilterInput label="Run id" placeholder="run id" value={filters.run_id} onChange={(value) => updateFilter('run_id', value)} />
        <FilterInput
          label="Strategy"
          placeholder="b2 / brick"
          value={filters.strategy}
          onChange={(value) => updateFilter('strategy', value)}
        />
        <FilterInput label="Code" placeholder="000001" value={filters.code} onChange={(value) => updateFilter('code', value)} />
        <button type="button" className="action-button secondary filter-reset" onClick={resetFilters} disabled={!hasFilters}>
          <XCircle size={17} aria-hidden="true" />
          <span>{t('Clear')}</span>
        </button>
      </form>

      {candidatesQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(candidatesQuery.error)}</span>
        </div>
      ) : null}

      <div className="candidate-grid">
        <section className="candidate-list-panel" aria-label={t('Candidate list')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Candidate rows')}</h2>
              <p>{candidatesQuery.isLoading ? t('Loading candidate rows') : `${candidatesQuery.data?.total ?? 0} ${t('records')}`}</p>
            </div>
            <ListSummaryChips
              loaded={candidates.length}
              total={candidatesQuery.data?.total ?? 0}
              selected={activeCandidate ? `${activeCandidate.code} / ${activeCandidate.strategy}` : null}
              filtered={hasFilters}
            />
          </div>

          {candidatesQuery.isLoading ? <RunSkeleton /> : null}
          {!candidatesQuery.isLoading && candidates.length === 0 ? (
            <div className="empty-state">
              <Search size={24} aria-hidden="true" />
              <h3>{hasFilters ? t('No candidates match the filters') : t('No candidate rows yet')}</h3>
            </div>
          ) : null}

          {candidates.length > 0 ? (
            <DenseListHeader className="candidate-result-header" columns={['Code / strategy', 'Pick date', 'Close', 'Turnover', 'Brick growth']} />
          ) : null}
          <div className="candidate-list">
            {candidates.map((candidate) => (
              <button
                key={candidate.id}
                type="button"
                className={candidate.id === activeCandidateId ? 'candidate-row selected' : 'candidate-row'}
                onClick={() => setSelectedCandidateId(candidate.id)}
                aria-label={`${candidate.code} ${candidate.strategy} candidate from run ${candidate.run_id}`}
                aria-pressed={candidate.id === activeCandidateId}
              >
                <span className="candidate-row-head">
                  <span className="candidate-code">{candidate.code}</span>
                  <span className="strategy-chip">{candidate.strategy}</span>
                </span>
                <CandidateCell label="Pick date" value={candidate.pick_date} />
                <CandidateCell label="Close" value={formatNumber(candidate.close)} strong />
                <CandidateCell label="Turnover" value={formatNumber(candidate.turnover_n)} />
                <CandidateCell label="Brick growth" value={formatNumber(candidate.brick_growth)} />
                <RowLineageStrip
                  items={[
                    { label: 'Run', value: candidate.run_id, mono: true },
                    { label: 'Batch', value: candidate.batch_id, mono: true },
                    { label: 'Extra', value: jsonInline(candidate.extra) },
                  ]}
                />
              </button>
            ))}
          </div>
        </section>

        <section className="detail-panel" aria-label={t('Candidate detail')}>
          {detailQuery.isError ? (
            <div className="alert detail-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(detailQuery.error)}</span>
            </div>
          ) : null}
          {detailQuery.isLoading && activeCandidateId ? <RunDetailSkeleton /> : null}
          {!activeCandidateId && !candidatesQuery.isLoading ? (
            <div className="empty-state detail-empty">
              <Database size={24} aria-hidden="true" />
              <h3>{t('Select a candidate')}</h3>
            </div>
          ) : null}
          {detailQuery.data ? <CandidateDetailPanel candidate={detailQuery.data.candidate} /> : null}
        </section>
      </div>
    </div>
  )
}

function ReviewsView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const filters = reviewFiltersFromParams(searchParams)
  const [selectedReviewId, setSelectedReviewId] = useState<number | null>(null)

  const normalizedFilters = {
    pick_date: filters.pick_date.trim(),
    run_id: filters.run_id.trim(),
    review_run_id: filters.review_run_id.trim(),
    candidate_batch_id: filters.candidate_batch_id.trim(),
    strategy: filters.strategy.trim(),
    code: filters.code.trim(),
    review_key: filters.review_key.trim(),
    reviewer: filters.reviewer.trim(),
    recommendation_status: filters.recommendation_status,
  }

  const reviewsQuery = useQuery({
    queryKey: ['reviews', normalizedFilters],
    queryFn: () => listReviews(normalizedFilters),
  })
  const reviews = reviewsQuery.data?.reviews ?? []
  const selectedStillVisible = selectedReviewId !== null && reviews.some((review) => review.id === selectedReviewId)
  const activeReviewId = selectedStillVisible ? selectedReviewId : reviews[0]?.id
  const activeReview = reviews.find((review) => review.id === activeReviewId) ?? null

  const detailQuery = useQuery({
    queryKey: ['review', activeReviewId],
    queryFn: () => getReview(activeReviewId as number),
    enabled: typeof activeReviewId === 'number',
  })

  const hasFilters = Object.entries(filters).some(([key, value]) => {
    if (key === 'recommendation_status') return value !== 'all'
    return value.trim()
  })

  function updateFilter(key: Exclude<keyof ReviewFilters, 'recommendation_status'>, value: string) {
    const nextFilters = { ...filters, [key]: value }
    setSearchParams(reviewFiltersToParams(nextFilters), { replace: true })
    setSelectedReviewId(null)
  }

  function updateRecommendationStatus(value: RecommendationStatus) {
    const nextFilters = { ...filters, recommendation_status: value }
    setSearchParams(reviewFiltersToParams(nextFilters), { replace: true })
    setSelectedReviewId(null)
  }

  function resetFilters() {
    const nextFilters = emptyReviewFilters()
    setSearchParams(reviewFiltersToParams(nextFilters), { replace: true })
    setSelectedReviewId(null)
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Review evidence')}</p>
          <h1>{t('Reviews')}</h1>
        </div>
        <button
          type="button"
          className="icon-button secondary"
          aria-label={t('Refresh reviews')}
          onClick={() => {
            queryClient.invalidateQueries({ queryKey: ['reviews'] })
            queryClient.invalidateQueries({ queryKey: ['review'] })
          }}
        >
          <RefreshCw size={17} aria-hidden="true" />
        </button>
      </header>

      <form className="filter-bar review-filter-bar" onSubmit={(event) => event.preventDefault()} aria-label={t('Review filters')}>
        <FilterInput
          label="Pick date"
          placeholder="2026-05-27"
          type="date"
          value={filters.pick_date}
          onChange={(value) => updateFilter('pick_date', value)}
        />
        <FilterInput label="Run id" placeholder="run id" value={filters.run_id} onChange={(value) => updateFilter('run_id', value)} />
        <FilterInput
          label="Review run"
          placeholder="review batch"
          value={filters.review_run_id}
          onChange={(value) => updateFilter('review_run_id', value)}
        />
        <FilterInput
          label="Batch"
          placeholder="candidate batch"
          value={filters.candidate_batch_id}
          onChange={(value) => updateFilter('candidate_batch_id', value)}
        />
        <FilterInput
          label="Strategy"
          placeholder="b2 / brick"
          value={filters.strategy}
          onChange={(value) => updateFilter('strategy', value)}
        />
        <FilterInput label="Code" placeholder="000001" value={filters.code} onChange={(value) => updateFilter('code', value)} />
        <FilterInput
          label="Review key"
          placeholder="000001_b2"
          value={filters.review_key}
          onChange={(value) => updateFilter('review_key', value)}
        />
        <FilterInput
          label="Reviewer"
          placeholder="gemini-cli"
          value={filters.reviewer}
          onChange={(value) => updateFilter('reviewer', value)}
        />
        <FilterSelect label="Status" value={filters.recommendation_status} onChange={updateRecommendationStatus} />
        <button type="button" className="action-button secondary filter-reset" onClick={resetFilters} disabled={!hasFilters}>
          <XCircle size={17} aria-hidden="true" />
          <span>{t('Clear')}</span>
        </button>
      </form>

      {reviewsQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(reviewsQuery.error)}</span>
        </div>
      ) : null}

      <div className="review-grid">
        <section className="review-list-panel" aria-label={t('Review list')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Review rows')}</h2>
              <p>{reviewsQuery.isLoading ? t('Loading review rows') : `${reviewsQuery.data?.total ?? 0} ${t('records')}`}</p>
            </div>
            <ListSummaryChips
              loaded={reviews.length}
              total={reviewsQuery.data?.total ?? 0}
              selected={activeReview ? `${activeReview.code} / ${activeReview.strategy}` : null}
              filtered={hasFilters}
            />
          </div>

          {reviewsQuery.isLoading ? <RunSkeleton /> : null}
          {!reviewsQuery.isLoading && reviews.length === 0 ? (
            <div className="empty-state">
              <FileSearch size={24} aria-hidden="true" />
              <h3>{hasFilters ? t('No reviews match the filters') : t('No review rows yet')}</h3>
            </div>
          ) : null}

          {reviews.length > 0 ? (
            <DenseListHeader className="review-result-header" columns={['Code / strategy', 'Verdict', 'Score', 'Rank', 'Pick date']} />
          ) : null}
          <div className="review-list">
            {reviews.map((review) => (
              <button
                key={review.id}
                type="button"
                className={review.id === activeReviewId ? 'review-row selected' : 'review-row'}
                onClick={() => setSelectedReviewId(review.id)}
                aria-label={`${review.code} ${review.strategy} review ${review.review_key}`}
                aria-pressed={review.id === activeReviewId}
              >
                <span className="review-row-head">
                  <span className="candidate-code">{review.code}</span>
                  <span className="strategy-chip">{review.strategy}</span>
                </span>
                <span className={`verdict-chip ${verdictClass(review.verdict)}`}>{review.verdict ?? t('No verdict')}</span>
                <CandidateCell label="Score" value={formatNumber(review.total_score)} strong />
                <CandidateCell label="Rank" value={formatRank(review)} />
                <CandidateCell label="Pick date" value={review.pick_date} />
                <RowLineageStrip
                  items={[
                    { label: 'Review key', value: review.review_key, mono: true },
                    { label: 'Run', value: review.run_id, mono: true },
                    { label: 'Reviewer', value: reviewerName(review) },
                  ]}
                />
              </button>
            ))}
          </div>
        </section>

        <section className="detail-panel" aria-label={t('Review detail')}>
          {detailQuery.isError ? (
            <div className="alert detail-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(detailQuery.error)}</span>
            </div>
          ) : null}
          {detailQuery.isLoading && activeReviewId ? <RunDetailSkeleton /> : null}
          {!activeReviewId && !reviewsQuery.isLoading ? (
            <div className="empty-state detail-empty">
              <FileSearch size={24} aria-hidden="true" />
              <h3>{t('Select a review')}</h3>
            </div>
          ) : null}
          {detailQuery.data ? <ReviewDetailPanel review={detailQuery.data.review} /> : null}
        </section>
      </div>
    </div>
  )
}

function ArchiveView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const filters = archiveFiltersFromParams(searchParams)
  const [selectedRowId, setSelectedRowId] = useState<number | null>(null)

  const snapshotsQuery = useQuery({
    queryKey: ['archive-snapshots'],
    queryFn: () => listArchiveSnapshots(),
  })
  const snapshots = snapshotsQuery.data?.snapshots ?? []
  const filteredPickDate = filters.pick_date.trim()
  const activePickDate = filteredPickDate || (snapshots[0]?.pick_date ?? '')
  const activeSnapshot = findActiveSnapshot(snapshots, activePickDate, filters.run_id.trim())

  const normalizedFilters = {
    pick_date: filteredPickDate,
    run_id: filters.run_id.trim(),
    strategy: filters.strategy.trim(),
    code: filters.code.trim(),
    review_key: filters.review_key.trim(),
    status: filters.status,
    rank: filters.rank.trim(),
  }

  const rowsQuery = useQuery({
    queryKey: ['archive-rows', activePickDate, normalizedFilters],
    queryFn: () => listArchiveRows(activePickDate, normalizedFilters),
    enabled: Boolean(activePickDate),
  })
  const rows = rowsQuery.data?.rows ?? []
  const selectedStillVisible = selectedRowId !== null && rows.some((row) => row.id === selectedRowId)
  const activeRowId = selectedStillVisible ? selectedRowId : rows[0]?.id
  const activeArchiveRow = rows.find((row) => row.id === activeRowId) ?? null

  const detailQuery = useQuery({
    queryKey: ['archive-row', activeRowId],
    queryFn: () => getArchiveRow(activeRowId as number),
    enabled: typeof activeRowId === 'number',
  })

  const hasFilters = Object.entries(filters).some(([key, value]) => {
    if (key === 'status') return value !== 'all'
    return value.trim()
  })

  function updateFilter(key: Exclude<keyof ArchiveRowFilters, 'status'>, value: string) {
    const nextFilters = { ...filters, [key]: value }
    setSearchParams(archiveFiltersToParams(nextFilters), { replace: true })
    setSelectedRowId(null)
  }

  function updateArchiveStatus(value: ArchiveStatus) {
    const nextFilters = { ...filters, status: value }
    setSearchParams(archiveFiltersToParams(nextFilters), { replace: true })
    setSelectedRowId(null)
  }

  function selectSnapshot(snapshot: ArchiveSnapshot) {
    setSelectedRowId(null)
    const nextFilters = {
      ...filters,
      pick_date: snapshot.pick_date,
      run_id: snapshot.run_id,
    }
    setSearchParams(archiveFiltersToParams(nextFilters), { replace: true })
  }

  function resetFilters() {
    const nextFilters = emptyArchiveFilters()
    setSearchParams(archiveFiltersToParams(nextFilters), { replace: true })
    setSelectedRowId(null)
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('History evidence')}</p>
          <h1>{t('Archive')}</h1>
        </div>
        <button
          type="button"
          className="icon-button secondary"
          aria-label={t('Refresh archive')}
          onClick={() => {
            queryClient.invalidateQueries({ queryKey: ['archive-snapshots'] })
            queryClient.invalidateQueries({ queryKey: ['archive-rows'] })
            queryClient.invalidateQueries({ queryKey: ['archive-row'] })
          }}
        >
          <RefreshCw size={17} aria-hidden="true" />
        </button>
      </header>

      <section className="summary-strip archive-summary-strip" aria-label={t('Archive summary')}>
        <Metric label="Archive dates" value={(snapshotsQuery.data?.total ?? 0).toString()} />
        <Metric label="Candidates" value={formatNumber(activeSnapshot?.candidate_count ?? null)} />
        <Metric label="Recommended" value={formatNumber(activeSnapshot?.recommended_count ?? null)} />
        <Metric label="Reviewed" value={formatNumber(activeSnapshot?.reviewed_count ?? null)} />
      </section>

      <form className="filter-bar archive-filter-bar" onSubmit={(event) => event.preventDefault()} aria-label={t('Archive filters')}>
        <FilterInput
          label="Pick date"
          placeholder="2026-05-27"
          type="date"
          value={filters.pick_date}
          onChange={(value) => updateFilter('pick_date', value)}
        />
        <FilterInput label="Run id" placeholder="archive run" value={filters.run_id} onChange={(value) => updateFilter('run_id', value)} />
        <FilterInput
          label="Strategy"
          placeholder="b2 / brick"
          value={filters.strategy}
          onChange={(value) => updateFilter('strategy', value)}
        />
        <FilterInput label="Code" placeholder="000001" value={filters.code} onChange={(value) => updateFilter('code', value)} />
        <FilterInput
          label="Review key"
          placeholder="000001_b2"
          value={filters.review_key}
          onChange={(value) => updateFilter('review_key', value)}
        />
        <ArchiveStatusSelect label="Status" value={filters.status} onChange={updateArchiveStatus} />
        <FilterInput label="Rank" placeholder="1" value={filters.rank} onChange={(value) => updateFilter('rank', value)} />
        <button type="button" className="action-button secondary filter-reset" onClick={resetFilters} disabled={!hasFilters}>
          <XCircle size={17} aria-hidden="true" />
          <span>{t('Clear')}</span>
        </button>
      </form>

      {snapshotsQuery.isError || rowsQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(snapshotsQuery.error ?? rowsQuery.error)}</span>
        </div>
      ) : null}

      <div className="archive-grid">
        <section className="archive-snapshot-panel" aria-label={t('Archive dates')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Archive dates')}</h2>
              <p>{snapshotsQuery.isLoading ? t('Loading snapshots') : `${snapshotsQuery.data?.total ?? 0} ${t('snapshots')}`}</p>
            </div>
          </div>

          {snapshotsQuery.isLoading ? <RunSkeleton /> : null}
          {!snapshotsQuery.isLoading && snapshots.length === 0 ? (
            <div className="empty-state">
              <Archive size={24} aria-hidden="true" />
              <h3>{t('No archive snapshots yet')}</h3>
            </div>
          ) : null}

          <div className="archive-snapshot-list">
            {snapshots.map((snapshot) => {
              const selected = snapshot.pick_date === activePickDate && (!filters.run_id.trim() || snapshot.run_id === filters.run_id.trim())
              return (
                <button
                  key={snapshot.id}
                  type="button"
                  className={selected ? 'archive-snapshot-row selected' : 'archive-snapshot-row'}
                  onClick={() => selectSnapshot(snapshot)}
                  aria-label={`Archive ${snapshot.pick_date} run ${snapshot.run_id}`}
                >
                  <span>
                    <strong>{snapshot.pick_date}</strong>
                    <small>{formatDateTime(snapshot.archived_at ?? snapshot.created_at)}</small>
                  </span>
                  <span className="snapshot-counts">
                    <span>{snapshot.candidate_count} {t('rows')}</span>
                    <span>{snapshot.recommended_count} {t('rec')}</span>
                  </span>
                  <code>{snapshot.run_id}</code>
                </button>
              )
            })}
          </div>
        </section>

        <section className="archive-list-panel" aria-label={t('Archive rows')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Archive rows')}</h2>
              <p>
                {rowsQuery.isLoading
                  ? t('Loading archive rows')
                  : activePickDate
                    ? `${rowsQuery.data?.total ?? 0} ${t('records for')} ${activePickDate}`
                    : t('Select an archive date')}
              </p>
            </div>
            <ListSummaryChips
              loaded={rows.length}
              total={rowsQuery.data?.total ?? 0}
              selected={activeArchiveRow ? `${activeArchiveRow.code} / ${activeArchiveRow.strategy}` : null}
              filtered={hasFilters}
            />
          </div>

          {rowsQuery.isLoading ? <RunSkeleton /> : null}
          {!rowsQuery.isLoading && activePickDate && rows.length === 0 ? (
            <div className="empty-state">
              <Search size={24} aria-hidden="true" />
              <h3>{hasFilters ? t('No archive rows match the filters') : t('No rows for this archive date')}</h3>
            </div>
          ) : null}
          {!activePickDate && !snapshotsQuery.isLoading ? (
            <div className="empty-state">
              <Archive size={24} aria-hidden="true" />
              <h3>{t('Select an archive date')}</h3>
            </div>
          ) : null}

          {rows.length > 0 ? <DenseListHeader className="archive-result-header" columns={['Code / strategy', 'Status', 'Rank', 'Close']} /> : null}
          <div className="archive-list">
            {rows.map((row) => (
              <button
                key={row.id}
                type="button"
                className={row.id === activeRowId ? 'archive-row selected' : 'archive-row'}
                onClick={() => setSelectedRowId(row.id)}
                aria-label={`${row.code} ${row.strategy} archive row ${row.review_key}`}
                aria-pressed={row.id === activeRowId}
              >
                <span className="review-row-head">
                  <span className="candidate-code">{row.code}</span>
                  <span className="strategy-chip">{row.strategy}</span>
                </span>
                <span className={`archive-status-chip ${archiveStatusClass(row.status)}`}>{t(archiveStatusLabel(row.status))}</span>
                <CandidateCell label="Rank" value={formatArchiveRank(row)} />
                <CandidateCell label="Close" value={formatNumber(row.close)} strong />
                <RowLineageStrip
                  items={[
                    { label: 'Review key', value: row.review_key, mono: true },
                    { label: 'Run', value: row.run_id, mono: true },
                    { label: 'Chart', value: archiveChartState(row) },
                    { label: 'Chart path', value: row.chart || 'Not linked' },
                  ]}
                />
              </button>
            ))}
          </div>
        </section>

        <section className="detail-panel" aria-label={t('Archive row detail')}>
          {detailQuery.isError ? (
            <div className="alert detail-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(detailQuery.error)}</span>
            </div>
          ) : null}
          {detailQuery.isLoading && activeRowId ? <RunDetailSkeleton /> : null}
          {!activeRowId && !rowsQuery.isLoading ? (
            <div className="empty-state detail-empty">
              <Archive size={24} aria-hidden="true" />
              <h3>{t('Select an archive row')}</h3>
            </div>
          ) : null}
          {detailQuery.data ? <ArchiveDetailPanel row={detailQuery.data.row} /> : null}
        </section>
      </div>
    </div>
  )
}

function MigrationsView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const [dataRoot, setDataRoot] = useState('data')
  const [importScope, setImportScope] = useState<LegacyImportVerifyScope>('candidates')
  const [importPickDate, setImportPickDate] = useState('')
  const [verifyScope, setVerifyScope] = useState<LegacyImportVerifyScope>('candidates')
  const [verifyPickDate, setVerifyPickDate] = useState('')
  const [verifyRunId, setVerifyRunId] = useState('')
  const dryRunMutation = useMutation({
    mutationFn: (root: string) => dryRunLegacyImport(root),
  })
  const importMutation = useMutation({
    mutationFn: ({ root, scope, pickDate }: { root: string; scope: LegacyImportVerifyScope; pickDate: string }) => {
      if (scope === 'candidates') return importLegacyCandidateBatch(root, pickDate)
      if (scope === 'reviews') return importLegacyReviewRun(root, pickDate)
      return importLegacyHistorySnapshot(root, pickDate)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['candidates'] })
      queryClient.invalidateQueries({ queryKey: ['reviews'] })
      queryClient.invalidateQueries({ queryKey: ['archive-snapshots'] })
      queryClient.invalidateQueries({ queryKey: ['archive-rows'] })
    },
  })
  const verifyMutation = useMutation({
    mutationFn: ({ root, scope, pickDate, runId }: { root: string; scope: LegacyImportVerifyScope; pickDate: string; runId: string }) =>
      verifyLegacyImport(root, scope, pickDate, runId.trim() || undefined),
  })
  const report = dryRunMutation.data
  const importSummary = importMutation.isSuccess ? (importMutation.data.import_summary ?? null) : null
  const verifyReport = verifyMutation.isSuccess ? verifyMutation.data : null
  const totals = report?.totals
  const sectionNames = ['candidates', 'reviews', 'history']
  const normalizedRoot = dataRoot.trim() || 'data'

  function runDryRun() {
    dryRunMutation.mutate(normalizedRoot)
  }

  function runImport() {
    if (!importPickDate.trim()) return
    importMutation.mutate({ root: normalizedRoot, scope: importScope, pickDate: importPickDate.trim() })
  }

  function runVerify() {
    if (!verifyPickDate.trim()) return
    verifyMutation.mutate({
      root: normalizedRoot,
      scope: verifyScope,
      pickDate: verifyPickDate.trim(),
      runId: verifyRunId,
    })
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Migration workbench')}</p>
          <h1>{t('Migrations')}</h1>
        </div>
        <span className="pending-chip">{t('Candidates / Reviews / History')}</span>
      </header>

      <section className="summary-strip migration-summary-strip" aria-label={t('Migration dry-run summary')}>
        <Metric label="Files" value={totals ? `${totals.files_valid}/${totals.files_seen}` : 'Not run'} />
        <Metric label="Records" value={totals ? `${totals.records_valid}/${totals.records_seen}` : 'Not run'} />
        <Metric label="Warnings" value={totals ? totals.warning_count.toString() : 'Not run'} />
        <Metric label="Quarantine" value={totals ? totals.quarantine_count.toString() : 'Not run'} />
      </section>

      <section className="migration-control-panel" aria-label={t('Legacy import dry-run control')}>
        <form
          className="migration-form"
          onSubmit={(event) => {
            event.preventDefault()
            runDryRun()
          }}
        >
          <FilterInput label="Data root" placeholder="data" value={dataRoot} onChange={setDataRoot} />
          <button type="submit" className="action-button" disabled={dryRunMutation.isPending}>
            {dryRunMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Play size={17} aria-hidden="true" />}
            <span>{t('Dry run')}</span>
          </button>
        </form>
        <p className="muted migration-note">
          {t('Dry-run scans candidates, reviews, and history before a write operation. Trading account and simulated trading data stay out of this migration flow.')}
        </p>
      </section>

      {dryRunMutation.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(dryRunMutation.error)}</span>
        </div>
      ) : null}

      <div className="migration-action-grid" aria-label={t('Legacy migration actions')}>
        <section className="migration-action-panel" aria-label={t('Legacy import execution')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Import')}</h2>
              <p>{t('Writes one legacy scope into product storage')}</p>
            </div>
            <Upload size={19} aria-hidden="true" />
          </div>
          <form
            className="migration-action-form"
            onSubmit={(event) => {
              event.preventDefault()
              runImport()
            }}
          >
            <ScopeSelector label="Import scope" value={importScope} onChange={setImportScope} />
            <FilterInput label="Pick date" type="date" placeholder="YYYY-MM-DD" value={importPickDate} onChange={setImportPickDate} />
            <button type="submit" className="action-button danger" disabled={importMutation.isPending || !importPickDate.trim()}>
              {importMutation.isPending ? (
                <Loader2 className="spin" size={17} aria-hidden="true" />
              ) : (
                <Upload size={17} aria-hidden="true" />
              )}
              <span>{t('Import scope')}</span>
            </button>
          </form>
          {importMutation.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(importMutation.error)}</span>
            </div>
          ) : null}
          {importSummary ? <ImportSummaryPanel summary={importSummary} /> : null}
        </section>

        <section className="migration-action-panel" aria-label={t('Legacy import verification')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Verify')}</h2>
              <p>{t('Compares legacy files with SQLite and DuckDB records')}</p>
            </div>
            <ShieldCheck size={19} aria-hidden="true" />
          </div>
          <form
            className="migration-action-form"
            onSubmit={(event) => {
              event.preventDefault()
              runVerify()
            }}
          >
            <ScopeSelector label="Verify scope" value={verifyScope} onChange={setVerifyScope} />
            <FilterInput label="Pick date" type="date" placeholder="YYYY-MM-DD" value={verifyPickDate} onChange={setVerifyPickDate} />
            <FilterInput label="Run id" placeholder="Optional" value={verifyRunId} onChange={setVerifyRunId} />
            <button type="submit" className="action-button" disabled={verifyMutation.isPending || !verifyPickDate.trim()}>
              {verifyMutation.isPending ? (
                <Loader2 className="spin" size={17} aria-hidden="true" />
              ) : (
                <ShieldCheck size={17} aria-hidden="true" />
              )}
              <span>{t('Verify')}</span>
            </button>
          </form>
          {verifyMutation.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(verifyMutation.error)}</span>
            </div>
          ) : null}
          {verifyReport ? <VerifyResultPanel report={verifyReport} /> : null}
        </section>
      </div>

      {!report && !dryRunMutation.isPending ? (
        <div className="empty-state migration-empty">
          <Database size={24} aria-hidden="true" />
          <h3>{t('Run a dry-run scan before importing legacy files')}</h3>
        </div>
      ) : null}

      {dryRunMutation.isPending ? <RunSkeleton /> : null}

      {report ? (
        <div className="migration-grid">
          <section className="migration-section-panel" aria-label={t('Migration section reports')}>
            <div className="panel-heading">
              <div>
                <h2>{t('Sections')}</h2>
                <p>{report.data_root}</p>
              </div>
              <span className="status-badge succeeded">
                <CheckCircle2 size={15} aria-hidden="true" />
                {t('Dry run')}
              </span>
            </div>
            <div className="migration-section-grid">
              {sectionNames.map((section) => (
                <MigrationSectionCard key={section} name={section} report={report.sections[section]} />
              ))}
            </div>
          </section>

          <section className="migration-issues-panel" aria-label={t('Migration warnings and quarantine')}>
            <div className="panel-heading">
              <div>
                <h2>{t('Issues')}</h2>
                <p>{report.warnings.length + report.quarantine.length} {t('findings')}</p>
              </div>
            </div>
            <MigrationIssueList title="Warnings" kind="warning" issues={report.warnings} />
            <MigrationIssueList title="Quarantine" kind="quarantine" issues={report.quarantine} />
          </section>
        </div>
      ) : null}
    </div>
  )
}

function RunsView() {
  const { t } = useUiPreferences()
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedRunId = paramValue(searchParams, 'run_id')
  const [selectedRunId, setSelectedRunId] = useState<string | null>(requestedRunId || null)
  const [marketDataForm, setMarketDataForm] = useState<Record<keyof MarketDataRunRequest, string>>({
    config_path: '',
    start: MARKET_DATA_DEFAULT_START_DATE,
    end: todayInputDate(),
    out_dir: '',
    log_path: '',
    workers: '',
  })
  const [preselectForm, setPreselectForm] = useState<Record<keyof PreselectRunRequest, string>>({
    config_path: '',
    data_dir: '',
    pick_date: '',
    end_date: '',
  })

  const healthQuery = useQuery({ queryKey: ['health'], queryFn: getHealth, refetchInterval: 15_000 })
  const runsQuery = useQuery({ queryKey: ['runs'], queryFn: listRuns, refetchInterval: 10_000 })
  const runs = runsQuery.data?.runs ?? []
  const activeRunId = selectedRunId ?? (requestedRunId || runs[0]?.id || '')
  const activeRunSummary = runs.find((run) => run.id === activeRunId)
  const activeRunIsLive = Boolean(
    activeRunId
    && (!activeRunSummary || activeRunSummary.status === 'queued' || activeRunSummary.status === 'running' || activeRunSummary.status === 'cancelling'),
  )

  const detailQuery = useQuery({
    queryKey: ['run', activeRunId],
    queryFn: () => getRun(activeRunId),
    enabled: Boolean(activeRunId),
    refetchInterval: activeRunIsLive ? 4_000 : false,
  })
  const eventsQuery = useQuery({
    queryKey: ['run-events', activeRunId],
    queryFn: () => getRunEvents(activeRunId),
    enabled: Boolean(activeRunId),
    refetchInterval: activeRunIsLive ? 2_000 : false,
  })
  const artifactsQuery = useQuery({
    queryKey: ['run-artifacts', activeRunId],
    queryFn: () => getRunArtifacts(activeRunId),
    enabled: Boolean(activeRunId),
  })

  const createMutation = useMutation({
    mutationFn: () => createDiagnosticRun(false),
    onSuccess: (run) => {
      selectRun(run.id)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.setQueryData(['run', run.id], run)
    },
  })

  const marketDataMutation = useMutation({
    mutationFn: () => createMarketDataRun(compactMarketDataRequest(marketDataForm)),
    onMutate: () => Date.now(),
    onSuccess: (response) => {
      selectRun(response.run.id)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['run', response.run.id] })
      queryClient.invalidateQueries({ queryKey: ['run-events', response.run.id] })
      queryClient.invalidateQueries({ queryKey: ['run-artifacts', response.run.id] })
    },
    onError: async (_error, _variables, startedAt) => {
      const started = startedAt ?? Date.now()
      const response = await queryClient.fetchQuery({ queryKey: ['runs'], queryFn: listRuns })
      const latest = response.runs[0]
      if (latest?.kind === 'market_data' && Date.parse(latest.created_at) >= started - 1_000) {
        selectRun(latest.id)
      }
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      if (latest) {
        queryClient.invalidateQueries({ queryKey: ['run', latest.id] })
        queryClient.invalidateQueries({ queryKey: ['run-events', latest.id] })
        queryClient.invalidateQueries({ queryKey: ['run-artifacts', latest.id] })
      }
    },
  })

  const preselectMutation = useMutation({
    mutationFn: () => createPreselectRun(compactPreselectRequest(preselectForm)),
    onSuccess: (response) => {
      selectRun(response.run.id)
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['candidates'] })
      queryClient.invalidateQueries({ queryKey: ['run', response.run.id] })
    },
  })

  const cancelMutation = useMutation({
    mutationFn: (runId: string) => cancelRun(runId),
    onSuccess: (run) => {
      queryClient.invalidateQueries({ queryKey: ['runs'] })
      queryClient.invalidateQueries({ queryKey: ['run', run.id] })
      queryClient.invalidateQueries({ queryKey: ['run-events', run.id] })
    },
  })

  const healthState = healthQuery.isLoading ? 'checking' : healthQuery.isError ? 'offline' : 'online'
  const marketDataResult = marketDataMutation.isSuccess ? marketDataMutation.data : null
  const preselectResult = preselectMutation.isSuccess ? preselectMutation.data : null

  function updateMarketDataField(key: keyof MarketDataRunRequest, value: string) {
    setMarketDataForm((current) => ({ ...current, [key]: value }))
  }

  function updatePreselectField(key: keyof PreselectRunRequest, value: string) {
    setPreselectForm((current) => ({ ...current, [key]: value }))
  }

  function selectRun(runId: string) {
    setSelectedRunId(runId)
    setSearchParams(nonEmptyParams({ run_id: runId }), { replace: true })
  }

  return (
    <div className="run-center">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t('Workflow control')}</p>
          <h1>{t('Run Center')}</h1>
        </div>
        <div className={`health-chip ${healthState}`}>
          <Server size={17} aria-hidden="true" />
          <span>{healthState === 'online' ? t('API online') : healthState === 'offline' ? t('API offline') : t('Checking API')}</span>
        </div>
      </header>

      <section className="summary-strip" aria-label={t('Runtime summary')}>
        <Metric label="Runs" value={runs.length.toString()} />
        <Metric label="Active" value={runs.filter((run) => run.status === 'queued' || run.status === 'running').length.toString()} />
        <Metric label="Backend" value={healthQuery.data?.stack.backend ?? 'FastAPI'} />
        <Metric label="Storage" value={healthQuery.data?.stack.product_state_database ?? 'SQLite'} />
      </section>

      {healthQuery.isError || runsQuery.isError ? (
        <div className="alert" role="alert">
          <ShieldAlert size={18} aria-hidden="true" />
          <span>{errorText(healthQuery.error ?? runsQuery.error)}</span>
        </div>
      ) : null}

      <div className="workspace-grid">
        <section className="run-list-panel" aria-label={t('Runs')}>
          <div className="panel-heading">
            <div>
              <h2>{t('Runs')}</h2>
              <p>{runsQuery.isLoading ? t('Loading runtime state') : `${runs.length} ${t('records')}`}</p>
            </div>
            <div className="button-row">
              <button
                type="button"
                className="icon-button secondary"
                aria-label={t('Refresh runs')}
                onClick={() => queryClient.invalidateQueries({ queryKey: ['runs'] })}
              >
                <RefreshCw size={17} aria-hidden="true" />
              </button>
              <button
                type="button"
                className="action-button"
                onClick={() => createMutation.mutate()}
                disabled={createMutation.isPending}
              >
                {createMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Play size={17} aria-hidden="true" />}
                <span>{t('Diagnostic')}</span>
              </button>
            </div>
          </div>

          <form
            className="run-setup-form"
            aria-label={t('Market data download setup')}
            onSubmit={(event) => {
              event.preventDefault()
              marketDataMutation.mutate()
            }}
          >
            <div className="run-setup-heading">
              <Database size={18} aria-hidden="true" />
              <h3>{t('Daily data download')}</h3>
            </div>
            <FilterInput
              label="Fetch config path"
              placeholder="config/fetch_kline.yaml"
              value={marketDataForm.config_path}
              onChange={(value) => updateMarketDataField('config_path', value)}
            />
            <FilterInput
              label="Start date"
              type="date"
              placeholder="YYYY-MM-DD"
              value={marketDataForm.start}
              onChange={(value) => updateMarketDataField('start', value)}
            />
            <FilterInput
              label="End date"
              type="date"
              placeholder="YYYY-MM-DD"
              value={marketDataForm.end}
              onChange={(value) => updateMarketDataField('end', value)}
            />
            <FilterInput
              label="Output dir"
              placeholder="data/raw"
              value={marketDataForm.out_dir}
              onChange={(value) => updateMarketDataField('out_dir', value)}
            />
            <FilterInput
              label="Workers"
              placeholder="4"
              value={marketDataForm.workers}
              onChange={(value) => updateMarketDataField('workers', value)}
            />
            <FilterInput
              label="Log path"
              placeholder="auto"
              value={marketDataForm.log_path}
              onChange={(value) => updateMarketDataField('log_path', value)}
            />
            <button type="submit" className="action-button run-setup-submit" disabled={marketDataMutation.isPending}>
              {marketDataMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Database size={17} aria-hidden="true" />}
              <span>{t('Download daily data')}</span>
            </button>
          </form>

          {marketDataMutation.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(marketDataMutation.error)}. {t('If a run was created, diagnostics are recorded in Runs.')}</span>
            </div>
          ) : null}

          {marketDataResult ? (
            <div className="run-setup-result" aria-label={t('Market data result')}>
              <StatusBadge status={marketDataResult.run.status} />
              <DataPair label="Output dir" value={summaryText(marketDataResult.summary, 'out_dir')} />
              <DataPair label="CSV files" value={summaryText(marketDataResult.summary, 'csv_file_count')} />
              <DataPair label="Latest local date" value={summaryText(marketDataResult.summary, 'local_latest_date')} />
            </div>
          ) : null}

          <form
            className="run-setup-form"
            aria-label={t('Preselect run setup')}
            onSubmit={(event) => {
              event.preventDefault()
              preselectMutation.mutate()
            }}
          >
            <div className="run-setup-heading">
              <Search size={18} aria-hidden="true" />
              <h3>{t('Quant preselect')}</h3>
            </div>
            <FilterInput
              label="Config path"
              placeholder="config/rules_preselect.yaml"
              value={preselectForm.config_path}
              onChange={(value) => updatePreselectField('config_path', value)}
            />
            <FilterInput
              label="Data dir"
              placeholder="data/daily"
              value={preselectForm.data_dir}
              onChange={(value) => updatePreselectField('data_dir', value)}
            />
            <FilterInput
              label="Pick date"
              type="date"
              placeholder="YYYY-MM-DD"
              value={preselectForm.pick_date}
              onChange={(value) => updatePreselectField('pick_date', value)}
            />
            <button type="submit" className="action-button run-setup-submit" disabled={preselectMutation.isPending}>
              {preselectMutation.isPending ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Play size={17} aria-hidden="true" />}
              <span>{t('Run preselect')}</span>
            </button>
          </form>

          {preselectMutation.isError ? (
            <div className="alert compact-alert" role="alert">
              <ShieldAlert size={18} aria-hidden="true" />
              <span>{errorText(preselectMutation.error)}</span>
            </div>
          ) : null}

          {preselectResult ? (
            <div className="run-setup-result" aria-label={t('Preselect result')}>
              <StatusBadge status={preselectResult.run.status} />
              <DataPair label="Batch" value={preselectResult.batch.id} />
              <DataPair label="Pick date" value={preselectResult.batch.pick_date} />
              <DataPair label="Candidates" value={preselectResult.batch.total.toString()} />
            </div>
          ) : null}

          {runsQuery.isLoading ? <RunSkeleton /> : null}
          {!runsQuery.isLoading && runs.length === 0 ? (
            <div className="empty-state">
              <Activity size={24} aria-hidden="true" />
              <h3>{t('No product runs yet')}</h3>
              <button type="button" className="action-button" onClick={() => createMutation.mutate()} disabled={createMutation.isPending}>
                <Play size={17} aria-hidden="true" />
                <span>{t('Diagnostic')}</span>
              </button>
            </div>
          ) : null}

          <div className="run-list">
            {runs.map((run) => (
              <button
                key={run.id}
                type="button"
                className={run.id === activeRunId ? 'run-row selected' : 'run-row'}
                onClick={() => selectRun(run.id)}
              >
                <span className={`status-dot ${run.status}`} aria-hidden="true" />
                <span className="run-row-main">
                  <span className="run-kind">{run.kind}</span>
                  <span className="run-id">{run.id}</span>
                </span>
                <StatusBadge status={run.status} />
              </button>
            ))}
          </div>
        </section>

        <section className="detail-panel" aria-label={t('Run detail')}>
          {detailQuery.isLoading && activeRunId ? <RunDetailSkeleton /> : null}
          {!activeRunId && !runsQuery.isLoading ? (
            <div className="empty-state detail-empty">
              <Clock3 size={24} aria-hidden="true" />
              <h3>{t('Select a run')}</h3>
            </div>
          ) : null}
          {detailQuery.data ? (
            <RunDetailPanel
              run={detailQuery.data}
              events={eventsQuery.data?.events ?? detailQuery.data.events}
              artifacts={artifactsQuery.data?.artifacts ?? detailQuery.data.artifacts}
              onCancel={() => cancelMutation.mutate(detailQuery.data.id)}
              cancelling={cancelMutation.isPending}
            />
          ) : null}
        </section>
      </div>
    </div>
  )
}

function RunDetailPanel({
  run,
  events,
  artifacts,
  onCancel,
  cancelling,
}: {
  run: RunSummary & { steps?: { id: number; name: string; status: RunStatus; error: Record<string, unknown> | null }[] }
  events: JobEvent[]
  artifacts: Artifact[]
  onCancel: () => void
  cancelling: boolean
}) {
  const { t } = useUiPreferences()
  const canCancel = run.status === 'queued' || run.status === 'running'
  const diagnostic = failureDiagnosticFromRun(run)

  return (
    <div className="detail-content">
      <div className="detail-header">
        <div>
          <p className="eyebrow">{t('Selected run')}</p>
          <h2>{run.kind}</h2>
          <p className="muted run-id-wrap">{run.id}</p>
        </div>
        <div className="button-row">
          <StatusBadge status={run.status} />
          <button type="button" className="action-button danger" onClick={onCancel} disabled={!canCancel || cancelling}>
            {cancelling ? <Loader2 className="spin" size={17} aria-hidden="true" /> : <Ban size={17} aria-hidden="true" />}
            <span>{t('Cancel')}</span>
          </button>
        </div>
      </div>

      <div className="detail-meta">
        <Metric label="Created" value={formatDateTime(run.created_at)} />
        <Metric label="Started" value={formatDateTime(run.started_at)} />
        <Metric label="Finished" value={formatDateTime(run.finished_at)} />
      </div>

      {diagnostic ? <FailureDiagnosticPanel diagnostic={diagnostic} /> : null}

      <section className="runtime-console-panel" aria-label={t('Runtime console')}>
        <div className="runtime-console-heading">
          <Terminal size={17} aria-hidden="true" />
          <h3>{t('Runtime console')}</h3>
        </div>
        <div className="runtime-console-lines" role="log" aria-live={canCancel ? 'polite' : 'off'}>
          {events.length === 0 ? <p className="muted">{t('Waiting for runtime events')}</p> : null}
          {events.map((event) => (
            <div className={`runtime-console-line ${event.level}`} key={event.id}>
              <time>{formatDateTime(event.created_at)}</time>
              <span>{event.level.toUpperCase()}</span>
              <code>{event.message}</code>
            </div>
          ))}
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Steps')}</h3>
        <div className="step-list">
          {(run.steps ?? []).length === 0 ? <p className="muted">{t('No steps recorded.')}</p> : null}
          {(run.steps ?? []).map((step) => (
            <div className="step-row" key={step.id}>
              <GitBranch size={16} aria-hidden="true" />
              <span>{step.name}</span>
              <StatusBadge status={step.status} />
            </div>
          ))}
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Artifacts')}</h3>
        {artifacts.length === 0 ? <p className="muted">{t('No artifacts linked.')}</p> : null}
        {artifacts.map((artifact) => (
          <div className="artifact-row" key={artifact.id}>
            <Database size={16} aria-hidden="true" />
            <span>{artifact.kind}</span>
            <code>{artifact.path}</code>
            {isProductOwnedArtifactPath(artifact.path) ? (
              <a
                className="artifact-open-link"
                href={artifactFileUrl(artifact.id)}
                target="_blank"
                rel="noreferrer"
                aria-label={`${t('Open')} ${artifact.kind} ${t('artifact')}`}
              >
                <ExternalLink size={15} aria-hidden="true" />
                <span>{t('Open')}</span>
              </a>
            ) : (
              <span className="artifact-source-chip">{t('Legacy path')}</span>
            )}
          </div>
        ))}
      </section>
    </div>
  )
}

type FailureDiagnostic = {
  code?: string
  title?: string
  explanation?: string
  next_actions?: string[]
  retryable?: boolean
  docs?: string[]
}

function FailureDiagnosticPanel({ diagnostic }: { diagnostic: FailureDiagnostic }) {
  const { t } = useUiPreferences()
  return (
    <section className="failure-diagnostic-panel" aria-label={t('Failure diagnostics')}>
      <div className="failure-diagnostic-heading">
        <ShieldAlert size={18} aria-hidden="true" />
        <div>
          <h3>{t('Failure diagnostics')}</h3>
          <p>{diagnostic.title || t('Run failed')}</p>
        </div>
      </div>
      {diagnostic.explanation ? <p className="failure-diagnostic-copy">{diagnostic.explanation}</p> : null}
      {diagnostic.code || diagnostic.retryable !== undefined ? (
        <div className="failure-diagnostic-meta">
          {diagnostic.code ? <DataPair label="Diagnostic code" value={diagnostic.code} /> : null}
          {diagnostic.retryable !== undefined ? (
            <DataPair label="Retryable" value={diagnostic.retryable ? t('Yes') : t('No')} />
          ) : null}
        </div>
      ) : null}
      {diagnostic.next_actions?.length ? (
        <div className="failure-diagnostic-actions">
          <h4>{t('Suggested actions')}</h4>
          <ol>
            {diagnostic.next_actions.map((action) => (
              <li key={action}>{action}</li>
            ))}
          </ol>
        </div>
      ) : null}
      {diagnostic.docs?.length ? (
        <div className="failure-diagnostic-docs">
          <span>{t('Related docs')}</span>
          {diagnostic.docs.map((doc) => (
            <code key={doc}>{doc}</code>
          ))}
        </div>
      ) : null}
    </section>
  )
}

function CandidateDetailPanel({ candidate }: { candidate: Candidate }) {
  const { t } = useUiPreferences()
  return (
    <div className="detail-content">
      <div className="detail-header">
        <div>
          <p className="eyebrow">{t('Selected candidate')}</p>
          <h2>{candidate.code}</h2>
          <p className="muted run-id-wrap">{candidate.batch_id}</p>
        </div>
        <span className="strategy-chip large">{candidate.strategy}</span>
      </div>

      <div className="detail-meta candidate-metrics">
        <Metric label="Pick date" value={candidate.pick_date} />
        <Metric label="Close" value={formatNumber(candidate.close)} />
        <Metric label="Turnover" value={formatNumber(candidate.turnover_n)} />
        <Metric label="Brick growth" value={formatNumber(candidate.brick_growth)} />
      </div>

      <section className="subsection">
        <h3>{t('Evidence route')}</h3>
        <div className="evidence-link-grid">
          <EvidenceLinkCard
            icon="runs"
            title="Run center"
            detail={candidate.run_id}
            to={evidencePath('/runs', { run_id: candidate.run_id })}
          />
          <EvidenceLinkCard
            icon="reviews"
            title="Review rows"
            detail={`${candidate.code} ${candidate.strategy}`}
            to={evidencePath('/reviews', {
              candidate_batch_id: candidate.batch_id,
              code: candidate.code,
              strategy: candidate.strategy,
            })}
          />
          <EvidenceLinkCard
            icon="archive"
            title="Archive rows"
            detail={candidate.pick_date}
            to={evidencePath('/archive', {
              pick_date: candidate.pick_date,
              code: candidate.code,
              strategy: candidate.strategy,
            })}
          />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Lineage')}</h3>
        <div className="lineage-grid">
          <DataPair label="Candidate id" value={candidate.id.toString()} />
          <DataPair label="Batch id" value={candidate.batch_id} />
          <DataPair label="Run id" value={candidate.run_id} />
          <DataPair label="Source" value={candidate.batch.source} />
          <DataPair label="Batch date" value={candidate.batch.pick_date} />
          <DataPair label="Created" value={formatDateTime(candidate.created_at)} />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Strategy counts')}</h3>
        <pre className="json-block">{jsonPreview(candidate.batch.strategy_counts)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Extra')}</h3>
        <pre className="json-block">{jsonPreview(candidate.extra)}</pre>
      </section>
    </div>
  )
}

function ReviewDetailPanel({ review }: { review: Review }) {
  const { t } = useUiPreferences()
  return (
    <div className="detail-content">
      <div className="detail-header">
        <div>
          <p className="eyebrow">{t('Selected review')}</p>
          <h2>{review.code}</h2>
          <p className="muted run-id-wrap">{review.review_key}</p>
        </div>
        <div className="review-detail-actions">
          <span className="strategy-chip large">{review.strategy}</span>
          <span className={`verdict-chip large ${verdictClass(review.verdict)}`}>{review.verdict ?? t('No verdict')}</span>
        </div>
      </div>

      <div className="detail-meta review-metrics">
        <Metric label="Pick date" value={review.pick_date} />
        <Metric label="Score" value={formatNumber(review.total_score)} />
        <Metric label="Rank" value={formatRank(review)} />
        <Metric label="Provider" value={reviewerName(review)} />
      </div>

      <section className="subsection">
        <h3>{t('Evidence route')}</h3>
        <div className="evidence-link-grid">
          <EvidenceLinkCard
            icon="candidates"
            title="Candidate row"
            detail={review.candidate_batch_id ?? 'No batch link'}
            to={review.candidate_batch_id
              ? evidencePath('/candidates', {
                batch_id: review.candidate_batch_id,
                code: review.code,
                strategy: review.strategy,
              })
              : undefined}
          />
          <EvidenceLinkCard
            icon="archive"
            title="Archive row"
            detail={review.review_key}
            to={evidencePath('/archive', {
              pick_date: review.pick_date,
              code: review.code,
              strategy: review.strategy,
              review_key: review.review_key,
            })}
          />
          <EvidenceLinkCard
            icon="runs"
            title="Run center"
            detail={review.run_id}
            to={evidencePath('/runs', { run_id: review.run_id })}
          />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Lineage')}</h3>
        <div className="lineage-grid">
          <DataPair label="Review id" value={review.id.toString()} />
          <DataPair label="Review run" value={review.review_run_id} />
          <DataPair label="Run id" value={review.run_id} />
          <DataPair label="Candidate batch" value={review.candidate_batch_id ?? 'Not linked'} />
          <DataPair label="Candidate id" value={review.candidate_id?.toString() ?? 'Not linked'} />
          <DataPair label="Created" value={formatDateTime(review.created_at)} />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Review payload')}</h3>
        <pre className="json-block">{jsonPreview(review.payload)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Review run summary')}</h3>
        <pre className="json-block">{jsonPreview(review.review_run.summary)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Recommendation payload')}</h3>
        {review.recommendation ? (
          <pre className="json-block">{jsonPreview(review.recommendation.payload)}</pre>
        ) : (
          <p className="muted">{t('This review is not in the recommendation list.')}</p>
        )}
      </section>
    </div>
  )
}

function ArchiveDetailPanel({ row }: { row: ArchiveRow }) {
  const { t } = useUiPreferences()
  return (
    <div className="detail-content">
      <div className="detail-header">
        <div>
          <p className="eyebrow">{t('Selected archive row')}</p>
          <h2>{row.code}</h2>
          <p className="muted run-id-wrap">{row.review_key}</p>
        </div>
        <div className="review-detail-actions">
          <span className="strategy-chip large">{row.strategy}</span>
          <span className={`archive-status-chip large ${archiveStatusClass(row.status)}`}>{t(archiveStatusLabel(row.status))}</span>
        </div>
      </div>

      <div className="detail-meta archive-metrics">
        <Metric label="Pick date" value={row.pick_date} />
        <Metric label="Rank" value={formatArchiveRank(row)} />
        <Metric label="Close" value={formatNumber(row.close)} />
        <Metric label="Turnover" value={formatNumber(row.turnover_n)} />
      </div>

      <ArchiveChartEvidence row={row} />

      <section className="subsection">
        <h3>{t('Evidence route')}</h3>
        <div className="evidence-link-grid">
          <EvidenceLinkCard
            icon="candidates"
            title="Candidate row"
            detail={row.candidate_batch_id ?? 'No batch link'}
            to={row.candidate_batch_id
              ? evidencePath('/candidates', {
                batch_id: row.candidate_batch_id,
                code: row.code,
                strategy: row.strategy,
              })
              : undefined}
          />
          <EvidenceLinkCard
            icon="reviews"
            title="Review row"
            detail={row.review_key}
            to={row.review_run_id
              ? evidencePath('/reviews', {
                review_run_id: row.review_run_id,
                review_key: row.review_key,
                code: row.code,
                strategy: row.strategy,
              })
              : undefined}
          />
          <EvidenceLinkCard
            icon="runs"
            title="Run center"
            detail={row.run_id}
            to={evidencePath('/runs', { run_id: row.run_id })}
          />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Lineage')}</h3>
        <div className="lineage-grid">
          <DataPair label="Archive row" value={row.id.toString()} />
          <DataPair label="Snapshot" value={row.snapshot_id} />
          <DataPair label="Run id" value={row.run_id} />
          <DataPair label="Candidate batch" value={row.candidate_batch_id ?? 'Not linked'} />
          <DataPair label="Review run" value={row.review_run_id ?? 'Not linked'} />
          <DataPair label="Candidate id" value={row.candidate_id?.toString() ?? 'Not linked'} />
          <DataPair label="Review id" value={row.review_id?.toString() ?? 'Not linked'} />
          <DataPair label="Recommendation id" value={row.recommendation_id?.toString() ?? 'Not linked'} />
          <DataPair label="Chart artifact" value={row.chart_artifact_id ?? 'Not linked'} />
          <DataPair label="Archived" value={formatDateTime(row.snapshot.archived_at ?? row.snapshot.created_at)} />
          <DataPair label="Chart" value={row.chart ?? 'Not linked'} />
        </div>
      </section>

      <section className="subsection">
        <h3>{t('Snapshot summary')}</h3>
        <div className="lineage-grid">
          <DataPair label="Candidates" value={row.snapshot.candidate_count.toString()} />
          <DataPair label="Recommended" value={row.snapshot.recommended_count.toString()} />
          <DataPair label="Reviewed" value={row.snapshot.reviewed_count.toString()} />
          <DataPair label="Threshold" value={formatNumber(row.snapshot.min_score_threshold)} />
        </div>
        <pre className="json-block">{jsonPreview(row.snapshot.summary)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Extra')}</h3>
        <pre className="json-block">{jsonPreview(row.extra)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Review payload')}</h3>
        <pre className="json-block">{jsonPreview(row.review_payload)}</pre>
      </section>

      <section className="subsection">
        <h3>{t('Strategy counts')}</h3>
        <pre className="json-block">{jsonPreview(row.snapshot.strategy_counts)}</pre>
      </section>
    </div>
  )
}

function ArchiveChartEvidence({ row }: { row: ArchiveRow }) {
  const { t } = useUiPreferences()
  const artifactId = row.chart_artifact_id
  const hasProductArtifact = artifactId !== null

  if (!hasProductArtifact) {
    if (!row.chart) {
      return (
        <section className="subsection evidence-asset-section" aria-label={t('Chart evidence')}>
          <div className="evidence-asset-panel missing">
            <div className="evidence-asset-head">
              <ImageIcon size={18} aria-hidden="true" />
              <div>
                <h3>{t('No chart linked')}</h3>
                <p>{t('Archive row is stored, but no chart artifact or legacy chart path is linked.')}</p>
              </div>
              <div className="evidence-asset-actions">
                <span className="artifact-source-chip">{t('Missing chart')}</span>
              </div>
            </div>
          </div>
        </section>
      )
    }

    return (
      <section className="subsection evidence-asset-section" aria-label={t('Chart evidence')}>
        <div className="evidence-asset-panel legacy">
          <div className="evidence-asset-head">
            <ImageIcon size={18} aria-hidden="true" />
            <div>
              <h3>{t('Legacy chart reference')}</h3>
              <p>{t('This path is legacy source material and is not served by the product artifact API.')}</p>
            </div>
            <div className="evidence-asset-actions">
              <span className="artifact-source-chip">{t('Legacy path')}</span>
            </div>
          </div>
          <code className="evidence-asset-path">{row.chart}</code>
        </div>
      </section>
    )
  }

  const artifactUrl = artifactFileUrl(artifactId)

  return (
    <section className="subsection evidence-asset-section" aria-label={t('Chart evidence')}>
      <div className="evidence-asset-panel product">
        <div className="evidence-asset-head">
          <ImageIcon size={18} aria-hidden="true" />
          <div>
            <h3>{t('Product chart evidence')}</h3>
            <p>{t('Served by the product artifact API and linked to this archive row.')}</p>
          </div>
          <div className="evidence-asset-actions">
            <span className="status-badge succeeded">
              <CheckCircle2 size={15} aria-hidden="true" />
              {t('Linked')}
            </span>
            <a className="artifact-open-link" href={artifactUrl} target="_blank" rel="noreferrer" aria-label={t('Open chart artifact')}>
              <ExternalLink size={15} aria-hidden="true" />
              <span>{t('Open')}</span>
            </a>
          </div>
        </div>
        <div className="evidence-asset-meta">
          <DataPair label="Artifact id" value={row.chart_artifact_id ?? 'Not linked'} />
          <DataPair label="Chart path" value={row.chart ?? 'Product artifact only'} />
        </div>
        <img className="artifact-preview-image" src={artifactUrl} alt={`${row.code} ${row.strategy} chart evidence`} loading="lazy" />
      </div>
    </section>
  )
}

function MigrationSectionCard({ name, report }: { name: string; report?: LegacyImportSectionReport }) {
  const { t } = useUiPreferences()
  return (
    <div className="migration-section-card">
      <div className="migration-section-head">
        <Database size={17} aria-hidden="true" />
        <h3>{t(sectionLabel(name))}</h3>
      </div>
      <div className="section-count-grid">
        <DataPair label="Files" value={report ? `${report.files_valid}/${report.files_seen}` : '0/0'} />
        <DataPair label="Records" value={report ? `${report.records_valid}/${report.records_seen}` : '0/0'} />
      </div>
      <pre className="json-block compact">{jsonPreview(report?.by_kind ?? null)}</pre>
    </div>
  )
}

function MigrationIssueList({
  title,
  kind,
  issues,
}: {
  title: string
  kind: 'warning' | 'quarantine'
  issues: LegacyImportIssue[]
}) {
  const { t } = useUiPreferences()
  return (
    <section className="migration-issue-section">
      <div className="migration-issue-heading">
        <h3>{t(title)}</h3>
        <span className={`issue-kind ${kind}`}>{issues.length}</span>
      </div>
      {issues.length === 0 ? (
        <p className="muted migration-issue-empty">{t(title === 'Warnings' ? 'No warnings found.' : 'No quarantine found.')}</p>
      ) : (
        <div className="migration-issue-list">
          {issues.map((issue, index) => (
            <div className="migration-issue-row" key={`${issue.section}-${issue.source_path}-${issue.reason}-${issue.record_key ?? index}`}>
              <span className={`issue-kind ${kind}`}>{t(sectionLabel(issue.section))}</span>
              <div>
                <strong>{issue.reason}</strong>
                <p>{issue.message}</p>
                <code>{issue.source_path}</code>
                {issue.record_key ? <small>{issue.record_key}</small> : null}
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

function ScopeSelector({
  label,
  value,
  onChange,
}: {
  label: string
  value: LegacyImportVerifyScope
  onChange: (value: LegacyImportVerifyScope) => void
}) {
  const { t } = useUiPreferences()
  return (
    <fieldset className="scope-selector">
      <legend>{t(label)}</legend>
      <div className="scope-button-grid">
        {legacyMigrationScopes.map((scope) => (
          <button
            key={scope.value}
            type="button"
            className={scope.value === value ? 'scope-button selected' : 'scope-button'}
            aria-pressed={scope.value === value}
            onClick={() => onChange(scope.value)}
          >
            <strong>{t(scope.label)}</strong>
            <span>{t(scope.description)}</span>
          </button>
        ))}
      </div>
    </fieldset>
  )
}

function ImportSummaryPanel({ summary }: { summary: LegacyImportSummary }) {
  const { t } = useUiPreferences()
  return (
    <div className="migration-result-panel">
      <div className="migration-result-head">
        <span className="status-badge succeeded">
          <CheckCircle2 size={15} aria-hidden="true" />
          {t('Imported')}
        </span>
        <span className="muted">{summary.pick_date}</span>
      </div>
      <div className="migration-result-grid">
        <DataPair label="Run id" value={summary.run_id} />
        <DataPair label="Source file" value={summary.source_file} />
        <DataPair label="Backup id" value={summary.pre_import_backup_id ?? 'Not created'} />
        <DataPair label="Backup path" value={summary.pre_import_backup_path ?? 'Not created'} />
        <DataPair label="Batch id" value={summary.batch_id ?? 'Not linked'} />
        <DataPair label="Review run" value={summary.review_run_id ?? 'Not linked'} />
        <DataPair label="Archive snapshot" value={summary.archive_snapshot_id ?? 'Not linked'} />
      </div>
      <div className="migration-count-grid" aria-label={t('Imported record counts')}>
        <Metric label="Candidates" value={summary.candidates_imported.toString()} />
        <Metric label="Reviews" value={summary.reviews_imported.toString()} />
        <Metric label="Recommendations" value={summary.recommendations_imported.toString()} />
        <Metric label="Archive rows" value={summary.archive_rows_imported.toString()} />
      </div>
      <pre className="json-block compact">{jsonPreview(summary.strategy_counts)}</pre>
    </div>
  )
}

function VerifyResultPanel({ report }: { report: LegacyImportVerifyReport }) {
  const { t } = useUiPreferences()
  const mismatchEntries = [
    { label: 'Missing in SQLite', values: report.mismatches.missing_in_sqlite },
    { label: 'Extra in SQLite', values: report.mismatches.extra_in_sqlite },
    { label: 'Missing in DuckDB', values: report.mismatches.missing_in_duckdb },
    { label: 'Extra in DuckDB', values: report.mismatches.extra_in_duckdb },
  ]
  const mismatchCount = mismatchEntries.reduce((total, entry) => total + entry.values.length, 0)

  return (
    <div className="migration-result-panel">
      <div className="migration-result-head">
        <span className={report.passed ? 'status-badge succeeded' : 'status-badge failed'}>
          {report.passed ? <CheckCircle2 size={15} aria-hidden="true" /> : <XCircle size={15} aria-hidden="true" />}
          {report.passed ? t('Verified') : t('Mismatch')}
        </span>
        <span className="muted">{t(sectionLabel(report.scope))} / {report.pick_date}</span>
      </div>
      <div className="migration-result-grid">
        <DataPair label="Source path" value={report.source_path} />
        <DataPair label="Run id" value={report.run_id ?? 'Not filtered'} />
        <DataPair label="DuckDB check" value={report.duckdb_checked ? 'Checked' : 'Skipped'} />
      </div>
      <div className="migration-count-grid" aria-label={t('Verification record counts')}>
        <Metric label="Legacy" value={report.counts.legacy.toString()} />
        <Metric label="SQLite" value={report.counts.sqlite.toString()} />
        <Metric label="DuckDB" value={report.counts.duckdb === null ? 'Skipped' : report.counts.duckdb.toString()} />
        <Metric label="Mismatches" value={mismatchCount.toString()} />
      </div>
      {mismatchCount === 0 ? (
        <p className="muted migration-issue-empty">{t('No mismatches found.')}</p>
      ) : (
        <div className="migration-mismatch-list">
          {mismatchEntries
            .filter((entry) => entry.values.length > 0)
            .map((entry) => (
              <div className="migration-mismatch-row" key={entry.label}>
                <strong>{t(entry.label)}</strong>
                <code>{entry.values.join(', ')}</code>
              </div>
            ))}
        </div>
      )}
    </div>
  )
}

function StrategyCard({ strategy, selected }: { strategy: StrategyDefinition; selected: boolean }) {
  const { t } = useUiPreferences()
  return (
    <article className={selected ? 'strategy-card selected' : 'strategy-card'}>
      <span className="strategy-card-head">
        <strong>{strategy.label}</strong>
        <span className="strategy-chip">{selected ? t('Default') : strategy.enabled_by_default ? t('Config default') : t('Available')}</span>
      </span>
      <p>{strategy.description}</p>
      <small>{strategy.config_provenance.path} / {strategy.config_provenance.section}</small>
    </article>
  )
}

function StrategySummaryList({ rows }: { rows: StrategySummaryRow[] }) {
  const { t } = useUiPreferences()
  if (rows.length === 0) return null
  return (
    <div className="strategy-summary-list">
      {rows.map((row) => (
        <div key={`${row.pick_date}-${row.run_id}-${row.strategy}`} className="strategy-summary-row">
          <span>
            <strong>{row.pick_date}</strong>
            <small>{row.run_id}</small>
          </span>
          <span className="strategy-chip">{row.strategy}</span>
          <span>{row.total} {t('total')}</span>
          <span>{formatPercent(row.recommended_rate)} {t('rec')}</span>
        </div>
      ))}
    </div>
  )
}

function NumberPreference({
  label,
  value,
  min,
  max,
  onChange,
}: {
  label: string
  value: number
  min: number
  max: number
  onChange: (value: number) => void
}) {
  const { t } = useUiPreferences()
  return (
    <label className="filter-field">
      <span>{t(label)}</span>
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </label>
  )
}

function SettingsInventory({ settings }: { settings: ProductSettingsResponse }) {
  const { t } = useUiPreferences()
  return (
    <div className="settings-inventory">
      <div className="overview-state-list">
        <DataPair label="SQLite" value={settings.local_state.sqlite_path} />
        <DataPair label="DuckDB" value={settings.local_state.duckdb_path ?? 'Disabled'} />
        <DataPair label="Artifacts" value={settings.local_state.artifact_root} />
        <DataPair label="Backups" value={settings.local_state.backup_root} />
      </div>

      <div className="settings-subsection">
        <h3>{t('Safe config files')}</h3>
        <div className="settings-list">
          {settings.config_files.map((config) => (
            <div key={config.key} className="settings-list-row">
              <Cog size={16} aria-hidden="true" />
              <span>
                <strong>{config.key}</strong>
                <small>{config.path}</small>
              </span>
              <span className={config.exists ? 'status-badge succeeded' : 'status-badge failed'}>
                {config.exists ? <CheckCircle2 size={15} aria-hidden="true" /> : <XCircle size={15} aria-hidden="true" />}
                {config.exists ? t('Found') : t('Missing')}
              </span>
            </div>
          ))}
        </div>
      </div>

      <div className="settings-subsection">
        <h3>{t('External integrations')}</h3>
        <div className="settings-list">
          {settings.external_integrations.map((integration) => (
            <div key={integration.key} className="settings-list-row">
              <Server size={16} aria-hidden="true" />
              <span>
                <strong>{integration.label}</strong>
                <small>{integration.source}</small>
              </span>
              <span className={integration.configured ? 'status-badge succeeded' : 'status-badge queued'}>
                {integration.configured ? <ShieldCheck size={15} aria-hidden="true" /> : <ShieldAlert size={15} aria-hidden="true" />}
                {integration.configured ? t('Configured') : t('Not configured')}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function FilterInput({
  label,
  value,
  placeholder,
  type = 'text',
  onChange,
}: {
  label: string
  value: string
  placeholder: string
  type?: 'text' | 'date'
  onChange: (value: string) => void
}) {
  const { t } = useUiPreferences()
  return (
    <label className="filter-field">
      <span>{t(label)}</span>
      <input type={type} value={value} placeholder={t(placeholder)} onChange={(event) => onChange(event.target.value)} />
    </label>
  )
}

function FilterSelect({
  label,
  value,
  onChange,
}: {
  label: string
  value: RecommendationStatus
  onChange: (value: RecommendationStatus) => void
}) {
  const { t } = useUiPreferences()
  return (
    <label className="filter-field">
      <span>{t(label)}</span>
      <select value={value} onChange={(event) => onChange(event.target.value as RecommendationStatus)}>
        <option value="all">{t('All reviews')}</option>
        <option value="recommended">{t('Recommended')}</option>
        <option value="reviewed">{t('Reviewed only')}</option>
      </select>
    </label>
  )
}

function ArchiveStatusSelect({
  label,
  value,
  onChange,
}: {
  label: string
  value: ArchiveStatus
  onChange: (value: ArchiveStatus) => void
}) {
  const { t } = useUiPreferences()
  return (
    <label className="filter-field">
      <span>{t(label)}</span>
      <select value={value} onChange={(event) => onChange(event.target.value as ArchiveStatus)}>
        <option value="all">{t('All rows')}</option>
        <option value="recommended">{t('Recommended')}</option>
        <option value="reviewed">{t('Reviewed')}</option>
        <option value="unreviewed">{t('Unreviewed')}</option>
      </select>
    </label>
  )
}

function DataPair({ label, value }: { label: string; value: string }) {
  const { t } = useUiPreferences()
  return (
    <div className="data-pair">
      <span>{t(label)}</span>
      <strong>{t(value)}</strong>
    </div>
  )
}

type EvidenceStepState = 'done' | 'ready' | 'waiting'

function BatchEvidenceFlow({ batch }: { batch: CandidateBatchSummary }) {
  const { t } = useUiPreferences()
  const hasCandidates = batch.candidate_count > 0
  const hasReview = Boolean(batch.latest_review_run_id)
  const hasArchive = batch.archive_snapshot_count > 0
  const steps: { label: string; value: string; state: EvidenceStepState }[] = [
    {
      label: 'Candidates',
      value: `${batch.candidate_count}`,
      state: hasCandidates ? 'done' : 'waiting',
    },
    {
      label: 'Charts',
      value: hasCandidates ? 'ready' : 'blocked',
      state: hasCandidates ? 'ready' : 'waiting',
    },
    {
      label: 'Review',
      value: hasReview ? `${batch.latest_reviewed_count}` : hasCandidates ? 'ready' : 'blocked',
      state: hasReview ? 'done' : hasCandidates ? 'ready' : 'waiting',
    },
    {
      label: 'Archive',
      value: hasArchive ? `${batch.archive_snapshot_count}` : hasReview ? 'ready' : 'blocked',
      state: hasArchive ? 'done' : hasReview ? 'ready' : 'waiting',
    },
  ]

  return (
    <span className="evidence-flow" aria-label={t('Batch evidence workflow')}>
      {steps.map((step) => (
        <span key={step.label} className={`evidence-step ${step.state}`}>
          {step.state === 'done' ? <CheckCircle2 size={14} aria-hidden="true" /> : <Clock3 size={14} aria-hidden="true" />}
          <span>{t(step.label)}</span>
          <strong>{t(step.value)}</strong>
        </span>
      ))}
    </span>
  )
}

function EvidenceLinkCard({
  icon,
  title,
  detail,
  to,
}: {
  icon: 'archive' | 'candidates' | 'reviews' | 'runs'
  title: string
  detail: string
  to?: string
}) {
  const { t } = useUiPreferences()
  const Icon = icon === 'archive' ? Archive : icon === 'reviews' ? FileSearch : icon === 'runs' ? Activity : Search
  const content = (
    <>
      <Icon size={17} aria-hidden="true" />
      <span>
        <strong>{t(title)}</strong>
        <small>{t(detail)}</small>
      </span>
    </>
  )

  if (!to) {
    return (
      <span className="evidence-link-card disabled" aria-disabled="true">
        {content}
      </span>
    )
  }

  return (
    <Link className="evidence-link-card" to={to}>
      {content}
    </Link>
  )
}

function CandidateCell({
  label,
  value,
  mono = false,
  strong = false,
  wide = false,
  extra = false,
}: {
  label: string
  value: string
  mono?: boolean
  strong?: boolean
  wide?: boolean
  extra?: boolean
}) {
  const { t } = useUiPreferences()
  const className = ['candidate-cell', mono ? 'mono' : '', wide ? 'wide' : '', extra ? 'extra' : ''].filter(Boolean).join(' ')
  return (
    <span className={className} title={value}>
      <span>{t(label)}</span>
      <strong className={strong ? 'numeric' : undefined}>{t(value)}</strong>
    </span>
  )
}

function DenseListHeader({ className, columns }: { className: string; columns: string[] }) {
  const { t } = useUiPreferences()
  return (
    <div className={`dense-list-header ${className}`} aria-hidden="true">
      {columns.map((column) => (
        <span key={column}>{t(column)}</span>
      ))}
    </div>
  )
}

function ListSummaryChips({
  loaded,
  total,
  selected,
  filtered,
}: {
  loaded: number
  total: number
  selected: string | null
  filtered: boolean
}) {
  const { t } = useUiPreferences()
  return (
    <div className="list-summary-chips" aria-label={t('List summary')}>
      <span className="list-summary-chip">{loaded}/{total} {t('loaded')}</span>
      {selected ? <span className="list-summary-chip selected">{t('Selected')} {selected}</span> : null}
      {filtered ? <span className="list-summary-chip filtered">{t('Filtered')}</span> : null}
    </div>
  )
}

function RowLineageStrip({
  items,
}: {
  items: Array<{
    label: string
    value: string
    mono?: boolean
  }>
}) {
  const { t } = useUiPreferences()
  return (
    <span className="row-lineage-strip">
      {items.map((item) => (
        <span key={item.label} className="row-lineage-item" title={item.value}>
          <span>{t(item.label)}</span>
          <strong className={item.mono ? 'mono' : undefined}>{t(item.value)}</strong>
        </span>
      ))}
    </span>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  const { t } = useUiPreferences()
  return (
    <div className="metric">
      <span>{t(label)}</span>
      <strong>{t(value)}</strong>
    </div>
  )
}

function StatusBadge({ status }: { status: RunStatus }) {
  const { t } = useUiPreferences()
  const Icon = status === 'succeeded' ? CheckCircle2 : status === 'failed' || status === 'cancelled' ? XCircle : Clock3
  return (
    <span className={`status-badge ${status}`}>
      <Icon size={15} aria-hidden="true" />
      {t(statusLabels[status])}
    </span>
  )
}

function RunSkeleton() {
  const { t } = useUiPreferences()
  return (
    <div className="skeleton-stack" aria-label={t('Loading runs')}>
      <span />
      <span />
      <span />
    </div>
  )
}

function RunDetailSkeleton() {
  const { t } = useUiPreferences()
  return (
    <div className="detail-content" aria-label={t('Loading run detail')}>
      <div className="skeleton-title" />
      <div className="skeleton-grid">
        <span />
        <span />
        <span />
      </div>
      <div className="skeleton-block" />
    </div>
  )
}

function formatDateTime(value: string | null) {
  if (!value) return 'Not set'
  return new Intl.DateTimeFormat(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value))
}

function todayInputDate() {
  const now = new Date()
  const localDate = new Date(now.getTime() - now.getTimezoneOffset() * 60_000)
  return localDate.toISOString().slice(0, 10)
}

function formatNumber(value: number | null) {
  if (value === null || Number.isNaN(value)) return 'Not set'
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 }).format(value)
}

function formatPercent(value: number) {
  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value * 100)}%`
}

function formatRank(review: Review) {
  return review.recommendation ? `#${review.recommendation.rank}` : 'No rank'
}

function reviewerName(review: Review) {
  return review.reviewer || review.review_run.provider || 'Not set'
}

function findActiveSnapshot(snapshots: ArchiveSnapshot[], pickDate: string, runId: string) {
  if (!pickDate) return undefined
  return snapshots.find((snapshot) => snapshot.pick_date === pickDate && (!runId || snapshot.run_id === runId))
    ?? snapshots.find((snapshot) => snapshot.pick_date === pickDate)
}

function verdictClass(verdict: string | null) {
  const normalized = verdict?.toLowerCase()
  if (normalized === 'pass') return 'pass'
  if (normalized === 'fail') return 'fail'
  if (normalized === 'watch') return 'watch'
  return 'neutral'
}

function archiveStatusLabel(status: ArchiveRow['status']) {
  if (status === 'recommended') return 'Recommended'
  if (status === 'reviewed') return 'Reviewed'
  return 'Unreviewed'
}

function archiveStatusClass(status: ArchiveRow['status']) {
  if (status === 'recommended') return 'recommended'
  if (status === 'reviewed') return 'reviewed'
  return 'unreviewed'
}

function archiveChartState(row: ArchiveRow) {
  if (row.chart_artifact_id) return 'Artifact linked'
  if (row.chart) return 'Legacy path'
  return 'Not linked'
}

function formatArchiveRank(row: ArchiveRow) {
  return row.rank ? `#${row.rank}` : 'No rank'
}

function sectionLabel(section: string) {
  if (section === 'candidates') return 'Candidates'
  if (section === 'reviews') return 'Reviews'
  if (section === 'history') return 'History'
  return section
}

function emptyCandidateFilters(): Required<CandidateFilters> {
  return {
    batch_id: '',
    pick_date: '',
    run_id: '',
    strategy: '',
    code: '',
  }
}

function candidateFiltersFromParams(params: URLSearchParams): Required<CandidateFilters> {
  return {
    batch_id: paramValue(params, 'batch_id'),
    pick_date: paramValue(params, 'pick_date'),
    run_id: paramValue(params, 'run_id'),
    strategy: paramValue(params, 'strategy'),
    code: paramValue(params, 'code'),
  }
}

function candidateFiltersToParams(filters: Required<CandidateFilters>) {
  return nonEmptyParams({
    batch_id: filters.batch_id,
    pick_date: filters.pick_date,
    run_id: filters.run_id,
    strategy: filters.strategy,
    code: filters.code,
  })
}

function emptyReviewFilters(): Required<ReviewFilters> {
  return {
    pick_date: '',
    run_id: '',
    review_run_id: '',
    candidate_batch_id: '',
    strategy: '',
    code: '',
    review_key: '',
    reviewer: '',
    recommendation_status: 'all',
  }
}

function reviewFiltersFromParams(params: URLSearchParams): Required<ReviewFilters> {
  return {
    pick_date: paramValue(params, 'pick_date'),
    run_id: paramValue(params, 'run_id'),
    review_run_id: paramValue(params, 'review_run_id'),
    candidate_batch_id: paramValue(params, 'candidate_batch_id'),
    strategy: paramValue(params, 'strategy'),
    code: paramValue(params, 'code'),
    review_key: paramValue(params, 'review_key'),
    reviewer: paramValue(params, 'reviewer'),
    recommendation_status: recommendationStatusValue(params.get('recommendation_status')),
  }
}

function reviewFiltersToParams(filters: Required<ReviewFilters>) {
  const params = nonEmptyParams({
    pick_date: filters.pick_date,
    run_id: filters.run_id,
    review_run_id: filters.review_run_id,
    candidate_batch_id: filters.candidate_batch_id,
    strategy: filters.strategy,
    code: filters.code,
    review_key: filters.review_key,
    reviewer: filters.reviewer,
  })
  if (filters.recommendation_status !== 'all') {
    params.set('recommendation_status', filters.recommendation_status)
  }
  return params
}

function emptyArchiveFilters(): Required<ArchiveRowFilters> {
  return {
    pick_date: '',
    run_id: '',
    strategy: '',
    code: '',
    review_key: '',
    status: 'all',
    rank: '',
  }
}

function archiveFiltersFromParams(params: URLSearchParams): Required<ArchiveRowFilters> {
  return {
    pick_date: paramValue(params, 'pick_date'),
    run_id: paramValue(params, 'run_id'),
    strategy: paramValue(params, 'strategy'),
    code: paramValue(params, 'code'),
    review_key: paramValue(params, 'review_key'),
    status: archiveStatusValue(params.get('status')),
    rank: paramValue(params, 'rank'),
  }
}

function archiveFiltersToParams(filters: Required<ArchiveRowFilters>) {
  const params = nonEmptyParams({
    pick_date: filters.pick_date,
    run_id: filters.run_id,
    strategy: filters.strategy,
    code: filters.code,
    review_key: filters.review_key,
    rank: filters.rank,
  })
  if (filters.status !== 'all') {
    params.set('status', filters.status)
  }
  return params
}

function emptyStrategySummaryFilters(): Required<StrategySummaryFilters> {
  return {
    pick_date: '',
    run_id: '',
    strategy: '',
    limit: '100',
  }
}

function strategySummaryFiltersFromParams(params: URLSearchParams): Required<StrategySummaryFilters> {
  return {
    pick_date: paramValue(params, 'pick_date'),
    run_id: paramValue(params, 'run_id'),
    strategy: paramValue(params, 'strategy'),
    limit: paramValue(params, 'limit') || '100',
  }
}

function strategySummaryFiltersToParams(filters: Required<StrategySummaryFilters>) {
  const params = nonEmptyParams({
    pick_date: filters.pick_date,
    run_id: filters.run_id,
    strategy: filters.strategy,
  })
  if (filters.limit.trim() && filters.limit.trim() !== '100') {
    params.set('limit', filters.limit.trim())
  }
  return params
}

function paramValue(params: URLSearchParams, key: string) {
  return params.get(key)?.trim() ?? ''
}

function nonEmptyParams(values: Record<string, string>) {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(values)) {
    const trimmed = value.trim()
    if (trimmed) params.set(key, trimmed)
  }
  return params
}

function evidencePath(path: string, values: Record<string, string | null | undefined>) {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(values)) {
    const trimmed = value?.trim()
    if (trimmed) params.set(key, trimmed)
  }
  const query = params.toString()
  return query ? `${path}?${query}` : path
}

function recommendationStatusValue(value: string | null): RecommendationStatus {
  if (value === 'recommended' || value === 'reviewed') return value
  return 'all'
}

function archiveStatusValue(value: string | null): ArchiveStatus {
  if (value === 'recommended' || value === 'reviewed' || value === 'unreviewed') return value
  return 'all'
}

function compactPreselectRequest(form: Record<keyof PreselectRunRequest, string>): PreselectRunRequest {
  return Object.fromEntries(Object.entries(form).filter(([, value]) => value.trim()).map(([key, value]) => [key, value.trim()]))
}

function compactMarketDataRequest(form: Record<keyof MarketDataRunRequest, string>): MarketDataRunRequest {
  const request: MarketDataRunRequest = {}
  for (const key of ['config_path', 'start', 'end', 'out_dir', 'log_path'] as const) {
    const value = form[key].trim()
    if (value) request[key] = value
  }
  const workers = Number.parseInt(form.workers.trim(), 10)
  if (Number.isFinite(workers) && workers > 0) request.workers = workers
  return request
}

function summaryText(summary: Record<string, unknown>, key: string) {
  const value = summary[key]
  if (value === null || value === undefined || value === '') return 'Not set'
  if (Array.isArray(value)) return value.length ? value.join(', ') : 'none'
  return String(value)
}

function languageConfirmText(t: (text: string) => string, pickDate: string, candidateCount: number) {
  return `${t('Run Gemini review for')} ${pickDate} (${candidateCount} ${t('candidates')})?\n${t('This will call Gemini CLI and may consume quota. Continue?')}`
}

function isProductOwnedArtifactPath(path: string | null) {
  if (!path?.trim()) return false
  const normalized = path.replaceAll('\\', '/')
  return normalized !== 'data' && !normalized.startsWith('data/')
}

function jsonPreview(value: Record<string, unknown> | null) {
  if (!value || Object.keys(value).length === 0) return '{}'
  return JSON.stringify(value, null, 2)
}

function jsonInline(value: Record<string, unknown> | null) {
  if (!value || Object.keys(value).length === 0) return '{}'
  return `keys: ${Object.keys(value).join(', ')}`
}

function failureDiagnosticFromRun(
  run: RunSummary & { steps?: { error: Record<string, unknown> | null }[] },
): FailureDiagnostic | null {
  const candidates = [run.summary, ...(run.steps ?? []).map((step) => step.error)]
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== 'object') continue
    const diagnostic = candidate.diagnostic
    if (!diagnostic || typeof diagnostic !== 'object' || Array.isArray(diagnostic)) continue
    return normalizeFailureDiagnostic(diagnostic as Record<string, unknown>)
  }
  return null
}

function normalizeFailureDiagnostic(value: Record<string, unknown>): FailureDiagnostic {
  return {
    code: typeof value.code === 'string' ? value.code : undefined,
    title: typeof value.title === 'string' ? value.title : undefined,
    explanation: typeof value.explanation === 'string' ? value.explanation : undefined,
    next_actions: Array.isArray(value.next_actions)
      ? value.next_actions.filter((item): item is string => typeof item === 'string')
      : undefined,
    retryable: typeof value.retryable === 'boolean' ? value.retryable : undefined,
    docs: Array.isArray(value.docs) ? value.docs.filter((item): item is string => typeof item === 'string') : undefined,
  }
}

function errorText(error: unknown) {
  if (error instanceof Error) return error.message
  return 'Unable to reach the API'
}

export default App
