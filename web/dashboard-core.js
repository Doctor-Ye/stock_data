(function () {
  function createDashboard(config) {
    const state = {
      summary: null,
      filteredCompanies: [],
      selectedTicker: null,
      detailCache: new Map(),
      customFilters: Array.from({ length: 3 }, () => ({ metric: '', operator: 'gte', value: '' })),
    };
    const numberFmt = new Intl.NumberFormat(config.locale, { maximumFractionDigits: 2 });
    const integerFmt = new Intl.NumberFormat(config.locale, { maximumFractionDigits: 0 });
    const filterMetrics = [
      { key: 'marketCap', getValue: (item) => item.marketData?.marketCap },
      { key: 'revenue', getValue: (item) => item.latestAnnual?.revenue },
      { key: 'revenueGrowth', getValue: (item) => item.revenueGrowthPct },
      { key: 'psRatio', getValue: (item) => item.psRatio },
      { key: 'normalizedPe', getValue: (item) => item.normalizedPeProxy },
      { key: 'marketCapPayback', getValue: (item) => item.fiveYearMarketCapPaybackPct },
      { key: 'forecastRevenue', getValue: (item) => item.forecastRevenue },
      { key: 'forecastNetIncome', getValue: (item) => item.forecastNetIncome },
      { key: 'forwardPe', getValue: (item) => item.forwardPeRatio },
    ];
    const filterOperators = ['gt', 'gte', 'lt', 'lte', 'eq'];

    const fmtNumber = (value) => value == null ? '--' : numberFmt.format(value);
    const fmtInteger = (value) => value == null ? '--' : integerFmt.format(value);
    const fmtDate = (value) => value || '--';
    const fmtPrice = (value) => value == null ? '--' : new Intl.NumberFormat(config.locale, {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
    const fmtRatio = (value) => value == null ? '--' : numberFmt.format(value);
    const fmtPercent = (value) => value == null ? '--' : `${value > 0 ? '+' : ''}${numberFmt.format(value)}%`;
    const fmtCurrencyCompact = (value) => {
      if (value == null) return '--';
      const absolute = Math.abs(value);
      if (absolute >= 1_000_000_000_000) return `$${(value / 1_000_000_000_000).toFixed(2)}T`;
      if (absolute >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(2)}B`;
      if (absolute >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
      return fmtPrice(value);
    };

    async function fetchJson(path) {
      const response = await fetch(path, { cache: 'no-store' });
      if (!response.ok) throw new Error(`${config.text.loadFailed}: ${path}`);
      return response.json();
    }

    function statCard(label, value) {
      return `<article class="stat-card"><p class="stat-label">${label}</p><p class="stat-value">${value}</p></article>`;
    }

    function metricCard(label, value, footnote) {
      return `<article class="metric-card"><p class="metric-label">${label}</p><p class="metric-value">${value}</p><div class="metric-footnote">${footnote || '--'}</div></article>`;
    }

    function findSummaryEntry(ticker) {
      const normalized = (ticker || '').toUpperCase();
      return state.summary.companies.find((item) => item.ticker === normalized || (item.aliases || []).includes(normalized)) || null;
    }

    function renderHeroStats(summary) {
      const latestFilings = summary.companies.filter((item) => item.latestFiling && item.latestFiling.filing_date).length;
      const withMarketData = summary.companies.filter((item) => item.marketData && item.marketData.priceDate).length;
      const quoteDates = summary.companies.map((item) => item.marketData?.priceDate).filter(Boolean).sort();
      const latestQuoteDate = quoteDates.length ? quoteDates[quoteDates.length - 1] : '--';
      document.getElementById('hero-stats').innerHTML = [
        statCard(config.text.heroCompanies, fmtInteger(summary.companyCount)),
        statCard(config.text.heroFilings, fmtInteger(latestFilings)),
        statCard(config.text.heroMarketData, fmtInteger(withMarketData)),
        statCard(config.text.heroQuoteDate, latestQuoteDate),
      ].join('');
    }

    function renderOverview(summary) {
      const renderRows = (id, rows, cells) => {
        document.getElementById(id).innerHTML = rows.map((item) => `<tr data-ticker="${item.ticker}">${cells(item)}</tr>`).join('');
      };

      renderRows('top-revenue-body', summary.highlights.topRevenue || [], (item) => (
        `<td>${item.ticker}</td><td>${item.security || item.name}</td><td>${fmtCurrencyCompact(item.latestAnnual?.revenue)}</td>`
      ));
      renderRows('top-profit-body', summary.highlights.topProfit || [], (item) => (
        `<td>${item.ticker}</td><td>${item.security || item.name}</td><td>${fmtCurrencyCompact(item.latestAnnual?.net_income)}</td>`
      ));
      renderRows('top-ps-body', summary.highlights.topPs || [], (item) => (
        `<td>${item.ticker}</td><td>${item.security || item.name}</td><td>${fmtRatio(item.psRatio)}</td>`
      ));
      renderRows('top-normalized-growth-body', summary.highlights.topNormalizedGrowth || [], (item) => (
        `<td>${item.ticker}</td><td>${item.security || item.name}</td><td>${fmtPercent(item.normalizedNetIncomeGrowthPct)}</td>`
      ));
      renderRows('top-market-cap-payback-body', summary.highlights.topMarketCapPayback || [], (item) => (
        `<td>${item.ticker}</td><td>${item.security || item.name}</td><td>${fmtPercent(item.fiveYearMarketCapPaybackPct)}</td>`
      ));
      renderRows('latest-filings-body', summary.highlights.latestFilings || [], (item) => (
        `<td>${fmtDate(item.latestFiling?.filing_date)}</td><td>${item.ticker}</td><td>${item.latestFiling?.form || '--'}</td>`
      ));

      document.querySelectorAll('.overview-table tbody tr').forEach((row) => {
        row.addEventListener('click', () => selectCompany(row.dataset.ticker));
      });
    }

    function buildSectorOptions(summary) {
      document.getElementById('sector-select').innerHTML = [`<option value="">${config.text.allSectors}</option>`]
        .concat(summary.sectors.map((sector) => `<option value="${sector.sector}">${sector.sector}</option>`))
        .join('');
    }

    function renderCustomFilters() {
      document.getElementById('custom-filter-grid').innerHTML = state.customFilters.map((filter, index) => `
        <div class="filter-row">
          <select data-filter-field="metric" data-filter-index="${index}">
            <option value="">${config.text.filterMetricPlaceholder}</option>
            ${filterMetrics.map((metric) => `<option value="${metric.key}" ${filter.metric === metric.key ? 'selected' : ''}>${config.text.filterMetricLabel(metric.key)}</option>`).join('')}
          </select>
          <select data-filter-field="operator" data-filter-index="${index}">
            ${filterOperators.map((operator) => `<option value="${operator}" ${filter.operator === operator ? 'selected' : ''}>${config.text.filterOperatorLabel(operator)}</option>`).join('')}
          </select>
          <input
            class="filter-value"
            data-filter-field="value"
            data-filter-index="${index}"
            type="text"
            placeholder="${config.text.filterValuePlaceholder}"
            value="${filter.value}"
          >
        </div>
      `).join('');

      document.querySelectorAll('[data-filter-field="metric"]').forEach((element) => {
        element.addEventListener('change', handleCustomFilterChange);
      });
      document.querySelectorAll('[data-filter-field="operator"]').forEach((element) => {
        element.addEventListener('change', handleCustomFilterChange);
      });
      document.querySelectorAll('[data-filter-field="value"]').forEach((element) => {
        element.addEventListener('input', handleCustomFilterChange);
      });
    }

    function handleCustomFilterChange(event) {
      const index = Number(event.target.dataset.filterIndex);
      const field = event.target.dataset.filterField;
      state.customFilters[index][field] = event.target.value;
      applyFilters();
    }

    function parseFilterValue(rawValue) {
      if (rawValue == null) return NaN;
      const normalized = String(rawValue).trim().toUpperCase();
      if (!normalized) return NaN;
      const match = normalized.match(/^(-?\d+(?:\.\d+)?)([BM])?$/);
      if (!match) return Number(normalized);
      const value = Number(match[1]);
      const unit = match[2];
      if (unit === 'B') return value * 1_000_000_000;
      if (unit === 'M') return value * 1_000_000;
      return value;
    }

    function matchesCustomFilter(item, filter) {
      if (!filter.metric || filter.value === '') return true;
      const metric = filterMetrics.find((entry) => entry.key === filter.metric);
      if (!metric) return true;
      const itemValue = metric.getValue(item);
      const compareValue = parseFilterValue(filter.value);
      if (itemValue == null || Number.isNaN(compareValue)) return false;
      if (filter.operator === 'gt') return itemValue > compareValue;
      if (filter.operator === 'gte') return itemValue >= compareValue;
      if (filter.operator === 'lt') return itemValue < compareValue;
      if (filter.operator === 'lte') return itemValue <= compareValue;
      if (filter.operator === 'eq') return itemValue === compareValue;
      return true;
    }

    function sortCompanies(companies, sort) {
      companies.sort((a, b) => {
        if (sort === 'ticker-asc') return a.ticker.localeCompare(b.ticker);
        if (sort === 'filing-desc') return (b.latestFiling?.filing_date || '').localeCompare(a.latestFiling?.filing_date || '');
        if (sort === 'netIncome-desc') return (b.latestAnnual?.net_income || 0) - (a.latestAnnual?.net_income || 0);
        if (sort === 'marketCap-desc') return (b.marketData?.marketCap || 0) - (a.marketData?.marketCap || 0);
        if (sort === 'ps-asc') return (a.psRatio || Number.MAX_SAFE_INTEGER) - (b.psRatio || Number.MAX_SAFE_INTEGER);
        if (sort === 'operatingProfit-desc') return (b.operatingProfit || 0) - (a.operatingProfit || 0);
        if (sort === 'feeAdjustedNetIncome-desc') return (b.feeAdjustedNetIncome || 0) - (a.feeAdjustedNetIncome || 0);
        if (sort === 'normalizedGrowth-desc') return (b.normalizedNetIncomeGrowthPct || -Infinity) - (a.normalizedNetIncomeGrowthPct || -Infinity);
        if (sort === 'normalizedPe-asc') return (a.normalizedPeProxy || Number.MAX_SAFE_INTEGER) - (b.normalizedPeProxy || Number.MAX_SAFE_INTEGER);
        if (sort === 'marketCapPayback-desc') return (b.fiveYearMarketCapPaybackPct || -Infinity) - (a.fiveYearMarketCapPaybackPct || -Infinity);
        return (b.latestAnnual?.revenue || 0) - (a.latestAnnual?.revenue || 0);
      });
      return companies;
    }

    function applyFilters() {
      const query = document.getElementById('search-input').value.trim().toLowerCase();
      const sector = document.getElementById('sector-select').value;
      const sort = document.getElementById('sort-select').value;

      const companies = sortCompanies([...state.summary.companies].filter((item) => {
        const haystack = [item.ticker, ...(item.aliases || []), item.name, item.security, item.sector, item.subIndustry].join(' ').toLowerCase();
        return haystack.includes(query)
          && (!sector || item.sector === sector)
          && state.customFilters.every((filter) => matchesCustomFilter(item, filter));
      }), sort);

      state.filteredCompanies = companies;
      document.getElementById('list-meta').textContent = config.text.showing(companies.length, state.summary.companyCount);
      document.getElementById('company-list').innerHTML = companies.map((item) => `
        <article class="company-card ${state.selectedTicker === item.ticker ? 'active' : ''}" data-ticker="${item.ticker}">
          <div class="company-row"><h3 class="ticker">${item.ticker}</h3><strong>${fmtPrice(item.marketData?.price)}</strong></div>
          <p class="company-name">${item.security || item.name}</p>
          <div class="pill-row">
            <span class="pill">${item.sector || '--'}</span>
            <span class="pill">${config.text.listPs} ${fmtRatio(item.psRatio)}</span>
            <span class="pill">${config.text.listOpProfit} ${fmtCurrencyCompact(item.operatingProfit)}</span>
            <span class="pill">${config.text.listCoreGrowth} ${fmtPercent(item.normalizedNetIncomeGrowthPct)}</span>
            <span class="pill">${config.text.listNormalizedPe || config.text.normalizedPe} ${fmtRatio(item.normalizedPeProxy)}</span>
            <span class="pill">${config.text.listMarketCapPayback || config.text.marketCapPayback} ${fmtPercent(item.fiveYearMarketCapPaybackPct)}</span>
          </div>
        </article>
      `).join('');

      document.querySelectorAll('.company-card').forEach((element) => {
        element.addEventListener('click', () => selectCompany(element.dataset.ticker));
      });
    }

    function resetFilters() {
      document.getElementById('search-input').value = '';
      document.getElementById('sector-select').value = '';
      document.getElementById('sort-select').value = 'revenue-desc';
      state.customFilters = Array.from({ length: 3 }, () => ({ metric: '', operator: 'gte', value: '' }));
      renderCustomFilters();
      applyFilters();
    }

    async function selectCompany(ticker) {
      const summaryEntry = findSummaryEntry(ticker);
      const primaryTicker = summaryEntry?.ticker || (ticker || '').toUpperCase();
      state.selectedTicker = primaryTicker;
      location.hash = primaryTicker;
      applyFilters();
      document.getElementById('detail-empty').classList.add('hidden');
      document.getElementById('detail-content').classList.remove('hidden');
      if (!state.detailCache.has(primaryTicker)) {
        state.detailCache.set(primaryTicker, await fetchJson(`./data/companies/${primaryTicker}.json`));
      }
      renderDetail(state.detailCache.get(primaryTicker));
    }

    function renderDetail(detail) {
      const company = detail.company;
      const latestAnnual = company.latestAnnual || {};
      const latestQuarter = company.latestQuarter || {};
      const latestFiling = company.latestFiling || {};
      const marketData = company.marketData || {};
      const analysis = company.analysis || {};
      const aliasLine = company.aliases && company.aliases.length > 1 ? `<p>${config.text.aliases}: ${company.aliases.join(', ')}</p>` : '';

      document.getElementById('company-head').innerHTML = `
        <p class="eyebrow">${company.ticker} / CIK ${company.cik}</p>
        <h2>${company.security || company.name}</h2>
        <p>${company.name}</p>
        ${aliasLine}
        <p>${company.sector || '--'} / ${company.subIndustry || '--'} / ${company.headquarters || '--'}</p>
      `;

      document.getElementById('metrics-grid').innerHTML = [
        metricCard(config.text.previousClose, fmtPrice(marketData.price), config.text.asOf(marketData.priceDate)),
        metricCard(config.text.marketCap, fmtCurrencyCompact(marketData.marketCap), config.text.balanceSheetBasis(marketData.equitySourceForm, marketData.equitySourceFiledDate)),
        metricCard(config.text.psRatio, fmtRatio(company.psRatio), config.text.fiscalYear(latestAnnual.fiscal_year)),
        metricCard(config.text.operatingProfit, fmtCurrencyCompact(company.operatingProfit), config.text.operatingMargin(analysis.latestOperatingMarginPct)),
        metricCard(config.text.feeAdjustedNetIncome, fmtCurrencyCompact(company.feeAdjustedNetIncome), config.text.proxyMetric),
        metricCard(config.text.revenueGrowth, fmtPercent(company.revenueGrowthPct), config.text.fiscalYear(latestAnnual.fiscal_year)),
        metricCard(config.text.revenueGrowthGeomean, fmtPercent(company.revenueGrowthGeomeanPct), config.text.marketCapPaybackProjectionFootnote),
        metricCard(config.text.normalizedGrowth, fmtPercent(company.normalizedNetIncomeGrowthPct), config.text.proxyMetric),
        metricCard(config.text.normalizedGrowthGeomean, fmtPercent(company.normalizedNetIncomeGrowthGeomeanPct), config.text.proxyMetric),
        metricCard(config.text.normalizedProjectionGrowth, fmtPercent(company.normalizedNetIncomeProjectionGrowthPct), config.text.marketCapPaybackProjectionFootnote),
        metricCard(config.text.normalizedPe, fmtRatio(company.normalizedPeProxy), config.text.proxyMetric),
        metricCard(config.text.projectedFiveYearNormalizedNetIncome, fmtCurrencyCompact(company.projectedFiveYearNormalizedNetIncome), config.text.proxyMetric),
        metricCard(config.text.marketCapPayback, fmtPercent(company.fiveYearMarketCapPaybackPct), config.text.marketCapPaybackFootnote),
        metricCard(config.text.forecastRevenue, fmtCurrencyCompact(company.forecastRevenue), config.text.forecastBasis(company.forecastRevenueFiscalYear, company.forecastRevenueSourceType, company.forecastRevenueSourceName)),
        metricCard(config.text.forecastNetIncome, fmtCurrencyCompact(company.forecastNetIncome), config.text.forecastBasis(company.forecastNetIncomeFiscalYear, company.forecastSourceType, company.forecastSourceName)),
        metricCard(config.text.forwardPeRatio, fmtRatio(company.forwardPeRatio), config.text.forecastBasis(company.forecastNetIncomeFiscalYear, company.forecastSourceType, company.forecastSourceName)),
        metricCard(config.text.peRatio, fmtRatio(marketData.peRatio), marketData.peRatio == null ? config.text.peUnavailable : config.text.earningsBasis(latestAnnual.fiscal_year)),
        metricCard(config.text.pbRatio, fmtRatio(marketData.pbRatio), marketData.pbRatio == null ? config.text.pbUnavailable : config.text.balanceSheetBasis(marketData.equitySourceForm, marketData.equitySourceFiledDate)),
        metricCard(config.text.annualRevenue, fmtCurrencyCompact(latestAnnual.revenue), config.text.fiscalYear(latestAnnual.fiscal_year)),
        metricCard(config.text.annualNetIncome, fmtCurrencyCompact(latestAnnual.net_income), config.text.fiscalYear(latestAnnual.fiscal_year)),
        metricCard(config.text.latestFiling, latestFiling.form || '--', fmtDate(latestFiling.filing_date)),
      ].join('');

      drawSeriesChart('annual-revenue-chart', detail.annuals.map((row) => ({ label: String(row.fiscal_year), value: row.revenue })), config.text.annualRevenue);
      drawSeriesChart('quarterly-revenue-chart', detail.quarterlies.slice(-12).map((row) => ({ label: `${row.fiscal_year}-${row.fiscal_period}`, value: row.revenue })), config.text.quarterlyRevenue);
      drawSeriesChart('annual-core-profit-chart', (analysis.years || []).map((row) => ({ label: String(row.fiscal_year), value: row.normalizedNetIncomeProxy })), config.text.normalizedNetIncome);

      document.getElementById('interpretation-list').innerHTML = (analysis.commentary || []).map((line) => `<li>${line}</li>`).join('');
      document.getElementById('methodology-list').innerHTML = (analysis.methodology || []).map((line) => `<li>${line}</li>`).join('');

      document.getElementById('annual-table-body').innerHTML = detail.annuals.slice().reverse().map((row) => (
        `<tr><td>${row.fiscal_year}</td><td>${fmtCurrencyCompact(row.revenue)}</td><td>${fmtCurrencyCompact(row.operating_income)}</td><td>${fmtCurrencyCompact(row.net_income)}</td><td>${fmtCurrencyCompact(row.share_based_compensation_expense)}</td><td>${fmtCurrencyCompact(row.special_items)}</td></tr>`
      )).join('');
      document.getElementById('analysis-table-body').innerHTML = (analysis.years || []).slice().reverse().map((row) => (
        `<tr><td>${row.fiscal_year}</td><td>${fmtRatio(row.psRatio)}</td><td>${fmtCurrencyCompact(row.operating_income)}</td><td>${fmtCurrencyCompact(row.feeAdjustedNetIncome)}</td><td>${fmtCurrencyCompact(row.normalizedNetIncomeProxy)}</td><td>${fmtPercent(row.operatingMarginPct)}</td></tr>`
      )).join('');
      document.getElementById('filings-table-body').innerHTML = detail.filings.slice(0, 15).map((row) => (
        `<tr><td>${fmtDate(row.filing_date)}</td><td>${row.form || '--'}</td><td>${row.accession_number}</td></tr>`
      )).join('');
    }

    function drawSeriesChart(containerId, points, title) {
      const container = document.getElementById(containerId);
      const valid = points.filter((point) => typeof point.value === 'number');
      if (!valid.length) {
        container.innerHTML = `<p class="chart-note">${config.text.noData(title)}</p>`;
        return;
      }

      const width = 760;
      const height = 260;
      const padding = 24;
      const min = Math.min(...valid.map((point) => point.value));
      const max = Math.max(...valid.map((point) => point.value));
      const spread = max - min || 1;
      const stepX = (width - padding * 2) / Math.max(valid.length - 1, 1);
      const polyline = valid.map((point, index) => {
        const x = padding + stepX * index;
        const y = height - padding - ((point.value - min) / spread) * (height - padding * 2);
        return `${x},${y}`;
      }).join(' ');
      const area = [`${padding},${height - padding}`, polyline, `${padding + stepX * (valid.length - 1)},${height - padding}`].join(' ');
      const markers = valid.map((point, index) => {
        const x = padding + stepX * index;
        const y = height - padding - ((point.value - min) / spread) * (height - padding * 2);
        return `<circle cx="${x}" cy="${y}" r="4" fill="${config.chart.marker}"></circle>`;
      }).join('');
      const labels = valid.map((point, index) => {
        const x = padding + stepX * index;
        return `<text x="${x}" y="${height - 6}" text-anchor="middle" font-size="11" fill="#6b7280">${point.label}</text>`;
      }).join('');

      container.innerHTML = `
        <div class="chart">
          <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
            <polyline fill="${config.chart.fill}" stroke="none" points="${area}"></polyline>
            <polyline fill="none" stroke="${config.chart.line}" stroke-width="3" points="${polyline}"></polyline>
            ${markers}
            ${labels}
          </svg>
        </div>
        <div class="chart-note">${config.text.range(fmtCurrencyCompact(min), fmtCurrencyCompact(max))}</div>
      `;
    }

    async function init() {
      state.summary = await fetchJson('./data/summary.json');
      renderHeroStats(state.summary);
      renderOverview(state.summary);
      buildSectorOptions(state.summary);
      renderCustomFilters();
      document.getElementById('search-input').addEventListener('input', applyFilters);
      document.getElementById('sector-select').addEventListener('change', applyFilters);
      document.getElementById('sort-select').addEventListener('change', applyFilters);
      document.getElementById('reset-filters').addEventListener('click', resetFilters);
      applyFilters();

      const requestedTicker = location.hash.replace('#', '').toUpperCase();
      const initialEntry = requestedTicker ? findSummaryEntry(requestedTicker) : null;
      const defaultTicker = state.filteredCompanies[0]?.ticker;
      if (initialEntry) {
        await selectCompany(initialEntry.ticker);
      } else if (defaultTicker) {
        await selectCompany(defaultTicker);
      }
    }

    init().catch((error) => {
      document.body.innerHTML = `<pre style="padding:24px;color:#8b0000;">${error.stack}</pre>`;
    });
  }

  window.createDashboard = createDashboard;
})();
