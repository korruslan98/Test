const state = {
  moysklad: null,
  stores: [],
  products: [],
  lastImportAt: null,
  lastSyncAt: null,
};

const dom = {
  healthStatus: document.getElementById('healthStatus'),
  lastImport: document.getElementById('lastImport'),
  lastSync: document.getElementById('lastSync'),
  productsCount: document.getElementById('productsCount'),
  storesCount: document.getElementById('storesCount'),
  syncedProductsCount: document.getElementById('syncedProductsCount'),
  wbStoresCount: document.getElementById('wbStoresCount'),
  ozonStoresCount: document.getElementById('ozonStoresCount'),
  storesList: document.getElementById('storesList'),
  productsTableBody: document.getElementById('productsTableBody'),
  messages: document.getElementById('messages'),
  productSearch: document.getElementById('productSearch'),
  marketFilter: document.getElementById('marketFilter'),
  productForm: document.getElementById('productForm'),
  storeForm: document.getElementById('storeForm'),
  moyskladForm: document.getElementById('moyskladForm'),
  storeMarketplaceInput: document.getElementById('storeMarketplaceInput'),
  ozonFields: document.getElementById('ozonFields'),
  wbFields: document.getElementById('wbFields'),
  moyskladAuthModeInput: document.getElementById('moyskladAuthModeInput'),
  moyskladSecretField: document.getElementById('moyskladSecretField'),
  moyskladCredentialLabel: document.getElementById('moyskladCredentialLabel'),
  wbEnabledInput: document.getElementById('wbEnabledInput'),
  ozonEnabledInput: document.getElementById('ozonEnabledInput'),
  productModalTitle: document.getElementById('productModalTitle'),
};

bootstrap();

async function bootstrap() {
  bindEvents();
  await Promise.all([checkHealth(), loadState()]);
}

function bindEvents() {
  document.getElementById('openMoySkladModal').addEventListener('click', () => openModal('moyskladModal'));
  document.getElementById('openStoreModal').addEventListener('click', () => openModal('storeModal'));
  document.getElementById('addProductButton').addEventListener('click', () => openProductModal());
  document.getElementById('importProductsButton').addEventListener('click', importProducts);
  document.getElementById('syncAllButton').addEventListener('click', syncProducts);

  document.querySelectorAll('[data-close-modal]').forEach((button) => {
    button.addEventListener('click', () => closeModal(button.dataset.closeModal));
  });

  document.querySelectorAll('[data-tab]').forEach((button) => {
    button.addEventListener('click', () => switchTab(button.dataset.tab));
  });

  dom.storeMarketplaceInput.addEventListener('change', toggleStoreFields);
  dom.moyskladAuthModeInput.addEventListener('change', toggleMoySkladSecret);
  dom.productSearch.addEventListener('input', renderProducts);
  dom.marketFilter.addEventListener('change', renderProducts);

  dom.storeForm.addEventListener('submit', handleStoreSubmit);
  dom.productForm.addEventListener('submit', handleProductSubmit);
  dom.moyskladForm.addEventListener('submit', handleMoySkladSubmit);
}

async function checkHealth() {
  try {
    const response = await fetch('/api/health');
    const data = await response.json();
    dom.healthStatus.textContent = data.ok ? 'Сервер работает' : 'Ошибка healthcheck';
  } catch {
    dom.healthStatus.textContent = 'Сервер недоступен';
  }
}

async function loadState() {
  const data = await api('/api/state');
  applyState(data);
  render();
}

function applyState(data) {
  state.moysklad = data.moysklad;
  state.stores = data.stores || [];
  state.products = data.products || [];
  state.lastImportAt = data.lastImportAt || null;
  state.lastSyncAt = data.lastSyncAt || null;
}

function render() {
  renderStatus();
  renderStats();
  renderStores();
  renderProducts();
}

function renderStatus() {
  dom.lastImport.textContent = state.lastImportAt ? formatDate(state.lastImportAt) : '—';
  dom.lastSync.textContent = state.lastSyncAt ? formatDate(state.lastSyncAt) : '—';
}

function renderStats() {
  const wbCount = state.stores.filter((store) => store.marketplace === 'wb').length;
  const ozonCount = state.stores.filter((store) => store.marketplace === 'ozon').length;
  const syncedCount = state.products.filter((product) => hasAnyMarketplace(product)).length;

  dom.productsCount.textContent = String(state.products.length);
  dom.storesCount.textContent = String(state.stores.length);
  dom.syncedProductsCount.textContent = String(syncedCount);
  dom.wbStoresCount.textContent = `${wbCount} магазинов`;
  dom.ozonStoresCount.textContent = `${ozonCount} магазинов`;
}

function renderStores() {
  dom.storesList.innerHTML = '';
  if (!state.stores.length) {
    dom.storesList.append(document.getElementById('emptyStoresTemplate').content.cloneNode(true));
    return;
  }

  state.stores.forEach((store) => {
    const card = document.createElement('article');
    card.className = 'store-card';
    card.innerHTML = `
      <div class="store-head">
        <div>
          <div class="badge-row">
            <span class="chip" data-market="${store.marketplace}">${labelForMarket(store.marketplace)}</span>
            <span class="sync-pill">${store.tokenMasked}</span>
          </div>
          <h4>${escapeHtml(store.name)}</h4>
        </div>
        <button type="button" class="icon-btn" data-delete-store="${store.id}">Удалить</button>
      </div>
      <div class="store-meta">
        <span>Склад: ${escapeHtml(String(store.warehouseId))}</span>
        ${store.clientId ? `<span>Client ID: ${escapeHtml(String(store.clientId))}</span>` : ''}
        <span>Добавлен: ${formatDate(store.createdAt)}</span>
      </div>
    `;
    dom.storesList.append(card);
  });

  dom.storesList.querySelectorAll('[data-delete-store]').forEach((button) => {
    button.addEventListener('click', () => removeStore(button.dataset.deleteStore));
  });
}

function renderProducts() {
  const query = dom.productSearch.value.trim().toLowerCase();
  const filter = dom.marketFilter.value;

  const visible = state.products.filter((product) => {
    const tokens = [
      product.name,
      product.sku,
      product.marketplaces?.wb?.sku,
      product.marketplaces?.ozon?.offerId,
      ...linkedStoreNames(product),
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();

    const matchesQuery = !query || tokens.includes(query);
    const matchesFilter =
      filter === 'all'
        ? true
        : filter === 'none'
          ? !hasAnyMarketplace(product)
          : Boolean(product.marketplaces?.[filter]?.enabled);
    return matchesQuery && matchesFilter;
  });

  dom.productsTableBody.innerHTML = '';
  if (!visible.length) {
    dom.productsTableBody.append(document.getElementById('emptyProductsTemplate').content.cloneNode(true));
    return;
  }

  visible.forEach((product) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>
        <div class="product-cell">
          <strong>${escapeHtml(product.name)}</strong>
          <span class="muted">Источник: ${product.source === 'moysklad' ? 'МойСклад' : 'Ручной товар'}</span>
        </div>
      </td>
      <td>${escapeHtml(product.sku)}</td>
      <td><span class="stock-pill">${product.stock} шт.</span></td>
      <td>
        <div class="market-badges">${renderMarketplaceBadges(product)}</div>
      </td>
      <td>
        <div class="status-badges">${renderSyncStatuses(product)}</div>
      </td>
      <td>
        <div class="badge-row">
          <button type="button" class="secondary-btn" data-edit-product="${product.id}">Редактировать</button>
          <button type="button" class="icon-btn" data-delete-product="${product.id}">Удалить</button>
        </div>
      </td>
    `;
    dom.productsTableBody.append(row);
  });

  dom.productsTableBody.querySelectorAll('[data-edit-product]').forEach((button) => {
    button.addEventListener('click', () => {
      const product = state.products.find((item) => item.id === button.dataset.editProduct);
      openProductModal(product);
    });
  });

  dom.productsTableBody.querySelectorAll('[data-delete-product]').forEach((button) => {
    button.addEventListener('click', () => removeProduct(button.dataset.deleteProduct));
  });
}

function renderMarketplaceBadges(product) {
  const badges = [];
  if (product.marketplaces?.wb?.enabled) {
    badges.push(
      `<span class="market-icon" data-market="wb" data-tooltip="${escapeHtml(tooltipForMarket(product, 'wb'))}">WB</span>`,
    );
  }
  if (product.marketplaces?.ozon?.enabled) {
    badges.push(
      `<span class="market-icon" data-market="ozon" data-tooltip="${escapeHtml(tooltipForMarket(product, 'ozon'))}">Ozon</span>`,
    );
  }
  return badges.length ? badges.join('') : '<span class="muted">Нет привязки</span>';
}

function renderSyncStatuses(product) {
  const statuses = Object.entries(product.lastSync || {});
  if (!statuses.length) {
    return '<span class="muted">Ещё не синхронизирован</span>';
  }

  return statuses
    .map(([storeId, item]) => {
      const store = state.stores.find((entry) => entry.id === storeId);
      const storeName = store?.name || 'Удалённый магазин';
      return `<span class="status-pill ${item.status}" title="${escapeHtml(item.message || '')}">${escapeHtml(storeName)}</span>`;
    })
    .join('');
}

function tooltipForMarket(product, market) {
  const stores = state.stores.filter((store) => store.marketplace === market).map((store) => store.name);
  const mapping = product.marketplaces?.[market] || {};
  if (!stores.length) {
    return `${labelForMarket(market)}: нет подключённых магазинов`;
  }

  if (market === 'wb') {
    const identifier = mapping.chrtId ? `chrtId ${mapping.chrtId}` : `SKU ${mapping.sku}`;
    return `${labelForMarket(market)} → ${stores.join(', ')}; идентификатор: ${identifier}`;
  }

  const identifier = mapping.offerId ? `offer_id ${mapping.offerId}` : `product_id ${mapping.productId}`;
  return `${labelForMarket(market)} → ${stores.join(', ')}; идентификатор: ${identifier}`;
}

function linkedStoreNames(product) {
  return ['wb', 'ozon']
    .filter((market) => product.marketplaces?.[market]?.enabled)
    .flatMap((market) => state.stores.filter((store) => store.marketplace === market).map((store) => store.name));
}

function hasAnyMarketplace(product) {
  return Boolean(product.marketplaces?.wb?.enabled || product.marketplaces?.ozon?.enabled);
}

function openModal(id) {
  document.getElementById(id).showModal();
}

function closeModal(id) {
  document.getElementById(id).close();
}

function switchTab(tabName) {
  document.querySelectorAll('[data-tab]').forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === tabName);
  });
  document.querySelectorAll('[data-panel]').forEach((panel) => {
    panel.classList.toggle('active', panel.dataset.panel === tabName);
  });
}

function toggleStoreFields() {
  const isOzon = dom.storeMarketplaceInput.value === 'ozon';
  dom.ozonFields.classList.toggle('hidden', !isOzon);
  dom.wbFields.classList.toggle('hidden', isOzon);
}

function toggleMoySkladSecret() {
  const isBasic = dom.moyskladAuthModeInput.value === 'basic';
  dom.moyskladSecretField.classList.toggle('hidden', !isBasic);
  dom.moyskladCredentialLabel.textContent = isBasic ? 'Логин' : 'Токен';
}

function openProductModal(product = null) {
  dom.productForm.reset();
  document.getElementById('productIdInput').value = product?.id || '';
  dom.productModalTitle.textContent = product ? 'Редактировать товар' : 'Добавить товар';

  if (product) {
    document.getElementById('productNameInput').value = product.name;
    document.getElementById('productSkuInput').value = product.sku;
    document.getElementById('productStockInput').value = product.stock;
    document.getElementById('wbEnabledInput').checked = Boolean(product.marketplaces?.wb?.enabled);
    document.getElementById('wbSkuInput').value = product.marketplaces?.wb?.sku || '';
    document.getElementById('wbChrtIdInput').value = product.marketplaces?.wb?.chrtId || '';
    document.getElementById('ozonEnabledInput').checked = Boolean(product.marketplaces?.ozon?.enabled);
    document.getElementById('ozonOfferIdInput').value = product.marketplaces?.ozon?.offerId || '';
    document.getElementById('ozonProductIdInput').value = product.marketplaces?.ozon?.productId || '';
    document.getElementById('ozonWarehouseIdInput').value = product.marketplaces?.ozon?.warehouseId || '';
  } else {
    document.getElementById('wbEnabledInput').checked = state.stores.some((store) => store.marketplace === 'wb');
    document.getElementById('ozonEnabledInput').checked = state.stores.some((store) => store.marketplace === 'ozon');
  }

  openModal('productModal');
}

async function handleMoySkladSubmit(event) {
  event.preventDefault();
  const payload = {
    name: document.getElementById('moyskladNameInput').value,
    baseUrl: document.getElementById('moyskladBaseUrlInput').value,
    authMode: document.getElementById('moyskladAuthModeInput').value,
    credential: document.getElementById('moyskladCredentialInput').value,
    secret: document.getElementById('moyskladSecretInput').value,
  };

  const data = await api('/api/moysklad/connect', {
    method: 'POST',
    body: JSON.stringify(payload),
  });

  showMessage('success', data.message);
  closeModal('moyskladModal');
  dom.moyskladForm.reset();
  toggleMoySkladSecret();
  await loadState();
}

async function handleStoreSubmit(event) {
  event.preventDefault();
  const marketplace = document.getElementById('storeMarketplaceInput').value;
  const payload = {
    marketplace,
    name: document.getElementById('storeNameInput').value,
    warehouseId: document.getElementById('storeWarehouseInput').value,
    token: document.getElementById('wbTokenInput').value,
    clientId: document.getElementById('ozonClientIdInput').value,
    apiKey: document.getElementById('ozonApiKeyInput').value,
  };

  const data = await api('/api/stores', {
    method: 'POST',
    body: JSON.stringify(payload),
  });

  showMessage('success', data.message);
  closeModal('storeModal');
  dom.storeForm.reset();
  toggleStoreFields();
  applyState(data.state);
  render();
}

async function handleProductSubmit(event) {
  event.preventDefault();
  const id = document.getElementById('productIdInput').value;
  const payload = {
    name: document.getElementById('productNameInput').value,
    sku: document.getElementById('productSkuInput').value,
    stock: document.getElementById('productStockInput').value,
    marketplaces: {
      wb: {
        enabled: document.getElementById('wbEnabledInput').checked,
        sku: document.getElementById('wbSkuInput').value,
        chrtId: document.getElementById('wbChrtIdInput').value,
      },
      ozon: {
        enabled: document.getElementById('ozonEnabledInput').checked,
        offerId: document.getElementById('ozonOfferIdInput').value,
        productId: document.getElementById('ozonProductIdInput').value,
        warehouseId: document.getElementById('ozonWarehouseIdInput').value,
      },
    },
  };

  const url = id ? `/api/products/${id}` : '/api/products';
  const method = id ? 'PUT' : 'POST';
  const data = await api(url, { method, body: JSON.stringify(payload) });

  showMessage('success', data.message);
  closeModal('productModal');
  applyState(data.state);
  render();
}

async function importProducts() {
  const data = await api('/api/moysklad/import', { method: 'POST' });
  showMessage('success', data.message);
  applyState(data.state);
  render();
}

async function syncProducts() {
  const data = await api('/api/sync', { method: 'POST' });
  const summaryText = data.summary
    .map((item) => `${item.name}: ${item.synced} ok, ${item.failed} ошибок, ${item.skipped} пропущено`)
    .join(' · ');

  showMessage('success', `${data.message} ${summaryText}`);
  applyState(data.state);
  render();
}

async function removeStore(id) {
  const data = await api(`/api/stores/${id}`, { method: 'DELETE' });
  showMessage('success', data.message);
  applyState(data.state);
  render();
}

async function removeProduct(id) {
  const data = await api(`/api/products/${id}`, { method: 'DELETE' });
  showMessage('success', data.message);
  applyState(data.state);
  render();
}

function showMessage(type, text) {
  const message = document.createElement('article');
  message.className = `message ${type}`;
  message.innerHTML = `
    <div>
      <strong>${type === 'success' ? 'Готово' : 'Ошибка'}</strong>
      <p>${escapeHtml(text)}</p>
    </div>
    <button type="button" class="icon-btn">Закрыть</button>
  `;
  message.querySelector('button').addEventListener('click', () => message.remove());
  dom.messages.prepend(message);
}

async function api(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    showMessage('error', data.message || 'Неизвестная ошибка');
    throw new Error(data.message || 'Request failed');
  }

  return data;
}

function formatDate(value) {
  return new Intl.DateTimeFormat('ru-RU', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(value));
}

function labelForMarket(market) {
  return market === 'wb' ? 'Wildberries' : 'Ozon';
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
