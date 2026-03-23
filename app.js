const STORAGE_KEY = 'inventory-hub-state-v1';

const demoProducts = [
  {
    id: crypto.randomUUID(),
    name: 'Футболка Oversize Graphite',
    sku: 'TSH-001',
    stock: 48,
    marketplaces: ['wb', 'ozon'],
    syncedAt: null,
  },
  {
    id: crypto.randomUUID(),
    name: 'Худи Basic Sand',
    sku: 'HDY-017',
    stock: 21,
    marketplaces: ['wb'],
    syncedAt: null,
  },
  {
    id: crypto.randomUUID(),
    name: 'Кроссовки Street White',
    sku: 'SNK-044',
    stock: 13,
    marketplaces: ['ozon'],
    syncedAt: null,
  },
];

const state = loadState();
const productsTableBody = document.getElementById('productsTableBody');
const storesList = document.getElementById('storesList');
const syncStatus = document.getElementById('syncStatus');
const productSearch = document.getElementById('productSearch');
const marketFilter = document.getElementById('marketFilter');
const marketplaceSelect = document.getElementById('marketplaceSelect');
const clientIdField = document.getElementById('clientIdField');
const productCheckboxes = Array.from(document.querySelectorAll('.product-market-checkbox'));

render();
attachEvents();

function loadState() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return {
      moysklad: null,
      tokens: [],
      products: [],
      lastSyncAt: null,
    };
  }

  try {
    const parsed = JSON.parse(raw);
    return {
      moysklad: parsed.moysklad ?? null,
      tokens: parsed.tokens ?? [],
      products: parsed.products ?? [],
      lastSyncAt: parsed.lastSyncAt ?? null,
    };
  } catch (error) {
    console.error('Не удалось загрузить состояние', error);
    return {
      moysklad: null,
      tokens: [],
      products: [],
      lastSyncAt: null,
    };
  }
}

function saveState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function attachEvents() {
  document.getElementById('openTokenModal').addEventListener('click', () => openModal('tokenModal'));
  document.getElementById('addProductButton').addEventListener('click', () => openModal('productModal'));
  document.getElementById('openMoySkladModal').addEventListener('click', () => openModal('moyskladModal'));
  document.getElementById('importDemoProducts').addEventListener('click', importDemoProducts);
  document.getElementById('syncAllButton').addEventListener('click', syncAllProducts);

  document.querySelectorAll('[data-close-modal]').forEach((button) => {
    button.addEventListener('click', () => closeModal(button.dataset.closeModal));
  });

  document.getElementById('tokenForm').addEventListener('submit', handleTokenSubmit);
  document.getElementById('productForm').addEventListener('submit', handleProductSubmit);
  document.getElementById('moyskladForm').addEventListener('submit', handleMoySkladSubmit);

  productSearch.addEventListener('input', renderProducts);
  marketFilter.addEventListener('change', renderProducts);

  marketplaceSelect.addEventListener('change', () => {
    const isOzon = marketplaceSelect.value === 'ozon';
    clientIdField.classList.toggle('hidden-field', !isOzon);
  });
}

function openModal(id) {
  document.getElementById(id).showModal();
}

function closeModal(id) {
  document.getElementById(id).close();
}

function handleTokenSubmit(event) {
  event.preventDefault();
  const marketplace = document.getElementById('marketplaceSelect').value;
  const name = document.getElementById('storeNameInput').value.trim();
  const token = document.getElementById('apiTokenInput').value.trim();
  const clientId = document.getElementById('clientIdInput').value.trim();

  state.tokens.unshift({
    id: crypto.randomUUID(),
    marketplace,
    name,
    token,
    clientId,
    createdAt: new Date().toISOString(),
  });

  saveState();
  event.target.reset();
  clientIdField.classList.add('hidden-field');
  closeModal('tokenModal');
  render();
}

function handleProductSubmit(event) {
  event.preventDefault();
  const name = document.getElementById('productNameInput').value.trim();
  const sku = document.getElementById('productSkuInput').value.trim();
  const stock = Number(document.getElementById('productStockInput').value);
  const marketplaces = productCheckboxes.filter((checkbox) => checkbox.checked).map((checkbox) => checkbox.value);

  state.products.unshift({
    id: crypto.randomUUID(),
    name,
    sku,
    stock,
    marketplaces,
    syncedAt: null,
  });

  saveState();
  event.target.reset();
  closeModal('productModal');
  render();
}

function handleMoySkladSubmit(event) {
  event.preventDefault();
  state.moysklad = {
    name: document.getElementById('moyskladNameInput').value.trim(),
    token: document.getElementById('moyskladTokenInput').value.trim(),
    secret: document.getElementById('moyskladSecretInput').value.trim(),
    connectedAt: new Date().toISOString(),
  };

  saveState();
  event.target.reset();
  closeModal('moyskladModal');
  renderHeaderStatus();
}

function importDemoProducts() {
  state.products = [...demoProducts.map((item) => ({ ...item, id: crypto.randomUUID() })), ...state.products];
  saveState();
  render();
}

function syncAllProducts() {
  const activeMarkets = new Set(state.tokens.map((token) => token.marketplace));
  const syncedAt = new Date().toISOString();

  state.products = state.products.map((product) => {
    const marketplaces = product.marketplaces.filter((market) => activeMarkets.has(market));
    return {
      ...product,
      syncedAt: marketplaces.length ? syncedAt : product.syncedAt,
    };
  });

  state.lastSyncAt = syncedAt;
  saveState();
  render();
}

function render() {
  renderHeaderStatus();
  renderStores();
  renderProducts();
  renderStats();
}

function renderHeaderStatus() {
  if (!state.moysklad) {
    syncStatus.textContent = 'Подключите МойСклад';
    return;
  }

  const lastSyncText = state.lastSyncAt ? `Последняя синхронизация: ${formatDate(state.lastSyncAt)}` : 'Готов к синхронизации';
  syncStatus.textContent = `${state.moysklad.name} · ${lastSyncText}`;
}

function renderStores() {
  const wbStores = state.tokens.filter((token) => token.marketplace === 'wb');
  const ozonStores = state.tokens.filter((token) => token.marketplace === 'ozon');

  document.getElementById('storesCount').textContent = String(state.tokens.length);
  document.getElementById('wbStoresCount').textContent = `${wbStores.length} магазинов`;
  document.getElementById('ozonStoresCount').textContent = `${ozonStores.length} магазинов`;

  storesList.innerHTML = '';
  if (!state.tokens.length) {
    storesList.append(document.getElementById('emptyStoresTemplate').content.cloneNode(true));
    return;
  }

  state.tokens.forEach((token) => {
    const card = document.createElement('article');
    card.className = 'store-card';
    card.innerHTML = `
      <div class="store-top">
        <div>
          <div class="badge-row">
            <span class="store-platform" data-market="${token.marketplace}">${labelForMarket(token.marketplace)}</span>
            <span class="sync-pill">${maskToken(token.token)}</span>
          </div>
          <h4>${token.name}</h4>
        </div>
        <button class="icon-btn" type="button" data-delete-token="${token.id}">Удалить</button>
      </div>
      <div class="store-meta">
        <span>Добавлен: ${formatDate(token.createdAt)}</span>
        ${token.clientId ? `<span>Client ID: ${token.clientId}</span>` : ''}
      </div>
    `;
    storesList.append(card);
  });

  storesList.querySelectorAll('[data-delete-token]').forEach((button) => {
    button.addEventListener('click', () => {
      state.tokens = state.tokens.filter((token) => token.id !== button.dataset.deleteToken);
      saveState();
      render();
    });
  });
}

function renderProducts() {
  const query = productSearch.value.trim().toLowerCase();
  const filter = marketFilter.value;

  const filtered = state.products.filter((product) => {
    const searchIndex = `${product.name} ${product.sku} ${product.marketplaces.join(' ')}`.toLowerCase();
    const matchesQuery = !query || searchIndex.includes(query);
    const matchesFilter =
      filter === 'all'
        ? true
        : filter === 'none'
          ? product.marketplaces.length === 0
          : product.marketplaces.includes(filter);
    return matchesQuery && matchesFilter;
  });

  productsTableBody.innerHTML = '';
  if (!filtered.length) {
    productsTableBody.append(document.getElementById('emptyProductsTemplate').content.cloneNode(true));
    return;
  }

  filtered.forEach((product) => {
    const row = document.createElement('tr');
    const activeStoreNames = getStoreNamesByMarket(product.marketplaces);
    row.innerHTML = `
      <td>
        <div class="product-name">
          <strong>${product.name}</strong>
          <span class="muted">${product.syncedAt ? `Синхронизирован: ${formatDate(product.syncedAt)}` : 'Ещё не синхронизирован'}</span>
        </div>
      </td>
      <td>${product.sku}</td>
      <td><span class="stock-pill">${product.stock} шт.</span></td>
      <td>
        <div class="market-badges">
          ${product.marketplaces.length
            ? product.marketplaces
                .map((market) => `
                  <span
                    class="market-icon"
                    data-market="${market}"
                    data-tooltip="${tooltipText(market, activeStoreNames[market])}"
                  >${shortLabelForMarket(market)}</span>
                `)
                .join('')
            : '<span class="muted">Нет привязки</span>'}
        </div>
      </td>
      <td>
        <div class="badge-row">
          <button class="secondary-btn" type="button" data-stock-plus="${product.id}">+1</button>
          <button class="icon-btn" type="button" data-delete-product="${product.id}">Удалить</button>
        </div>
      </td>
    `;
    productsTableBody.append(row);
  });

  productsTableBody.querySelectorAll('[data-stock-plus]').forEach((button) => {
    button.addEventListener('click', () => {
      state.products = state.products.map((product) =>
        product.id === button.dataset.stockPlus ? { ...product, stock: product.stock + 1 } : product,
      );
      saveState();
      renderProducts();
      renderStats();
    });
  });

  productsTableBody.querySelectorAll('[data-delete-product]').forEach((button) => {
    button.addEventListener('click', () => {
      state.products = state.products.filter((product) => product.id !== button.dataset.deleteProduct);
      saveState();
      render();
    });
  });
}

function renderStats() {
  document.getElementById('productsCount').textContent = String(state.products.length);
  document.getElementById('syncedProductsCount').textContent = String(
    state.products.filter((product) => product.marketplaces.length > 0).length,
  );
}

function getStoreNamesByMarket(markets) {
  return markets.reduce((acc, market) => {
    acc[market] = state.tokens.filter((token) => token.marketplace === market).map((token) => token.name);
    return acc;
  }, {});
}

function tooltipText(market, stores) {
  if (!stores || !stores.length) {
    return `${labelForMarket(market)}: токен не подключен`;
  }

  return `${labelForMarket(market)}: ${stores.join(', ')}`;
}

function labelForMarket(market) {
  return market === 'wb' ? 'Wildberries' : 'Ozon';
}

function shortLabelForMarket(market) {
  return market === 'wb' ? 'WB' : 'Ozon';
}

function maskToken(token) {
  if (token.length <= 8) return token;
  return `${token.slice(0, 4)}•••${token.slice(-4)}`;
}

function formatDate(value) {
  return new Intl.DateTimeFormat('ru-RU', {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(value));
}
