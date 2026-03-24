const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { URL } = require('node:url');

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'app-state.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const MOYSKLAD_BASE_URL = 'https://api.moysklad.ru/api/remap/1.2';
const WB_API_URL = 'https://marketplace-api.wildberries.ru';
const WB_CONTENT_API_URL = 'https://content-api.wildberries.ru';
const OZON_API_URL = 'https://api-seller.ozon.ru';
const CHUNK_SIZE = 1000;
const AUTO_SYNC_INTERVAL_MS = 15 * 60 * 1000;
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
};
let syncInProgress = false;

const server = http.createServer(async (req, res) => {
  try {
    const requestUrl = new URL(req.url, `http://${req.headers.host}`);

    if (requestUrl.pathname.startsWith('/api/')) {
      await handleApi(req, res, requestUrl);
      return;
    }

    await serveStatic(req, res, requestUrl);
  } catch (error) {
    sendJson(res, error.statusCode || 500, {
      message: error.message || 'Внутренняя ошибка сервера.',
    });
  }
});

server.listen(PORT, async () => {
  await ensureStateFile();
  console.log(`Inventory Sync Hub running on http://localhost:${PORT}`);
  startAutoSync();
});

async function handleApi(req, res, requestUrl) {
  if (req.method === 'GET' && requestUrl.pathname === '/api/health') {
    sendJson(res, 200, { ok: true, timestamp: new Date().toISOString() });
    return;
  }

  if (req.method === 'GET' && requestUrl.pathname === '/api/state') {
    const state = await readState();
    sendJson(res, 200, sanitizeState(state));
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/moysklad/connect') {
    const payload = normalizeMoySkladPayload(await readJsonBody(req));
    await verifyMoySkladConnection(payload);
    const state = await readState();
    state.moysklad = { ...payload, connectedAt: new Date().toISOString() };
    await writeState(state);
    sendJson(res, 200, {
      message: 'Подключение к МойСклад сохранено и проверено.',
      moysklad: sanitizeState(state).moysklad,
    });
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/moysklad/import') {
    const state = await readState();
    if (!state.moysklad) {
      throw createHttpError(400, 'Сначала подключите МойСклад.');
    }

    const importedProducts = await fetchMoySkladProducts(state.moysklad);
    const existingByKey = new Map(state.products.map((product) => [product.sourceId || product.sku, product]));

    state.products = importedProducts.map((product) => {
      const existing = existingByKey.get(product.sourceId || product.sku);
      if (!existing) {
        return createStoredProduct(product);
      }

      return {
        ...existing,
        name: product.name,
        sku: product.sku,
        stock: product.stock,
        marketplaces: mergeImportedMarketplaces(existing.marketplaces, product.sku),
        source: 'moysklad',
        sourceId: product.sourceId,
        updatedAt: new Date().toISOString(),
      };
    });

    state.lastImportAt = new Date().toISOString();
    await writeState(state);
    sendJson(res, 200, {
      message: `Импортировано ${state.products.length} товаров из МойСклад.`,
      count: state.products.length,
      state: sanitizeState(state),
    });
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/stores') {
    const store = normalizeStorePayload(await readJsonBody(req));
    const state = await readState();
    state.stores.unshift({ ...store, id: crypto.randomUUID(), createdAt: new Date().toISOString() });
    await writeState(state);
    sendJson(res, 201, { message: 'Магазин сохранён.', state: sanitizeState(state) });
    return;
  }

  if (req.method === 'DELETE' && requestUrl.pathname.startsWith('/api/stores/')) {
    const storeId = requestUrl.pathname.split('/').pop();
    const state = await readState();
    state.stores = state.stores.filter((store) => store.id !== storeId);
    state.products = state.products.map((product) => ({
      ...product,
      lastSync: Object.fromEntries(Object.entries(product.lastSync || {}).filter(([id]) => id !== storeId)),
    }));
    await writeState(state);
    sendJson(res, 200, { message: 'Магазин удалён.', state: sanitizeState(state) });
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/products') {
    const payload = normalizeProductPayload(await readJsonBody(req));
    const state = await readState();
    state.products.unshift(createStoredProduct(payload, 'manual'));
    await writeState(state);
    sendJson(res, 201, { message: 'Товар сохранён.', state: sanitizeState(state) });
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/products/bulk-marketplace') {
    const payload = normalizeBulkMarketplacePayload(await readJsonBody(req));
    const state = await readState();

    state.products = state.products.map((product) => ({
      ...product,
      marketplaces: applyBulkMarketplaceFlag(product, payload.marketplace, payload.enabled),
      updatedAt: new Date().toISOString(),
    }));

    await writeState(state);
    sendJson(res, 200, {
      message: payload.enabled
        ? `Подключение ${labelForMarketplace(payload.marketplace)} включено для всех товаров.`
        : `Подключение ${labelForMarketplace(payload.marketplace)} выключено для всех товаров.`,
      state: sanitizeState(state),
    });
    return;
  }

  if (req.method === 'PUT' && requestUrl.pathname.startsWith('/api/products/')) {
    const productId = requestUrl.pathname.split('/').pop();
    const payload = normalizeProductPayload(await readJsonBody(req));
    const state = await readState();
    const index = state.products.findIndex((product) => product.id === productId);
    if (index === -1) {
      throw createHttpError(404, 'Товар не найден.');
    }

    const current = state.products[index];
    state.products[index] = {
      ...current,
      ...payload,
      source: current.source,
      sourceId: current.sourceId,
      updatedAt: new Date().toISOString(),
    };
    await writeState(state);
    sendJson(res, 200, { message: 'Товар обновлён.', state: sanitizeState(state) });
    return;
  }

  if (req.method === 'DELETE' && requestUrl.pathname.startsWith('/api/products/')) {
    const productId = requestUrl.pathname.split('/').pop();
    const state = await readState();
    state.products = state.products.filter((product) => product.id !== productId);
    await writeState(state);
    sendJson(res, 200, { message: 'Товар удалён.', state: sanitizeState(state) });
    return;
  }

  if (req.method === 'POST' && requestUrl.pathname === '/api/sync') {
    const result = await runSyncJob('manual');
    sendJson(res, 200, {
      message: 'Синхронизация завершена.',
      summary: result.summary,
      state: sanitizeState(result.state),
    });
    return;
  }

  throw createHttpError(404, 'Маршрут не найден.');
}

async function serveStatic(_req, res, requestUrl) {
  let filePath = requestUrl.pathname === '/' ? path.join(PUBLIC_DIR, 'index.html') : path.join(PUBLIC_DIR, requestUrl.pathname);
  filePath = path.normalize(filePath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    throw createHttpError(403, 'Доступ запрещён.');
  }

  try {
    const stats = await fs.stat(filePath);
    if (stats.isDirectory()) {
      filePath = path.join(filePath, 'index.html');
    }
    const extension = path.extname(filePath).toLowerCase();
    const content = await fs.readFile(filePath);
    res.writeHead(200, { 'Content-Type': MIME_TYPES[extension] || 'application/octet-stream' });
    res.end(content);
  } catch {
    const content = await fs.readFile(path.join(PUBLIC_DIR, 'index.html'));
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(content);
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  if (!chunks.length) {
    return {};
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw createHttpError(400, 'Некорректный JSON в теле запроса.');
  }
}

async function ensureStateFile() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  try {
    await fs.access(STATE_FILE);
  } catch {
    await fs.writeFile(STATE_FILE, JSON.stringify(defaultState(), null, 2), 'utf8');
  }
}

function defaultState() {
  return {
    moysklad: null,
    stores: [],
    products: [],
    lastImportAt: null,
    lastSyncAt: null,
  };
}

async function readState() {
  await ensureStateFile();
  const raw = await fs.readFile(STATE_FILE, 'utf8');
  try {
    const parsed = JSON.parse(raw);
    return { ...defaultState(), ...parsed };
  } catch (error) {
    const brokenStatePath = path.join(DATA_DIR, `app-state.corrupted-${Date.now()}.json`);

    try {
      await fs.rename(STATE_FILE, brokenStatePath);
      await writeState(defaultState());
      return defaultState();
    } catch {
      throw createHttpError(
        500,
        'Файл состояния повреждён после неудачной записи. Освободите место на диске и перезапустите приложение.',
      );
    }
  }
}

async function writeState(state) {
  const preparedState = compactStateForStorage(state);
  const tempFile = `${STATE_FILE}.${process.pid}.tmp`;

  try {
    await fs.writeFile(tempFile, JSON.stringify(preparedState, null, 2), 'utf8');
    await fs.rename(tempFile, STATE_FILE);
  } catch (error) {
    await fs.rm(tempFile, { force: true }).catch(() => {});
    if (error.code === 'ENOSPC') {
      throw createHttpError(
        507,
        'На диске закончилось место. Освободите место и повторите операцию — текущий файл состояния не был перезаписан.',
      );
    }
    throw error;
  }
}

function compactStateForStorage(state) {
  return {
    ...state,
    products: (state.products || []).map((product) => ({
      ...product,
      lastSync: Object.fromEntries(
        Object.entries(product.lastSync || {}).map(([storeId, info]) => [
          storeId,
          {
            ...info,
            message: truncateText(info?.message || '', 280),
          },
        ]),
      ),
    })),
  };
}

function sanitizeState(state) {
  return {
    moysklad: state.moysklad
      ? {
          name: state.moysklad.name,
          authMode: state.moysklad.authMode,
          baseUrl: state.moysklad.baseUrl,
          connectedAt: state.moysklad.connectedAt,
        }
      : null,
    stores: state.stores.map((store) => ({
      id: store.id,
      marketplace: store.marketplace,
      name: store.name,
      warehouseId: store.warehouseId,
      clientId: store.clientId || '',
      tokenMasked: maskSecret(store.token || store.apiKey || ''),
      createdAt: store.createdAt,
    })),
    products: state.products,
    lastImportAt: state.lastImportAt,
    lastSyncAt: state.lastSyncAt,
  };
}

function normalizeMoySkladPayload(body) {
  const name = requiredString(body.name, 'Укажите название подключения МойСклад.');
  const authMode = body.authMode === 'basic' ? 'basic' : 'token';
  const baseUrl = normalizeMoySkladBaseUrl(body.baseUrl || MOYSKLAD_BASE_URL);
  const credential = requiredString(body.credential, 'Укажите логин или токен МойСклад.');
  const secret = authMode === 'basic' ? requiredString(body.secret, 'Укажите пароль для МойСклад.') : (body.secret || '').trim();

  return { name, authMode, baseUrl, credential, secret };
}


function normalizeMoySkladBaseUrl(value) {
  const raw = cleanString(value) || MOYSKLAD_BASE_URL;
  let url;

  try {
    url = new URL(raw);
  } catch {
    throw createHttpError(400, 'Укажите корректный URL API МойСклад, например https://api.moysklad.ru/api/remap/1.2.');
  }

  if (url.hostname === 'dev.moysklad.ru' && url.pathname.includes('/doc/api/remap/1.2')) {
    return MOYSKLAD_BASE_URL;
  }

  if (url.pathname.includes('/doc/')) {
    throw createHttpError(400, 'Укажите URL JSON API МойСклад. Если вы вставляете ссылку из документации, будет работать адрес https://api.moysklad.ru/api/remap/1.2.');
  }

  if (url.hostname === 'api.moysklad.ru' && (url.pathname === '' || url.pathname === '/')) {
    url.pathname = '/api/remap/1.2';
  }

  if (!url.pathname.includes('/api/remap/1.2')) {
    throw createHttpError(400, 'Укажите URL API МойСклад в формате https://api.moysklad.ru/api/remap/1.2.');
  }

  url.pathname = url.pathname.replace(/\/$/, '');
  url.search = '';
  url.hash = '';

  return `${url.origin}${url.pathname}`;
}

function normalizeStorePayload(body) {
  const marketplace = body.marketplace === 'ozon' ? 'ozon' : 'wb';
  const name = requiredString(body.name, 'Укажите название магазина.');
  const warehouseId = requiredString(body.warehouseId, 'Укажите ID склада/warehouse_id.');

  if (marketplace === 'wb') {
    return {
      marketplace,
      name,
      warehouseId,
      token: requiredString(body.token, 'Укажите WB API токен.'),
    };
  }

  return {
    marketplace,
    name,
    warehouseId,
    clientId: requiredString(body.clientId, 'Укажите Ozon Client ID.'),
    apiKey: requiredString(body.apiKey, 'Укажите Ozon API key.'),
  };
}

function normalizeBulkMarketplacePayload(body) {
  const marketplace = body.marketplace === 'ozon' ? 'ozon' : 'wb';
  return {
    marketplace,
    enabled: body.enabled !== false,
  };
}

function normalizeProductPayload(body) {
  const wbEnabled = Boolean(body.marketplaces?.wb?.enabled);
  const ozonEnabled = Boolean(body.marketplaces?.ozon?.enabled);

  const product = {
    name: requiredString(body.name, 'Укажите название товара.'),
    sku: requiredString(body.sku, 'Укажите SKU товара.'),
    stock: normalizeStock(body.stock),
    marketplaces: {
      wb: {
        enabled: wbEnabled,
        sku: cleanString(body.marketplaces?.wb?.sku),
        chrtId: cleanString(body.marketplaces?.wb?.chrtId),
      },
      ozon: {
        enabled: ozonEnabled,
        offerId: cleanString(body.marketplaces?.ozon?.offerId),
        productId: cleanString(body.marketplaces?.ozon?.productId),
        warehouseId: cleanString(body.marketplaces?.ozon?.warehouseId),
      },
    },
  };

  if (wbEnabled && !product.marketplaces.wb.sku && !product.marketplaces.wb.chrtId) {
    throw createHttpError(400, 'Для WB укажите SKU или chrtId у товара.');
  }
  if (ozonEnabled && !product.marketplaces.ozon.offerId && !product.marketplaces.ozon.productId) {
    throw createHttpError(400, 'Для Ozon укажите offer_id или product_id у товара.');
  }

  return product;
}

function createStoredProduct(product, source = 'moysklad') {
  return {
    id: crypto.randomUUID(),
    source,
    sourceId: product.sourceId || null,
    name: product.name,
    sku: product.sku,
    stock: normalizeStock(product.stock),
    marketplaces: product.marketplaces || {
      wb: { enabled: false, sku: '', chrtId: '' },
      ozon: { enabled: false, offerId: '', productId: '', warehouseId: '' },
    },
    lastSync: {},
    lastSyncedAt: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function createDefaultMarketplacesForSku(sku) {
  return {
    wb: { enabled: true, sku, chrtId: '' },
    ozon: { enabled: true, offerId: sku, productId: '', warehouseId: '' },
  };
}

function mergeImportedMarketplaces(existingMarketplaces, sku) {
  const defaults = createDefaultMarketplacesForSku(sku);
  const current = existingMarketplaces || {};

  return {
    wb: {
      enabled: current.wb?.enabled ?? defaults.wb.enabled,
      sku: current.wb?.sku || defaults.wb.sku,
      chrtId: current.wb?.chrtId || '',
    },
    ozon: {
      enabled: current.ozon?.enabled ?? defaults.ozon.enabled,
      offerId: current.ozon?.offerId || defaults.ozon.offerId,
      productId: current.ozon?.productId || '',
      warehouseId: current.ozon?.warehouseId || '',
    },
  };
}

function applyBulkMarketplaceFlag(product, marketplace, enabled) {
  const current = product.marketplaces || {};
  const defaults = createDefaultMarketplacesForSku(product.sku);

  if (marketplace === 'wb') {
    return {
      ...current,
      wb: {
        enabled,
        sku: current.wb?.sku || defaults.wb.sku,
        chrtId: current.wb?.chrtId || '',
      },
      ozon: current.ozon || defaults.ozon,
    };
  }

  return {
    ...current,
    wb: current.wb || defaults.wb,
    ozon: {
      enabled,
      offerId: current.ozon?.offerId || defaults.ozon.offerId,
      productId: current.ozon?.productId || '',
      warehouseId: current.ozon?.warehouseId || '',
    },
  };
}

function labelForMarketplace(marketplace) {
  return marketplace === 'wb' ? 'WB' : 'Ozon';
}

async function verifyMoySkladConnection(config) {
  const normalizedConfig = { ...config, baseUrl: normalizeMoySkladBaseUrl(config.baseUrl) };
  const response = await fetch(`${normalizedConfig.baseUrl}/entity/organization?limit=1`, {
    method: 'GET',
    headers: buildMoySkladHeaders(normalizedConfig),
  });

  if (!response.ok) {
    throw await createMoySkladResponseError(response, 'Не удалось проверить МойСклад');
  }

  await parseMoySkladJson(response, 'Не удалось проверить МойСклад');
}

async function fetchMoySkladProducts(config) {
  const normalizedConfig = { ...config, baseUrl: normalizeMoySkladBaseUrl(config.baseUrl) };
  const items = [];
  let offset = 0;
  const limit = 100;

  while (true) {
    const url = new URL(`${normalizedConfig.baseUrl}/entity/assortment`);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(offset));
    url.searchParams.set('filter', 'archived=false');

    const response = await fetch(url, {
      method: 'GET',
      headers: buildMoySkladHeaders(normalizedConfig),
    });

    if (!response.ok) {
      throw await createMoySkladResponseError(response, 'Ошибка выгрузки из МойСклад');
    }

    const data = await parseMoySkladJson(response, 'Ошибка выгрузки из МойСклад');
    const rows = Array.isArray(data.rows) ? data.rows : [];
    items.push(
      ...rows
        .filter((row) => ['product', 'variant', 'bundle'].includes(row.meta?.type) || row.stock !== undefined)
        .map((row) => ({
          sourceId: row.id,
          name: row.name || row.article || row.code || row.id,
          sku: row.code || row.article || row.id,
          stock: normalizeImportedStock(row.stock ?? row.quantity ?? 0),
          marketplaces: createDefaultMarketplacesForSku(row.code || row.article || row.id),
        })),
    );

    if (rows.length < limit) {
      break;
    }
    offset += limit;
  }

  return items;
}


async function parseMoySkladJson(response, context) {
  const text = await safeReadText(response);
  const trimmed = text.trim();
  const contentType = (response.headers.get('content-type') || '').toLowerCase();

  if (!trimmed) {
    return {};
  }

  if (looksLikeHtml(trimmed) || !contentType.includes('application/json')) {
    throw createHttpError(502, `${context}: МойСклад вернул HTML вместо JSON. Проверьте URL API — нужен https://api.moysklad.ru/api/remap/1.2, а не страница документации.`);
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    throw createHttpError(502, `${context}: МойСклад вернул некорректный JSON.`);
  }
}

async function createMoySkladResponseError(response, context) {
  const text = await safeReadText(response);
  const detail = explainMoySkladResponse(text) || response.statusText;
  return createHttpError(response.status, `${context}: ${detail}`);
}

function explainMoySkladResponse(text) {
  const trimmed = text.trim();
  if (!trimmed) {
    return '';
  }

  if (looksLikeHtml(trimmed)) {
    return 'МойСклад вернул HTML вместо JSON. Проверьте URL API — нужен https://api.moysklad.ru/api/remap/1.2, а не ссылка на документацию.';
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed.errors) && parsed.errors.length) {
      return parsed.errors.map((item) => item.error || item.code).filter(Boolean).join('; ');
    }

    if (typeof parsed.error === 'string') {
      return parsed.error;
    }

    if (typeof parsed.message === 'string') {
      return parsed.message;
    }
  } catch {
    return trimmed.slice(0, 300);
  }

  return trimmed.slice(0, 300);
}

function looksLikeHtml(value) {
  const normalized = value.trim().toLowerCase();
  return normalized.startsWith('<!doctype') || normalized.startsWith('<html') || normalized.startsWith('<body') || normalized.startsWith('<');
}

function buildMoySkladHeaders(config) {
  if (config.authMode === 'basic') {
    const credentials = Buffer.from(`${config.credential}:${config.secret}`).toString('base64');
    return {
      Authorization: `Basic ${credentials}`,
      Accept: 'application/json;charset=utf-8',
    };
  }

  return {
    Authorization: `Bearer ${config.credential}`,
    Accept: 'application/json;charset=utf-8',
  };
}

function isProductLinked(product, marketplace) {
  return Boolean(product.marketplaces?.[marketplace]?.enabled);
}

function startAutoSync() {
  setInterval(async () => {
    try {
      await runSyncJob('auto');
    } catch (error) {
      console.error('[auto-sync]', error.message);
    }
  }, AUTO_SYNC_INTERVAL_MS);

  console.log(`Automatic stock sync enabled every ${AUTO_SYNC_INTERVAL_MS / 60000} minutes.`);
}

async function runSyncJob(trigger = 'manual') {
  if (syncInProgress) {
    if (trigger === 'auto') {
      return null;
    }
    throw createHttpError(409, 'Синхронизация уже выполняется.');
  }

  syncInProgress = true;

  try {
    const state = await readState();
    if (!state.stores.length) {
      throw createHttpError(400, 'Добавьте хотя бы один магазин WB или Ozon.');
    }

    const syncAt = new Date().toISOString();
    const summary = [];

    for (const store of state.stores) {
      const products = state.products.filter((product) => isProductLinked(product, store.marketplace));
      if (!products.length) {
        summary.push({ storeId: store.id, name: store.name, marketplace: store.marketplace, synced: 0, skipped: 0, failed: 0 });
        continue;
      }

      const result =
        store.marketplace === 'wb'
          ? await syncWildberriesStore(store, products)
          : await syncOzonStore(store, products);

      const successIds = new Set(result.successIds);
      const failureIds = new Map(result.failures.map((item) => [item.productId, item.message]));
      const skippedIds = new Map(result.skipped.map((item) => [item.productId, item.message]));

      state.products = state.products.map((product) => {
        if (!products.some((item) => item.id === product.id)) {
          return product;
        }

        const log = product.lastSync || {};
        if (successIds.has(product.id)) {
          return {
            ...product,
            lastSyncedAt: syncAt,
            lastSync: {
              ...log,
              [store.id]: {
                status: 'success',
                syncedAt: syncAt,
                message: truncateText(`Остаток отправлен в ${store.name}.`, 280),
              },
            },
          };
        }

        if (failureIds.has(product.id)) {
          return {
            ...product,
            lastSync: {
              ...log,
              [store.id]: {
                status: 'error',
                syncedAt: syncAt,
                message: truncateText(failureIds.get(product.id), 280),
              },
            },
          };
        }

        if (skippedIds.has(product.id)) {
          return {
            ...product,
            lastSync: {
              ...log,
              [store.id]: {
                status: 'skipped',
                syncedAt: syncAt,
                message: truncateText(skippedIds.get(product.id), 280),
              },
            },
          };
        }

        return product;
      });

      summary.push({
        storeId: store.id,
        name: store.name,
        marketplace: store.marketplace,
        synced: result.successIds.length,
        skipped: result.skipped.length,
        failed: result.failures.length,
      });
    }

    state.lastSyncAt = syncAt;
    await writeState(state);
    return { state, summary };
  } finally {
    syncInProgress = false;
  }
}

async function syncWildberriesStore(store, products) {
  const vendorCodes = products
    .map((product) => product.marketplaces?.wb?.sku)
    .filter((value) => typeof value === 'string' && value.trim().length > 0);
  const wbCardMap = await fetchWbCardMapByVendorCodes(store, vendorCodes);
  const prepared = products.flatMap((product) => buildWbStockEntries(product, wbCardMap));

  const valid = prepared.filter((item) => item.stock);
  const skipped = prepared.filter((item) => item.type === 'skip');
  const successIds = [];
  const failures = [];

  for (const chunk of chunkArray(valid, CHUNK_SIZE)) {
    const response = await fetch(`${WB_API_URL}/api/v3/stocks/${store.warehouseId}`, {
      method: 'PUT',
      headers: {
        Authorization: store.token,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ stocks: chunk.map((item) => item.stock) }),
    });

    if (!response.ok) {
      const message = await safeReadText(response);
      failures.push(
        ...chunk.map((item) => ({
          productId: item.productId,
          message: truncateText(`WB ${store.name}: ${message || response.statusText}`, 280),
        })),
      );
      continue;
    }

    successIds.push(...chunk.map((item) => item.productId));
    await wait(220);
  }

  return { successIds, failures, skipped };
}

function buildWbStockEntries(product, wbCardMap) {
  const mapping = product.marketplaces.wb || {};
  if (mapping.chrtId) {
    return [
      {
        productId: product.id,
        stock: {
          amount: normalizeStock(product.stock),
          chrtId: Number(mapping.chrtId),
        },
      },
    ];
  }

  if (!mapping.sku) {
    return [{ productId: product.id, type: 'skip', message: truncateText('У товара не указан WB артикул продавца или chrtId.', 280) }];
  }

  const resolvedSkus = wbCardMap.get(mapping.sku);
  if (resolvedSkus?.length) {
    return resolvedSkus.map((resolved) => ({
      productId: product.id,
      stock: {
        amount: normalizeStock(product.stock),
        ...(resolved.sku ? { sku: resolved.sku } : {}),
        ...(resolved.chrtId ? { chrtId: Number(resolved.chrtId) } : {}),
      },
    }));
  }

  return [
    {
      productId: product.id,
      stock: {
        amount: normalizeStock(product.stock),
        sku: mapping.sku,
      },
    },
  ];
}

async function fetchWbCardMapByVendorCodes(store, vendorCodes) {
  const remaining = new Set(vendorCodes);
  const cardMap = new Map();
  let cursor = { limit: 100 };

  while (remaining.size) {
    const response = await fetch(`${WB_CONTENT_API_URL}/content/v2/get/cards/list`, {
      method: 'POST',
      headers: {
        Authorization: store.token,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        settings: {
          cursor,
          filter: {
            withPhoto: -1,
          },
        },
      }),
    });

    if (!response.ok) {
      break;
    }

    const data = await safeReadJson(response);
    const cards = Array.isArray(data?.cards) ? data.cards : [];
    if (!cards.length) {
      break;
    }

    for (const card of cards) {
      const vendorCode = card.vendorCode;
      if (!vendorCode || !remaining.has(vendorCode)) {
        continue;
      }

      const entries = (card.sizes || [])
        .flatMap((size) =>
          (size.skus || []).map((sku) => ({
            sku,
            chrtId: size.chrtID || size.chrtId || null,
          })),
        )
        .filter((entry) => entry.sku || entry.chrtId);

      if (entries.length) {
        cardMap.set(vendorCode, entries);
        remaining.delete(vendorCode);
      }
    }

    const updatedAt = data?.cursor?.updatedAt;
    const nmID = data?.cursor?.nmID;
    if (!updatedAt || !nmID) {
      break;
    }

    cursor = {
      limit: 100,
      updatedAt,
      nmID,
    };
  }

  return cardMap;
}

async function syncOzonStore(store, products) {
  const prepared = products.map((product) => {
    const mapping = product.marketplaces.ozon || {};
    const warehouseId = mapping.warehouseId || store.warehouseId;

    if (!mapping.offerId && !mapping.productId) {
      return { productId: product.id, type: 'skip', message: truncateText('У товара не указан Ozon offer_id или product_id.', 280) };
    }
    if (!warehouseId) {
      return { productId: product.id, type: 'skip', message: truncateText('Не указан warehouse_id для Ozon.', 280) };
    }

    return {
      productId: product.id,
      stock: {
        stock: normalizeStock(product.stock),
        warehouse_id: Number(warehouseId),
        ...(mapping.offerId ? { offer_id: mapping.offerId } : {}),
        ...(mapping.productId ? { product_id: Number(mapping.productId) } : {}),
      },
    };
  });

  const valid = prepared.filter((item) => item.stock);
  const skipped = prepared.filter((item) => item.type === 'skip');
  const successIds = [];
  const failures = [];

  for (const chunk of chunkArray(valid, CHUNK_SIZE)) {
    const response = await fetch(`${OZON_API_URL}/v2/products/stocks`, {
      method: 'POST',
      headers: {
        'Client-Id': String(store.clientId),
        'Api-Key': store.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ stocks: chunk.map((item) => item.stock) }),
    });

    const data = await safeReadJson(response);
    if (!response.ok) {
      const message = data?.message || data?.error || response.statusText;
      failures.push(
        ...chunk.map((item) => ({
          productId: item.productId,
          message: truncateText(`Ozon ${store.name}: ${message}`, 280),
        })),
      );
      continue;
    }

    if (Array.isArray(data?.result) && data.result.length) {
      const resultMap = new Map(data.result.map((entry) => [String(entry.offer_id || entry.product_id), entry]));
      chunk.forEach((item) => {
        const key = String(item.stock.offer_id || item.stock.product_id);
        const resultItem = resultMap.get(key);
        if (resultItem?.errors?.length) {
          failures.push({
            productId: item.productId,
            message: truncateText(`Ozon ${store.name}: ${resultItem.errors.join('; ')}`, 280),
          });
        } else {
          successIds.push(item.productId);
        }
      });
    } else {
      successIds.push(...chunk.map((item) => item.productId));
    }
  }

  return { successIds, failures, skipped };
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requiredString(value, message) {
  const normalized = cleanString(value);
  if (!normalized) {
    throw createHttpError(400, message);
  }
  return normalized;
}

function cleanString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeImportedStock(value) {
  const stock = Number(value);
  if (!Number.isFinite(stock)) {
    return 0;
  }
  return Math.max(0, Math.floor(stock));
}

function normalizeStock(value) {
  const stock = Number(value);
  if (!Number.isFinite(stock) || stock < 0) {
    throw createHttpError(400, 'Остаток должен быть числом больше или равным 0.');
  }
  return Math.floor(stock);
}

function maskSecret(value) {
  if (!value) {
    return '';
  }
  if (value.length <= 8) {
    return '••••••';
  }
  return `${value.slice(0, 4)}•••${value.slice(-4)}`;
}

function truncateText(value, maxLength = 280) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1)}…`;
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return '';
  }
}

async function safeReadJson(response) {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function createHttpError(statusCode, message) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}
