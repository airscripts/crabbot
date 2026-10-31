import { createRequire } from 'node:module';
import { createInterface } from 'node:readline';
import { isIP } from 'node:net';
import { lookup } from 'node:dns/promises';
import { randomBytes } from 'node:crypto';

const require = createRequire(import.meta.url);
const { chromium } = require(process.env.KRABB_PLAYWRIGHT_MODULE);
const MAX_FRAME = 8 * 1024 * 1024;
const IMAGE_LIMIT = 4 * 1024 * 1024;
const TEXT_LIMIT = 48 * 1024;
const TAB_LIMIT = 8;
const VIEWPORT = { width: 1280, height: 720 };

const allowedHosts = new Set(
    (process.env.KRABB_BROWSER_ALLOWED_HOSTS || '')
        .split(',')
        .map((host) => host.trim().toLowerCase())
        .filter(Boolean),
);

const sessions = new Map();
let browser;

function id() {
    return randomBytes(18).toString('hex');
}

function privateAddress(address) {
    if (isIP(address) === 4) {
        const octets = address.split('.').map(Number);
        return octets[0] === 0 || octets[0] === 10 || octets[0] === 127
            || octets[0] >= 224 || (octets[0] === 100 && octets[1] >= 64 && octets[1] <= 127)
            || (octets[0] === 169 && octets[1] === 254)
            || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31)
            || (octets[0] === 192 && (octets[1] === 0 || octets[1] === 88 || octets[1] === 168))
            || (octets[0] === 192 && octets[1] === 0 && octets[2] === 2)
            || (octets[0] === 198 && (octets[1] === 18 || octets[1] === 19 || octets[1] === 51))
            || (octets[0] === 203 && octets[1] === 0 && octets[2] === 113);
    }

    const value = address.toLowerCase().split('%')[0];
    const mapped = value.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
    if (mapped) return privateAddress(mapped[1]);

    const mappedHex = value.match(/^::ffff:([\da-f]{1,4}):([\da-f]{1,4})$/);
    if (mappedHex) {
        const high = Number.parseInt(mappedHex[1], 16);
        const low = Number.parseInt(mappedHex[2], 16);
        return privateAddress(`${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`);
    }

    const first = Number.parseInt(value.split(':')[0] || '0', 16);
    const second = Number.parseInt(value.split(':')[1] || '0', 16);
    return first < 0x2000 || first >= 0x4000
        || (first === 0x2001 && second <= 0x01ff)
        || first === 0x2002
        || first === 0x3fff;
}

async function permitted(raw, protocols = ['http:', 'https:']) {
    let url;
    try {
        url = new URL(raw);
    } catch {
        return false;
    }

    const host = url.hostname.toLowerCase();
    if (!protocols.includes(url.protocol) || url.username || url.password
            || !allowedHosts.has(host)) {
        return false;
    }

    if (isIP(host)) return !privateAddress(host);

    try {
        const addresses = await lookup(host, { all: true, verbatim: true });
        return addresses.length > 0 && addresses.every(({ address }) => !privateAddress(address));
    } catch {
        return false;
    }
}

async function installRoutes(context) {
    await context.route('**/*', async (route) => {
        if (await permitted(route.request().url())) {
            await route.continue();
        } else {
            await route.abort('blockedbyclient');
        }
    });

    if (typeof context.routeWebSocket === 'function') {
        await context.routeWebSocket('**/*', async (route) => {
            if (await permitted(route.url(), ['ws:', 'wss:'])) {
                await route.connectToServer();
            } else {
                await route.close();
            }
        });
    }
}

function registerTab(state, page) {
    if (state.tabs.size >= TAB_LIMIT) {
        void page.close().catch(() => {});
        return null;
    }

    const tabId = id();
    state.tabs.set(tabId, page);
    page.on('popup', (popup) => registerTab(state, popup));
    page.on('close', () => {
        state.tabs.delete(tabId);
        state.observations.delete(tabId);
    });

    return tabId;
}

async function launch() {
    if (!browser || !browser.isConnected()) {
        const proxyUrl = process.env.KRABB_BROWSER_EGRESS_PROXY;
        let proxy;
        try {
            const value = new URL(proxyUrl || '');
            if (!['http:', 'https:'].includes(value.protocol) || !value.hostname
                    || value.pathname !== '/' || value.search || value.hash) {
                throw new Error();
            }

            proxy = { server: `${value.protocol}//${value.host}` };
            if (value.username || value.password) {
                proxy.username = decodeURIComponent(value.username);
                proxy.password = decodeURIComponent(value.password);
            }
        } catch {
            throw new Error('KRABB_BROWSER_EGRESS_PROXY must be a valid HTTP or HTTPS proxy URL.');
        }

        browser = await chromium.launch({
            headless: true,
            timeout: 30000,
            proxy,
            env: Object.fromEntries(
                [
                    'PATH', 'HOME', 'USERPROFILE', 'TMPDIR', 'TMP', 'TEMP',
                    'SystemRoot', 'WINDIR', 'APPDATA', 'LOCALAPPDATA',
                ]
                    .filter((key) => process.env[key])
                    .map((key) => [key, process.env[key]]),
            ),
            args: [
                '--disable-background-networking',
                '--disable-sync',
                '--disable-quic',
                '--force-webrtc-ip-handling-policy=disable_non_proxied_udp',
                '--proxy-bypass-list=<-loopback>',
                '--no-first-run',
            ],
        });
    }

    return browser;
}

async function open(params) {
    const instance = await launch();
    const context = await instance.newContext({
        viewport: VIEWPORT,
        deviceScaleFactor: 1,
        acceptDownloads: false,
        permissions: [],
        serviceWorkers: 'block',
    });

    await installRoutes(context);
    const state = { context, tabs: new Map(), observations: new Map() };
    const page = await context.newPage();
    const tabId = registerTab(state, page);
    sessions.set(params.session_id, state);

    return { session_id: params.session_id, tab_id: tabId, viewport: VIEWPORT };
}

function session(params) {
    const value = sessions.get(params.session_id);
    if (!value) throw new Error('Browser session is closed or unavailable.');
    return value;
}

function tab(state, params) {
    const entry = params.tab_id
        ? [params.tab_id, state.tabs.get(params.tab_id)]
        : state.tabs.entries().next().value;

    if (!entry || !entry[1]) throw new Error('Browser tab is unavailable.');
    return { id: entry[0], page: entry[1] };
}

function safeUrl(value) {
    try {
        const url = new URL(value);
        url.search = '';
        url.hash = '';
        return url.toString();
    } catch {
        return '';
    }
}

function locator(page, params) {
    const target = params.target;
    switch (params.by) {
        case 'role':
            if (!params.role) throw new Error('role is required for role locators.');
            return page.getByRole(params.role, { name: params.name, exact: params.exact === true });

        case 'label': return page.getByLabel(target, { exact: params.exact === true });
        case 'placeholder': return page.getByPlaceholder(target, { exact: params.exact === true });
        case 'text': return page.getByText(target, { exact: params.exact === true });
        case 'test_id': return page.getByTestId(target);
        case 'css': return page.locator(target);
        default: throw new Error('Unsupported structured locator.');
    }
}

async function snapshot(params) {
    const { page } = tab(session(params), params);
    const text = (
        await page.locator('body').innerText({ timeout: 5000 }).catch(() => '')
    ).slice(0, TEXT_LIMIT);

    const controls = await page
        .locator('button,input,textarea,select,a,[role="button"],[role="link"]')
        .evaluateAll((items) => items.slice(0, 100).flatMap((element) => {
            const bounds = element.getBoundingClientRect();
            const style = getComputedStyle(element);
            if (
                !bounds.width || !bounds.height
                || style.visibility === 'hidden'
                || style.display === 'none'
            ) return [];

            const label = element.getAttribute('aria-label') || element.labels?.[0]?.innerText
                || element.innerText || element.getAttribute('placeholder') || '';

            return [{
                role: element.getAttribute('role') || element.tagName.toLowerCase(),
                label: String(label).trim().slice(0, 256),
                type: element.getAttribute('type') || undefined,
                disabled: Boolean(element.disabled),
            }];
        }));

    return { url: safeUrl(page.url()), title: (await page.title()).slice(0, 512), text, controls };
}

async function screenshot(params) {
    const state = session(params);
    const { id: tabId, page } = tab(state, params);
    const format = params.format === 'jpeg' ? 'jpeg' : 'png';
    const options = {
        type: format,
        quality: format === 'jpeg' ? 85 : undefined,
        animations: 'disabled',
        timeout: 10000,
    };

    let image;
    let width;
    let height;
    let coordinateSpace;

    if (params.by) {
        if (params.full_page === true) {
            throw new Error('Choose either an element or a full-page screenshot.');
        }

        const target = locator(page, params);
        const bounds = await target.boundingBox({ timeout: 5000 });
        if (!bounds) throw new Error('The screenshot element is not visible.');

        image = await target.screenshot(options);
        width = Math.ceil(bounds.width);
        height = Math.ceil(bounds.height);
        coordinateSpace = 'element';
        state.observations.delete(tabId);
    } else if (params.full_page === true) {
        image = await page.screenshot({ ...options, fullPage: true });
        const bounds = await page.evaluate(() => ({
            width: document.documentElement.scrollWidth,
            height: document.documentElement.scrollHeight,
        }));

        width = bounds.width;
        height = bounds.height;
        coordinateSpace = 'document';
        state.observations.delete(tabId);
    } else {
        if (params.target || params.role || params.name || params.exact !== undefined) {
            throw new Error('Element locator fields require a supported `by` value.');
        }

        image = await page.screenshot({ ...options, fullPage: false });
        width = VIEWPORT.width;
        height = VIEWPORT.height;
        coordinateSpace = 'viewport';
    }

    if (image.byteLength > IMAGE_LIMIT) {
        throw new Error('Screenshot exceeds the 4 MiB image limit.');
    }

    const mime = `image/${format}`;
    const metadata = {
        width,
        height,
        format: mime,
        coordinate_space: coordinateSpace,
        scale: 1,
        tab_id: tabId,
    };

    if (coordinateSpace === 'viewport') {
        const observationId = id();
        state.observations.set(tabId, {
            id: observationId,
            url: page.url(),
            viewport: page.viewportSize(),
            turn: params.turn_id,
            time: Date.now(),
        });

        metadata.observation_id = observationId;
    }

    return {
        content: [
            { kind: 'text', text: 'Browser viewport captured.' },
            {
                kind: 'image',
                uri: `data:${mime};base64,${image.toString('base64')}`,
                alt: 'Browser viewport',
            },
        ],
        metadata,
    };
}

async function click(params) {
    const state = session(params);
    const { id: tabId, page } = tab(state, params);
    if (params.by !== 'coordinate') {
        await locator(page, params).click({ timeout: 10000 });
        state.observations.delete(tabId);
        return { clicked: true };
    }

    const observation = state.observations.get(tabId);
    const viewport = page.viewportSize();
    if (
        !observation
        || observation.id !== params.observation_id
        || observation.url !== page.url()
        || observation.turn !== params.turn_id
        || observation.time + 30000 < Date.now()
        || observation.viewport.width !== viewport.width
        || observation.viewport.height !== viewport.height
        || params.x >= viewport.width
        || params.y >= viewport.height
    ) {
        throw new Error('Coordinate click requires a fresh screenshot of the current viewport.');
    }

    await page.mouse.click(params.x, params.y, { timeout: 10000 });
    state.observations.delete(tabId);
    return { clicked: true, observation_id: observation.id };
}

async function call(method, params) {
    if (method === 'browser_open') return open(params);
    if (method === 'browser_close') {
        const state = session(params);
        await state.context.close();
        sessions.delete(params.session_id);
        return { closed: true };
    }

    const state = session(params);
    switch (method) {
        case 'browser_tabs': {
            const tabs = await Promise.all([...state.tabs].map(async ([id, page]) => {
                try {
                    return {
                        id,
                        url: safeUrl(page.url()),
                        title: (await page.title()).slice(0, 256),
                    };
                } catch (error) {
                    if (!page.isClosed()) throw error;
                    return null;
                }
            }));

            return tabs.filter(Boolean);
        }

        case 'browser_new_tab': {
            if (state.tabs.size >= TAB_LIMIT) throw new Error('Tab capacity is full.');
            const page = await state.context.newPage();
            return { tab_id: registerTab(state, page) };
        }
    }

    const { id: tabId, page } = tab(state, params);
    switch (method) {
        case 'browser_navigate': {
            let url;
            try {
                url = new URL(params.url);
            } catch {
                throw new Error('A valid URL is required.');
            }

            if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
                throw new Error('Only credential-free HTTP and HTTPS URLs are supported.');
            }

            if (!(await permitted(url.toString()))) {
                throw new Error('The navigation target is not permitted by the browser policy.');
            }

            await page.goto(url.toString(), { waitUntil: 'domcontentloaded', timeout: 30000 });
            if (!(await permitted(page.url()))) {
                throw new Error(
                    'The navigation ended at a URL that is not permitted by the browser policy.',
                );
            }

            state.observations.delete(tabId);
            return { url: safeUrl(page.url()), title: (await page.title()).slice(0, 512) };
        }

        case 'browser_close_tab': {
            const selected = state.tabs.get(params.tab_id);
            if (!selected) throw new Error('Browser tab is unavailable.');
            await selected.close();
            return { closed: true };
        }

        case 'browser_snapshot': return snapshot(params);
        case 'browser_screenshot': return screenshot(params);
        case 'browser_click': return click(params);
        case 'browser_fill':
            await locator(page, params).fill(params.value, { timeout: 10000 });
            state.observations.delete(tabId);
            return { filled: true };

        case 'browser_select':
            await locator(page, params).selectOption(params.value, { timeout: 10000 });
            state.observations.delete(tabId);
            return { selected: true };

        case 'browser_press':
            await page.keyboard.press(params.key);
            state.observations.delete(tabId);
            return { pressed: true };

        case 'browser_scroll':
            await page.mouse.wheel(params.delta_x || 0, params.delta_y);
            state.observations.delete(tabId);
            return { scrolled: true };

        case 'browser_wait':
            await locator(page, params).waitFor({
                state: 'visible',
                timeout: params.timeout_ms || 5000,
            });

            return { visible: true };
        default:
            throw new Error('Unsupported browser operation.');
    }
}

function safeError(error) {
    return String(error?.message || 'Browser operation failed.')
        .replace(/https?:\/\/[^\s]+/g, '<URL>')
        .slice(0, 512);
}

const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of input) {
    let request;
    try {
        if (Buffer.byteLength(line) > MAX_FRAME) {
            throw new Error('Request exceeded the frame limit.');
        }

        request = JSON.parse(line);
        if (!Number.isSafeInteger(request.id) || typeof request.method !== 'string') {
            throw new Error('Malformed browser request.');
        }

        const result = await call(request.method, request.params || {});
        const response = JSON.stringify({ id: request.id, ok: true, result });
        if (Buffer.byteLength(response) > MAX_FRAME - 1) {
            throw new Error('Browser response exceeded the frame limit.');
        }

        process.stdout.write(`${response}\n`);
    } catch (error) {
        const response = JSON.stringify({
            id: request?.id ?? 0,
            ok: false,
            error: safeError(error),
        });

        process.stdout.write(`${response}\n`);
    }
}

await browser?.close().catch(() => {});
