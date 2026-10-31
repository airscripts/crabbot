import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { isIP } from 'node:net';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';

const source = readFileSync(new URL('../src/worker.mjs', import.meta.url), 'utf8');
const policy = source.slice(source.indexOf('const MAX_FRAME'), source.indexOf('const input ='));

test('HTTP and WebSocket routes enforce the same host and address policy', async () => {
    const { installRoutes, call, sessions } = runInNewContext(
        `${policy}\n({ installRoutes, call, sessions })`, {
            process: { env: {
                KRABB_BROWSER_ALLOWED_HOSTS:
                    'public.test,private.test,mixed.test,missing.test,127.0.0.1',
            } },
            URL,
            isIP,
            lookup: async (host) => {
                if (host === 'missing.test') throw new Error('DNS unavailable');
                if (host === 'private.test') return [{ address: '10.0.0.1' }];
                if (host === 'mixed.test') return [{ address: '8.8.8.8' }, { address: '::1' }];
                return [{ address: '8.8.8.8' }];
            },
        },
    );

    let http;
    let websocket;
    await installRoutes({
        route: async (_, handler) => { http = handler; },
        routeWebSocket: async (_, handler) => { websocket = handler; },
    });

    for (const protocol of ['http:', 'https:', 'ws:', 'wss:']) {
        for (const [host, permitted] of [
            ['public.test', true],
            ['unlisted.test', false],
            ['private.test', false],
            ['mixed.test', false],
            ['missing.test', false],
            ['127.0.0.1', false],
            ['user:password@public.test', false],
        ]) {
            const url = `${protocol}//${host}/live`;
            let result;
            const route = {
                url: () => url,
                request: () => ({ url: () => url }),
                continue: async () => { result = true; },
                connectToServer: async () => { result = true; },
                abort: async () => { result = false; },
                close: async () => { result = false; },
            };

            const handler = protocol.startsWith('ws') ? websocket : http;
            await handler(route);

            assert.equal(result, permitted, url);
        }
    }

    for (const handler of [http, websocket]) {
        let rejected = false;
        await handler({
            url: () => 'file://public.test/private',
            request: () => ({ url: () => 'file://public.test/private' }),
            abort: async () => { rejected = true; },
            close: async () => { rejected = true; },
        });

        assert.equal(rejected, true);
    }

    let current;
    sessions.set('navigation', {
        tabs: new Map([['tab', {
            goto: async (url) => { current = url; },
            url: () => current,
            title: async () => 'Public page',
        }]]),
        observations: new Map(),
    });

    const params = { session_id: 'navigation', tab_id: 'tab' };
    const result = await call('browser_navigate', { ...params, url: 'https://public.test/' });

    assert.equal(result.url, 'https://public.test/');
    await assert.rejects(
        call('browser_navigate', { ...params, url: 'wss://public.test/' }),
        /HTTP and HTTPS/,
    );
});
