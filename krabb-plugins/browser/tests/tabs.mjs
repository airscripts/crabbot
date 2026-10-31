import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';

const source = readFileSync(new URL('../src/worker.mjs', import.meta.url), 'utf8');
const dispatcher = source.slice(source.indexOf('const MAX_FRAME'), source.indexOf('const input ='));

class Page extends EventEmitter {
    closed = false;

    url() { return 'about:blank'; }

    isClosed() { return this.closed; }

    async title() {
        assert.equal(this.closed, false, 'closed pages must not be queried');
        return 'Fixture';
    }

    async close() {
        this.closed = true;
        this.emit('close');
    }
}

function fixture() {
    const worker = runInNewContext(`${dispatcher}\n({ call, sessions, registerTab })`, {
        process: { env: {} },
        randomBytes,
    });

    const state = {
        context: { newPage: async () => new Page() },
        tabs: new Map(),
        observations: new Map(),
    };

    worker.sessions.set('fixture', state);
    const opened = { tab_id: worker.registerTab(state, new Page()) };
    return { ...worker, state, opened, params: { session_id: 'fixture' } };
}

test('closing the final tab permits listing and opening in the same context', async () => {
    const { call, state, opened, params } = fixture();
    const context = state.context;
    state.observations.set(opened.tab_id, { id: 'observation' });
    await call('browser_close_tab', { ...params, tab_id: opened.tab_id });

    assert.equal((await call('browser_tabs', params)).length, 0);
    assert.equal(state.observations.size, 0);

    const replacement = await call('browser_new_tab', params);
    const tabs = await call('browser_tabs', params);
    assert.equal(tabs.length, 1);
    assert.equal(tabs[0].id, replacement.tab_id);
    assert.equal(state.context, context);
});

test('website-closed popups release observations and capacity', async () => {
    const { call, state, opened, params } = fixture();
    const original = state.tabs.get(opened.tab_id);

    for (let attempt = 0; attempt < 10; attempt += 1) {
        const popup = new Page();
        original.emit('popup', popup);
        const tabId = [...state.tabs].find(([, page]) => page === popup)?.[0];
        assert.ok(tabId, 'popup capacity must be reclaimed');
        state.observations.set(tabId, { id: 'observation' });
        await popup.close();

        assert.equal(state.observations.has(tabId), false);
        assert.equal((await call('browser_tabs', params)).length, 1);
    }

    await call('browser_new_tab', params);
    assert.equal((await call('browser_tabs', params)).length, 2);
});

test('listing tolerates a page closing during its title request', async () => {
    const { call, state, opened, params } = fixture();
    const page = state.tabs.get(opened.tab_id);
    page.title = async () => {
        await page.close();
        throw new Error('Page closed');
    };

    assert.equal((await call('browser_tabs', params)).length, 0);
    assert.equal(state.tabs.size, 0);

    const replacement = await call('browser_new_tab', params);
    state.tabs.get(replacement.tab_id).title = async () => {
        throw new Error('Unrelated failure');
    };

    await assert.rejects(call('browser_tabs', params), /Unrelated failure/);
});
