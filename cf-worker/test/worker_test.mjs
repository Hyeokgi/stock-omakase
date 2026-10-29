// Worker 의 직전 사본 먼저 · 예열 동작을 가짜 GAS · 가짜 캐시로 확인하는 시험 — 실행: node cf-worker/test/worker_test.mjs
import assert from 'node:assert/strict';

const store = new Map();
globalThis.caches = { default: {
  async match(req) { const r = store.get(req.url); return r ? r.clone() : undefined; },
  async put(req, res) { store.set(req.url, res); },
}};
let calls = 0, gasBody = '{"scanner":[1]}', gasStatus = 200;
globalThis.fetch = async () => { calls++; return new Response(gasBody, { status: gasStatus }); };

const mod = await import(new URL('../src/index.js', import.meta.url));
const W = mod.default;
const env = { GAS_URL: 'https://gas.example/exec', ASSETS: { fetch: async () => new Response('html') } };
const pending = [];
const ctx = { waitUntil: p => pending.push(p) };
const get = async q => W.fetch(new Request('https://app.example/api' + q), env, ctx);
const realNow = Date.now;

// 1) 처음 — GAS 를 부르고 나이 0
let r = await get('');
assert.equal(calls, 1); assert.equal(r.headers.get('X-Data-Age'), '0'); assert.equal(r.headers.get('Cache-Control'), 'no-store');
assert.equal(await r.text(), '{"scanner":[1]}');

// 2) 20초 안 — GAS 를 다시 안 부른다
r = await get('');
assert.equal(calls, 1);

// 3) 5분 뒤 — 직전 사본을 바로 주고(나이 약 300초) 뒤에서 새로 받는다
Date.now = () => realNow() + 300_000;
gasBody = '{"scanner":[2]}';
r = await get('');
assert.equal(await r.text(), '{"scanner":[1]}');
assert.ok(Number(r.headers.get('X-Data-Age')) >= 299);
await Promise.all(pending.splice(0));
assert.equal(calls, 2);
r = await get('');
assert.equal(await r.text(), '{"scanner":[2]}', '뒤에서 받은 새 자료가 다음 요청에 나간다');

// 4) GAS 가 error 칸을 주면 캐시를 바꾸지 않는다
Date.now = () => realNow() + 900_000;
gasBody = '{"error":"boom"}';
r = await get('');
await Promise.all(pending.splice(0));
r = await get('');
assert.equal(await r.text(), '{"scanner":[2]}', '오류 응답으로 정상 사본을 덮지 않는다');

// 5) 7시간 뒤 — 사본이 너무 오래됐으면 기다려서 새로 받는다
Date.now = () => realNow() + 7 * 3600_000;
gasBody = '{"scanner":[3]}';
r = await get('');
assert.equal(await r.text(), '{"scanner":[3]}'); assert.equal(r.headers.get('X-Data-Age'), '0');

// 6) 처음인데 GAS 가 500 — 그대로 실패를 알린다(캐시 안 함)
store.clear(); gasStatus = 500; gasBody = '{"x":1}';
r = await get('?mode=themeDetail');
assert.equal(r.status, 500);
assert.equal(store.size, 0);

// 7) 예열 — 기본 /api 를 캐시에 채운다
gasStatus = 200; gasBody = '{"scanner":[4]}';
await W.scheduled({}, env, ctx); await Promise.all(pending.splice(0));
assert.ok(store.has('https://gas.example/exec'));

// 8) 화면 요청은 그대로 정적 파일
assert.equal(await (await W.fetch(new Request('https://app.example/'), env, ctx)).text(), 'html');
Date.now = realNow;
console.log('worker ok');
