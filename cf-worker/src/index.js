// 웹앱 화면(정적 파일)을 내보내고 /api 요청을 GAS 로 넘기는 Cloudflare Worker — 직전 응답을 먼저 주고 뒤에서 새로 받는다
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // /api 로 오는 요청만 GAS로 전달 (데이터), 그 외에는 화면(정적 파일) 서빙
    if (url.pathname === '/api' || url.pathname.startsWith('/api/')) {
      return handleApi(url, env, ctx);
    }
    return env.ASSETS.fetch(request);
  },

  // 🆕 2026-10-29 [예열] 평일 장중 · 저녁(KST 08~20시) 5분마다 기본 /api 를 한 번 받아 둔다.
  //    GAS 는 한동안 안 쓰면 첫 호출이 느리다(콜드 스타트). 예열로 GAS 를 깨워 두고 이 지역 캐시도 채운다.
  //    (Cloudflare 캐시는 데이터센터별이라 예열이 도는 곳과 사용자가 붙는 곳이 다를 수 있다 — GAS 를 깨우는 효과가 주목적)
  async scheduled(event, env, ctx) {
    if (!env.GAS_URL) return;
    ctx.waitUntil(refresh(env.GAS_URL, cacheKeyFor(env.GAS_URL), caches.default).catch(() => {}));
  }
}

// [짧은 캐시] 20초 이내 재요청(재시도/연속 새로고침/여러 사용자)은 GAS를 다시 안 타고 바로 응답
//    자동 갱신 주기(3분)보다 훨씬 짧아서 데이터 신선도에는 영향 없음
const FRESH_SEC = 20;
// 🆕 2026-10-29 [직전 사본 먼저] 20초가 지났어도 6시간 안의 마지막 정상 응답이 있으면 GAS 를 기다리지 않고 그것부터 준다.
//    동시에 뒤에서 GAS 를 새로 불러 캐시를 바꿔 둔다. 응답 머리글 X-Data-Age(초)로 자료 나이를 알려 화면이 표시 · 재요청한다.
const STALE_SEC = 6 * 3600;
// [시간제한] GAS가 비정상적으로 오래 걸리면(12초) 무한정 기다리지 않고 빠르게 실패 처리
const TIMEOUT_MS = 12000;

function cacheKeyFor(target) {
  return new Request(target, { method: 'GET' });
}

async function handleApi(url, env, ctx) {
  const gasUrl = env.GAS_URL; // Cloudflare 대시보드에서 Secret으로 등록 (코드에 직접 쓰지 않음)

  if (!gasUrl) {
    return new Response(JSON.stringify({ error: "GAS_URL이 설정되지 않았습니다." }), { status: 500 });
  }

  const target = gasUrl + url.search; // ?mode=themeDetail&date=... 등 쿼리 그대로 전달
  const key = cacheKeyFor(target);
  const cache = caches.default;
  const hit = await cache.match(key);
  const age = hit ? (Date.now() - Number(hit.headers.get('X-Fetched-At') || 0)) / 1000 : Infinity;

  if (hit && age <= FRESH_SEC) return forClient(hit, age);
  if (hit && age <= STALE_SEC) {
    ctx.waitUntil(refresh(target, key, cache).catch(() => {}));
    return forClient(hit, age);
  }
  try {
    return forClient(await refresh(target, key, cache), 0);
  } catch (e) {
    const timedOut = e.name === 'AbortError';
    return new Response(JSON.stringify({ error: String(e), timedOut }), { status: 502 });
  }
}

// GAS 를 한 번 불러 정상 응답(200 · JSON · error 칸 없음)만 캐시에 넣는다. 반환은 받은 그대로.
async function refresh(target, key, cache) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let resp, body;
  try {
    resp = await fetch(target, { signal: controller.signal, cf: { cacheTtl: 0, cacheEverything: false } });
    body = await resp.text();
  } finally {
    clearTimeout(timeoutId);
  }
  const out = new Response(body, {
    status: resp.status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': `public, max-age=${STALE_SEC}`,
      'X-Fetched-At': String(Date.now())
    }
  });
  if (resp.ok && isGoodBody(body)) {
    await cache.put(key, out.clone());
  }
  return out;
}

function isGoodBody(body) {
  try {
    const v = JSON.parse(body);
    return v !== null && typeof v === 'object' && !('error' in v);
  } catch {
    return false;
  }
}

// 브라우저에는 저장하지 말라고 하고(화면이 따로 캐시를 깬다) 자료 나이를 붙여 준다.
function forClient(resp, ageSec) {
  const r = new Response(resp.body, resp);
  r.headers.set('Cache-Control', 'no-store');
  r.headers.set('X-Data-Age', String(Math.max(0, Math.round(ageSec))));
  r.headers.set('Access-Control-Expose-Headers', 'X-Data-Age');
  return r;
}
