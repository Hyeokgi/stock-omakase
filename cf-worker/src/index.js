export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // /api 로 오는 요청만 GAS로 전달 (데이터), 그 외에는 화면(정적 파일) 서빙
    if (url.pathname === '/api' || url.pathname.startsWith('/api/')) {
      return handleApi(url, env, ctx);
    }
    return env.ASSETS.fetch(request);
  }
}

async function handleApi(url, env, ctx) {
  const gasUrl = env.GAS_URL; // Cloudflare 대시보드에서 Secret으로 등록 (코드에 직접 쓰지 않음)

  if (!gasUrl) {
    return new Response(JSON.stringify({ error: "GAS_URL이 설정되지 않았습니다." }), { status: 500 });
  }

  const search = url.search; // ?mode=themeDetail&date=... 등 쿼리 그대로 전달
  const target = gasUrl + search;

  // 🆕 [짧은 캐시] 20초 이내 재요청(재시도/연속 새로고침/여러 사용자)은 GAS를 다시 안 타고 바로 응답
  //    자동 갱신 주기(3분)보다 훨씬 짧아서 데이터 신선도에는 영향 없음
  const cacheKey = new Request(target, { method: 'GET' });
  const cache = caches.default;
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  // 🆕 [시간제한] GAS가 비정상적으로 오래 걸리면(12초) 무한정 기다리지 않고 빠르게 실패 처리
  //    → 클라이언트 재시도 3번이 겹쳐도 최악의 경우 12초×3이 상한선이 됨 (기존엔 상한이 없었음)
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 12000);

  try {
    const resp = await fetch(target, { signal: controller.signal, cf: { cacheTtl: 0, cacheEverything: false } });
    clearTimeout(timeoutId);
    const body = await resp.text();
    const response = new Response(body, {
      status: resp.status,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=20'
      }
    });
    if (resp.ok) {
      ctx.waitUntil(cache.put(cacheKey, response.clone()));
    }
    return response;
  } catch (e) {
    clearTimeout(timeoutId);
    const timedOut = e.name === 'AbortError';
    return new Response(JSON.stringify({ error: String(e), timedOut }), { status: 502 });
  }
}
