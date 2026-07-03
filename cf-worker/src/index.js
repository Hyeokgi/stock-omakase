export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // /api 로 오는 요청만 GAS로 전달 (데이터), 그 외에는 화면(정적 파일) 서빙
    if (url.pathname === '/api' || url.pathname.startsWith('/api/')) {
      return handleApi(url, env);
    }
    return env.ASSETS.fetch(request);
  }
}

async function handleApi(url, env) {
  const gasUrl = env.GAS_URL; // Cloudflare 대시보드에서 Secret으로 등록 (코드에 직접 쓰지 않음)

  if (!gasUrl) {
    return new Response(JSON.stringify({ error: "GAS_URL이 설정되지 않았습니다." }), { status: 500 });
  }

  const search = url.search; // ?mode=themeDetail&date=... 등 쿼리 그대로 전달
  const target = gasUrl + search;
  try {
    const resp = await fetch(target, { cf: { cacheTtl: 0, cacheEverything: false } });
    const body = await resp.text();
    return new Response(body, {
      status: resp.status,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store'
      }
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 502 });
  }
}
