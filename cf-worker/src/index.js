export default {
  async fetch(request, env) {
    // Cloudflare Access가 이 코드에 도달하기 전에 이미 이메일 인증을 처리합니다.
    const url = new URL(request.url);
    const gasUrl = env.GAS_URL; // Cloudflare 대시보드에서 Secret으로 등록 (코드에 직접 쓰지 않음)

    if (!gasUrl) {
      return new Response(JSON.stringify({ error: "GAS_URL이 설정되지 않았습니다." }), { status: 500 });
    }

    const target = gasUrl + url.search;
    try {
      const resp = await fetch(target, { cf: { cacheTtl: 0, cacheEverything: false } });
      const body = await resp.text();
      return new Response(body, {
        status: resp.status,
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'Access-Control-Allow-Origin': '*'
        }
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: String(e) }), { status: 502 });
    }
  }
}
