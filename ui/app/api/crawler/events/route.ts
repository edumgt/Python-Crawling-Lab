import { NextRequest } from "next/server";

const CRAWLER_API = process.env.CRAWLER_API_URL || "http://crawler-api:8080";

export async function GET(req: NextRequest) {
  const jobId = req.nextUrl.searchParams.get("job_id");
  if (!jobId) {
    return new Response("job_id 파라미터가 필요합니다.", { status: 400 });
  }

  const upstream = await fetch(`${CRAWLER_API}/crawl/events/${jobId}`, {
    headers: { Accept: "text/event-stream" },
  });

  if (!upstream.ok || !upstream.body) {
    return new Response("잡을 찾을 수 없습니다.", { status: 404 });
  }

  // 업스트림 SSE 스트림을 FE로 그대로 프록시
  return new Response(upstream.body, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",
    },
  });
}
