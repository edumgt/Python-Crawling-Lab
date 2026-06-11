import { NextRequest, NextResponse } from "next/server";

const CRAWLER_API = process.env.CRAWLER_API_URL || "http://crawler-api:8080";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const res = await fetch(`${CRAWLER_API}/crawl/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      return NextResponse.json({ error: err }, { status: res.status });
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch (err) {
    console.error("Crawler API 연결 실패:", err);
    return NextResponse.json(
      { error: "크롤러 백엔드에 연결할 수 없습니다." },
      { status: 503 }
    );
  }
}
