import { NextRequest, NextResponse } from "next/server";

const CRAWLER_API = process.env.CRAWLER_API_URL || "http://crawler-api:8080";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const market = searchParams.get("market");
  const limit = searchParams.get("limit") || "15";

  try {
    const params = new URLSearchParams({ limit });
    if (market) params.set("market", market);

    const res = await fetch(`${CRAWLER_API}/lake/recommendations?${params}`, {
      cache: "no-store",
    });

    if (!res.ok) {
      const err = await res.text();
      return NextResponse.json({ error: err }, { status: res.status });
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch (err) {
    console.error("추천 종목 API 연결 실패:", err);
    return NextResponse.json(
      { error: "크롤러 백엔드에 연결할 수 없습니다." },
      { status: 503 }
    );
  }
}
