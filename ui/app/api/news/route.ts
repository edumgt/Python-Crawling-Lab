import { NextResponse } from "next/server";

const QDRANT_URL = process.env.QDRANT_URL || "http://qdrant:6333";
const COLLECTION = "korean_stocks";

interface StockPayload {
  source: string;
  code: string;
  name: string;
  market: string;
  current_price: number;
  change_rate: number;
  volume: number;
  trading_value: number;
  crawled_at: string;
}

interface StockNews {
  id: string | number;
  name: string;
  code: string;
  market: string;
  current_price: number;
  change_rate: number;
  volume: number;
  crawled_at: string;
}

export async function GET() {
  try {
    const res = await fetch(`${QDRANT_URL}/collections/${COLLECTION}/points/scroll`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        limit: 250,
        with_payload: true,
        with_vector: false,
        filter: { must: [{ key: "source", match: { value: "naver" } }] },
      }),
      next: { revalidate: 300 },
    });

    if (!res.ok) throw new Error(`Qdrant ${res.status}`);
    const data = await res.json();

    const points: { id: string | number; payload: StockPayload }[] = data.result?.points ?? [];

    const stocks: StockNews[] = points
      .map((p) => ({
        id: p.id,
        name: p.payload.name,
        code: p.payload.code,
        market: p.payload.market,
        current_price: p.payload.current_price,
        change_rate: p.payload.change_rate,
        volume: p.payload.volume,
        crawled_at: p.payload.crawled_at,
      }))
      .sort((a, b) => Math.abs(b.change_rate) - Math.abs(a.change_rate))
      .slice(0, 20);

    return NextResponse.json(stocks);
  } catch (err) {
    return NextResponse.json({ error: (err as Error).message }, { status: 502 });
  }
}
