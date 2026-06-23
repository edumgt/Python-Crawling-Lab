import { NextRequest, NextResponse } from "next/server";

const QDRANT_URL = process.env.QDRANT_URL || "http://qdrant:6333";
const COLLECTION = "korean_stocks";

interface RawPoint {
  id: string | number;
  payload: {
    source: string;
    code: string;
    name: string;
    market: string;
    current_price: number;
    change_rate: number;
    volume: number;
    trading_value: number;
    market_cap: number;
    foreign_ratio: number;
    per: number;
    pbr: number;
    eps: number;
    roe: number;
    dividend_yield: number;
    crawled_at: string;
  };
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const q = searchParams.get("q")?.trim().toLowerCase() || "";
  const page = Number(searchParams.get("page") || "1");
  const limit = Number(searchParams.get("limit") || "10");
  const market = searchParams.get("market") || "";
  const offset = (page - 1) * limit;

  try {
    const res = await fetch(`${QDRANT_URL}/collections/${COLLECTION}/points/scroll`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        limit: 2000,
        with_payload: true,
        with_vector: false,
        filter: { must: [{ key: "source", match: { value: "naver" } }] },
      }),
    });

    if (!res.ok) throw new Error(`Qdrant ${res.status}`);
    const data = await res.json();
    const allPoints: RawPoint[] = data.result?.points ?? [];

    let filtered = allPoints;
    if (q) {
      filtered = allPoints.filter(
        (p) =>
          p.payload.name.toLowerCase().includes(q) ||
          p.payload.code.includes(q)
      );
    }
    if (market) {
      filtered = filtered.filter((p) => p.payload.market === market);
    }

    filtered.sort(
      (a, b) => Math.abs(b.payload.change_rate) - Math.abs(a.payload.change_rate)
    );

    const total = filtered.length;
    const totalPages = Math.ceil(total / limit) || 1;
    const paged = filtered.slice(offset, offset + limit);

    const markets = [...new Set(allPoints.map((p) => p.payload.market))].sort();
    const stats = {
      total_count: allPoints.length,
      market_count: markets.length,
      markets,
    };

    return NextResponse.json({
      data: paged.map((p) => ({
        id: p.id,
        name: p.payload.name,
        code: p.payload.code,
        market: p.payload.market,
        current_price: p.payload.current_price,
        change_rate: p.payload.change_rate,
        volume: p.payload.volume,
        trading_value: p.payload.trading_value,
        market_cap: p.payload.market_cap,
        foreign_ratio: p.payload.foreign_ratio,
        per: p.payload.per,
        pbr: p.payload.pbr,
        eps: p.payload.eps,
        roe: p.payload.roe,
        dividend_yield: p.payload.dividend_yield,
        crawled_at: p.payload.crawled_at,
      })),
      pagination: { total, page, totalPages },
      stats,
    });
  } catch (err) {
    return NextResponse.json({ error: (err as Error).message }, { status: 502 });
  }
}
