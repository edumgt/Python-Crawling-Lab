"use client";
import React, { useCallback, useEffect, useState } from "react";

interface Recommendation {
  market: string;
  code: string;
  name: string;
  current_price: number;
  change_rate: number;
  per: number;
  pbr: number;
  roe: number;
  dividend_yield: number;
  market_cap: number;
  volume: number;
  recommendation_score: number;
  reason: string;
  crawled_at: string;
}

const MARKET_TABS = ["KOSPI", "KOSDAQ"];

export default function RecommendationsPanel({ refreshKey }: { refreshKey?: number }) {
  const [data, setData] = useState<Recommendation[]>([]);
  const [market, setMarket] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ limit: "10" });
      if (market) params.set("market", market);
      const res = await fetch(`/api/recommendations?${params}`);
      if (!res.ok) throw new Error("추천 종목을 불러오지 못했습니다.");
      const json = await res.json();
      setData(json.data ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "오류가 발생했습니다.");
      setData([]);
    } finally {
      setLoading(false);
    }
  }, [market]);

  useEffect(() => {
    fetchData();
  }, [fetchData, refreshKey]);

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <h2 className="text-lg font-bold text-gray-800">🎯 오늘의 추천 종목</h2>
        <div className="flex gap-2">
          <button
            onClick={() => setMarket(null)}
            className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
              !market ? "bg-blue-600 text-white border-blue-600" : "border-gray-300 text-gray-600 hover:border-blue-400"
            }`}
          >
            전체
          </button>
          {MARKET_TABS.map((m) => (
            <button
              key={m}
              onClick={() => setMarket(m)}
              className={`px-2.5 py-1 rounded-full text-xs font-medium border transition-colors ${
                market === m ? "bg-blue-600 text-white border-blue-600" : "border-gray-300 text-gray-600 hover:border-blue-400"
              }`}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      {loading && <p className="text-gray-400 text-sm">불러오는 중...</p>}
      {!loading && error && <p className="text-red-500 text-sm">{error}</p>}
      {!loading && !error && data.length === 0 && (
        <p className="text-gray-400 text-sm">
          추천 종목이 아직 없습니다. 크롤링을 완료하면 자동으로 분석됩니다.
        </p>
      )}

      {!loading && !error && data.length > 0 && (
        <ul className="divide-y divide-gray-100">
          {data.map((r, i) => {
            const isUp = r.change_rate > 0;
            const isDown = r.change_rate < 0;
            const rateColor = isUp ? "text-red-600" : isDown ? "text-blue-600" : "text-gray-500";
            const rateSign = isUp ? "▲" : isDown ? "▼" : "-";

            return (
              <li key={`${r.market}-${r.code}`} className="py-3 flex items-center gap-3">
                <span className="w-6 shrink-0 text-center font-bold text-gray-400">{i + 1}</span>
                <div className="flex-grow min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="font-semibold text-gray-900">{r.name}</span>
                    <span className="text-xs bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded border">{r.market}</span>
                    <span className="text-xs text-gray-400">{r.code}</span>
                  </div>
                  <div className="text-xs text-gray-500 mt-0.5 truncate">
                    PER {r.per?.toFixed(1) ?? "-"} · PBR {r.pbr?.toFixed(1) ?? "-"} · ROE {r.roe?.toFixed(1) ?? "-"}%
                    {r.reason ? ` · ${r.reason}` : ""}
                  </div>
                </div>
                <div className="text-right shrink-0">
                  <div className="text-sm font-semibold text-gray-900">{r.current_price?.toLocaleString()}원</div>
                  <div className={`text-xs font-medium ${rateColor}`}>
                    {rateSign} {Math.abs(r.change_rate ?? 0).toFixed(2)}%
                  </div>
                </div>
                <div className="text-right shrink-0 w-12">
                  <div className="text-sm font-bold text-blue-600">{r.recommendation_score?.toFixed(0)}</div>
                  <div className="text-[10px] text-gray-400">점수</div>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
