"use client";
import React, { useEffect, useState } from "react";

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

export default function NewsPanel() {
  const [stocks, setStocks] = useState<StockNews[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/news")
      .then((r) => r.json())
      .then((data) => setStocks(Array.isArray(data) ? data : []))
      .catch(() => setStocks([]))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-gray-500 text-sm">Loading stock data...</p>;
  if (stocks.length === 0) return <p className="text-gray-400 text-sm">No stock data available.</p>;

  return (
    <div className="overflow-y-auto max-h-[80vh]">
      <ul className="space-y-3">
        {stocks.map((s) => {
          const isUp = s.change_rate > 0;
          const isDown = s.change_rate < 0;
          const rateColor = isUp ? "text-red-600" : isDown ? "text-blue-600" : "text-gray-500";
          const rateSign = isUp ? "▲" : isDown ? "▼" : "-";

          return (
            <li key={s.id} className="border-b pb-3">
              <div className="flex items-center justify-between gap-2">
                <span className="font-bold text-gray-900 text-sm">{s.name}</span>
                <span className={`text-sm font-semibold ${rateColor} whitespace-nowrap`}>
                  {rateSign} {Math.abs(s.change_rate).toFixed(2)}%
                </span>
              </div>
              <div className="flex items-center gap-2 mt-1 text-xs text-gray-500">
                <span className="bg-gray-100 px-1.5 py-0.5 rounded">{s.market}</span>
                <span>{s.code}</span>
                <span className="ml-auto font-medium text-gray-700">
                  {s.current_price.toLocaleString()}원
                </span>
              </div>
              <div className="text-xs text-gray-400 mt-1">
                거래량 {s.volume.toLocaleString()} ·{" "}
                {new Date(s.crawled_at).toLocaleString("ko-KR", {
                  month: "short",
                  day: "numeric",
                  hour: "2-digit",
                  minute: "2-digit",
                })}
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
