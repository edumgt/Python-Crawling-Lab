"use client";
import React, { Suspense, useState, useEffect, useMemo } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import NewsPanel from "@/components/NewsPanel";

interface StockRecord {
  id: string | number;
  name: string;
  code: string;
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
}

interface Pagination {
  total: number;
  page: number;
  totalPages: number;
}

interface Stats {
  total_count: number;
  market_count: number;
  markets: string[];
}

export default function StocksPage() {
  return (
    <Suspense fallback={<div className="min-h-screen flex items-center justify-center text-gray-400">Loading...</div>}>
      <StocksPageContent />
    </Suspense>
  );
}

function StocksPageContent() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<StockRecord[]>([]);
  const [pagination, setPagination] = useState<Pagination | null>(null);
  const [stats, setStats] = useState<Stats | null>(null);
  const [page, setPage] = useState(1);
  const [selectedMarket, setSelectedMarket] = useState<string | null>(null);
  const [selectedRecord, setSelectedRecord] = useState<StockRecord | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [lastSearchKeyword, setLastSearchKeyword] = useState("");
  const LIMIT = 10;
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    fetchStats();
  }, []);

  const fetchStats = async () => {
    try {
      const res = await fetch("/api/stocks");
      if (!res.ok) return;
      const data = await res.json();
      setStats(data.stats ?? null);
    } catch {}
  };

  const fetchData = async (pageNum = 1) => {
    if (query.trim().length < 1 && !selectedMarket) {
      setToast("검색어를 입력하거나 시장을 선택해 주세요.");
      return;
    }
    setLoading(true);
    try {
      const params = new URLSearchParams({
        page: String(pageNum),
        limit: String(LIMIT),
      });
      if (query.trim()) params.set("q", query.trim());
      if (selectedMarket) params.set("market", selectedMarket);

      const res = await fetch(`/api/stocks?${params}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      setResults(data.data ?? []);
      setPagination(data.pagination ?? null);
      setStats(data.stats ?? null);
      setSearched(true);
      setSelectedRecord(null);
      setPage(pageNum);
      setLastSearchKeyword(query);
    } catch (err) {
      console.error("❌ Fetch error:", err);
      setToast("데이터를 불러오는 중 오류가 발생했습니다.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const id = searchParams.get("id");
    if (!id) {
      setSelectedRecord(null);
    } else {
      const found = results.find((r) => String(r.id) === id);
      if (found) setSelectedRecord(found);
    }
  }, [searchParams, results]);

  useEffect(() => {
    if (searched) fetchData(page);
  }, [page]);

  useEffect(() => {
    if (toast) {
      const t = setTimeout(() => setToast(null), 2000);
      return () => clearTimeout(t);
    }
  }, [toast]);

  const marketCounts = useMemo(() => {
    const acc: Record<string, number> = {};
    results.forEach((r) => {
      acc[r.market] = (acc[r.market] || 0) + 1;
    });
    return Object.entries(acc).sort((a, b) => b[1] - a[1]);
  }, [results]);

  const filteredResults = useMemo(() => {
    if (!selectedMarket) return results;
    return results.filter((r) => r.market === selectedMarket);
  }, [results, selectedMarket]);

  const renderRate = (rate: number) => {
    const color = rate > 0 ? "text-red-600" : rate < 0 ? "text-blue-600" : "text-gray-500";
    const sign = rate > 0 ? "▲" : rate < 0 ? "▼" : "-";
    return (
      <span className={`font-semibold ${color}`}>
        {sign} {Math.abs(rate).toFixed(2)}%
      </span>
    );
  };

  const renderPagination = () => {
    if (!pagination) return null;
    const { page: current, totalPages } = pagination;
    const safeTotal = Math.max(totalPages, 1);
    const maxVisible = 3;
    const pages: number[] = [];
    let start = Math.max(1, current - Math.floor(maxVisible / 2));
    let end = Math.min(safeTotal, start + maxVisible - 1);
    if (end - start < maxVisible - 1) start = Math.max(1, end - maxVisible + 1);
    for (let i = start; i <= end; i++) pages.push(i);

    return (
      <div className="flex justify-center items-center gap-2 mt-6">
        <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={current <= 1 || loading} className="px-3 py-1 border rounded disabled:opacity-40">◀ 이전</button>
        {start > 1 && (<><button onClick={() => setPage(1)} className={`px-3 py-1 border rounded ${current === 1 ? "bg-blue-600 text-white" : "hover:bg-blue-50"}`}>1</button>{start > 2 && <span className="px-2 text-gray-500">…</span>}</>)}
        {pages.map((num) => (<button key={num} onClick={() => setPage(num)} disabled={num === current} className={`px-3 py-1 border rounded ${num === current ? "bg-blue-600 text-white" : "hover:bg-blue-50"}`}>{num}</button>))}
        {end < safeTotal && (<>{end < safeTotal - 1 && <span className="px-2 text-gray-500">…</span>}<button onClick={() => setPage(safeTotal)} className={`px-3 py-1 border rounded ${current === safeTotal ? "bg-blue-600 text-white" : "hover:bg-blue-50"}`}>{safeTotal}</button></>)}
        <button onClick={() => setPage((p) => Math.min(safeTotal, p + 1))} disabled={current >= safeTotal || loading} className="px-3 py-1 border rounded disabled:opacity-40">다음 ▶</button>
      </div>
    );
  };

  const fmt = (v: number) => v?.toLocaleString() ?? "-";

  return (
    <main className="min-h-screen flex flex-col bg-white relative">
      {toast && (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 bg-red-500 text-white px-6 py-2 rounded shadow-md z-50">
          {toast}
        </div>
      )}

      {/* 검색 영역 */}
      <section className="bg-[#2156d4] py-6 text-center relative z-30">
        <h1 className="text-white text-2xl font-bold mb-4">한국 주식 검색</h1>
        <div className="flex justify-center px-4">
          <div className="flex w-full max-w-2xl bg-white rounded-md shadow-md overflow-hidden">
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && fetchData(1)}
              placeholder="종목명 또는 종목코드 검색..."
              className="flex-grow px-3 py-2 text-gray-900 font-semibold focus:outline-none"
            />
            <button
              onClick={() => fetchData(1)}
              disabled={loading}
              className="bg-green-600 hover:bg-green-700 text-white px-6 font-semibold"
            >
              {loading ? "검색 중..." : "Search"}
            </button>
          </div>
        </div>
      </section>

      {/* 현황판 */}
      <div className="bg-white shadow border-t border-gray-200 py-3 text-center">
        {stats ? (
          <div className="flex justify-center gap-8 text-gray-800 font-semibold">
            <span>- 종목 수: <span className="text-blue-600">{fmt(stats.total_count)}</span></span>
            <span>- 시장 수: <span className="text-green-700">{fmt(stats.market_count)}</span></span>
          </div>
        ) : (
          <div className="text-gray-400 text-sm">통계 정보를 불러오는 중...</div>
        )}
      </div>

      <div className="flex flex-col md:flex-row flex-grow">
        {/* 왼쪽: 목록 or 상세 */}
        <div className="w-full md:w-3/5 p-6 overflow-y-auto">
          {!searched ? (
            <div className="text-center text-gray-400 py-10">
              🔍 종목명이나 코드를 입력하고 "Search" 버튼을 눌러주세요.
            </div>
          ) : selectedRecord ? (
            <>
              <button onClick={() => { setSelectedRecord(null); router.push("/", { scroll: false }); }} className="mb-4 text-blue-600 hover:underline text-sm">← 목록으로</button>
              <div className="flex items-center gap-3 mb-4">
                <h2 className="text-2xl font-bold">{selectedRecord.name}</h2>
                <span className="bg-gray-100 text-gray-700 text-sm px-2 py-0.5 rounded border">{selectedRecord.market}</span>
                <span className="text-gray-500 text-sm">{selectedRecord.code}</span>
              </div>
              <div className="mb-4 text-3xl font-bold">
                {fmt(selectedRecord.current_price)}원 {renderRate(selectedRecord.change_rate)}
              </div>
              <table className="w-full text-sm border-t border-gray-200">
                <tbody>
                  {[
                    ["거래량", fmt(selectedRecord.volume)],
                    ["거래대금 (백만)", fmt(selectedRecord.trading_value)],
                    ["시가총액 (억)", fmt(selectedRecord.market_cap)],
                    ["외국인비율", selectedRecord.foreign_ratio ? `${selectedRecord.foreign_ratio.toFixed(2)}%` : "-"],
                    ["PER", selectedRecord.per || "-"],
                    ["PBR", selectedRecord.pbr || "-"],
                    ["EPS", selectedRecord.eps ? fmt(selectedRecord.eps) : "-"],
                    ["ROE", selectedRecord.roe ? `${selectedRecord.roe.toFixed(2)}%` : "-"],
                    ["배당수익률", selectedRecord.dividend_yield ? `${selectedRecord.dividend_yield.toFixed(2)}%` : "-"],
                    ["수집일시", new Date(selectedRecord.crawled_at).toLocaleString("ko-KR")],
                  ].map(([label, value]) => (
                    <tr key={label as string} className="border-b">
                      <td className="py-2 font-medium w-36 text-gray-600">{label}</td>
                      <td className="py-2 text-gray-800">{value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : (
            <>
              {filteredResults.length === 0 && !loading && (
                <p className="text-center text-red-500 font-medium">검색 결과가 없습니다.</p>
              )}
              {searched && !selectedRecord && (
                <div className="mb-5 p-4 bg-blue-50 border border-blue-200 rounded-md shadow-sm">
                  <div className="text-lg font-bold text-gray-800">
                    {lastSearchKeyword && <><span className="text-blue-700 text-xl">"{lastSearchKeyword}"</span>{" "}</>}
                    <span className="text-gray-800">검색 결과</span>{" "}
                    <span className="text-green-700 text-xl">{pagination?.total?.toLocaleString() || 0}</span>건
                  </div>
                </div>
              )}
              <ul className="divide-y divide-gray-200">
                {filteredResults.map((r) => (
                  <li
                    key={r.id}
                    onClick={() => { setSelectedRecord(r); router.push(`?id=${r.id}`, { scroll: false }); }}
                    className="py-4 px-2 hover:bg-gray-50 cursor-pointer transition"
                  >
                    <div className="flex justify-between items-start">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-bold text-blue-700 text-base">{r.name}</span>
                        <span className="text-xs bg-gray-100 text-gray-700 px-2 py-0.5 rounded border">{r.market}</span>
                        <span className="text-xs text-gray-500">{r.code}</span>
                      </div>
                      <div className="text-right whitespace-nowrap">
                        <div className="font-semibold text-gray-900">{fmt(r.current_price)}원</div>
                        <div className="text-sm">{renderRate(r.change_rate)}</div>
                      </div>
                    </div>
                    <div className="mt-1 text-xs text-gray-500">
                      거래량 {fmt(r.volume)} · 시총 {fmt(r.market_cap)}억
                    </div>
                  </li>
                ))}
              </ul>
              {renderPagination()}
            </>
          )}
        </div>

        {/* 오른쪽: 시장 필터 + 주식 뉴스 */}
        <aside className="w-full md:w-2/5 border-l border-gray-200 p-6 bg-gray-50">
          {!selectedRecord && (
            <>
              <h2 className="text-lg font-bold mb-4 text-gray-800">시장별</h2>
              <div className="space-y-2 overflow-y-auto max-h-[30vh] pr-2 mb-4">
                {marketCounts.length === 0 ? (
                  <p className="text-gray-400 text-sm">검색 후 표시됩니다.</p>
                ) : (
                  marketCounts.map(([market, count]) => (
                    <button
                      key={market}
                      onClick={() => setSelectedMarket(selectedMarket === market ? null : market)}
                      className={`w-full flex justify-between items-center text-left px-3 py-2 rounded-md transition ${
                        selectedMarket === market ? "bg-blue-600 text-white" : "bg-white hover:bg-blue-50 text-gray-800"
                      }`}
                    >
                      <span className="font-medium">{market}</span>
                      <span className="text-sm opacity-70">{count}</span>
                    </button>
                  ))
                )}
              </div>
              <div className="my-4 border-t border-gray-300" />
            </>
          )}

          <h2 className="text-lg font-bold mb-3 text-gray-800">📈 주요 등락 종목</h2>
          <NewsPanel />
        </aside>
      </div>
    </main>
  );
}
