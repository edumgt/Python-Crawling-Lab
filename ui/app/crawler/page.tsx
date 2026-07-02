"use client";

import React, { useState, useRef, useCallback } from "react";
import RecommendationsPanel from "@/components/RecommendationsPanel";

// ---------------------------------------------------------------------------
// 타입
// ---------------------------------------------------------------------------

type CrawlEvent = {
  type: string;
  source?: string;
  market?: string;
  page?: number;
  count?: number;
  total?: number;
  done?: number;
  batch?: number;
  uploaded?: number;
  total_uploaded?: number;
  duration_seconds?: number;
  message?: string;
  url?: string;
  sources?: string[];
  markets?: string[];
  max_pages?: number;
  qdrant_url?: string;
};

type JobStatus = "idle" | "running" | "completed" | "failed";

const EVENT_LABELS: Record<string, string> = {
  init: "초기화",
  qdrant_ok: "Qdrant 연결",
  start: "크롤링 시작",
  page: "페이지 수집",
  enrich: "API 보강",
  upload: "Qdrant 업로드",
  done: "소스 완료",
  complete: "전체 완료",
  gold_build_start: "추천 분석",
  gold_build_done: "추천 완료",
  error: "오류",
  stream_end: "스트림 종료",
};

const EVENT_COLORS: Record<string, string> = {
  init: "text-blue-500",
  qdrant_ok: "text-green-500",
  start: "text-indigo-500",
  page: "text-sky-500",
  enrich: "text-yellow-600",
  upload: "text-purple-500",
  done: "text-green-600",
  complete: "text-emerald-600",
  gold_build_start: "text-amber-500",
  gold_build_done: "text-teal-600",
  error: "text-red-500",
  stream_end: "text-gray-400",
};

const SOURCE_OPTIONS = ["naver", "daum", "krx"];
const MARKET_OPTIONS = ["KOSPI", "KOSDAQ"];

// ---------------------------------------------------------------------------
// 이벤트 포맷터
// ---------------------------------------------------------------------------

function formatEvent(ev: CrawlEvent): string {
  switch (ev.type) {
    case "init":
      return `소스: ${ev.sources?.join(", ")} | 시장: ${ev.markets?.join(", ")} | 최대 ${ev.max_pages}페이지`;
    case "qdrant_ok":
      return `연결 성공: ${ev.url}`;
    case "start":
      return `[${ev.source?.toUpperCase()}] ${ev.market} 크롤링 시작`;
    case "page":
      return `[${ev.source?.toUpperCase()}] ${ev.market} - ${ev.page}페이지 ${ev.count}개 수집 (누적 ${ev.total}개)`;
    case "enrich":
      return `[${ev.source?.toUpperCase()}] ${ev.market} - API 보강 ${ev.done}/${ev.total}`;
    case "upload":
      return `[${ev.source?.toUpperCase()}] ${ev.market} - Qdrant 업로드 +${ev.batch} (총 ${ev.total}개)`;
    case "done":
      return `[${ev.source?.toUpperCase()}] ${ev.market} 완료 - ${ev.uploaded}개 적재`;
    case "complete":
      return `전체 완료: ${ev.total_uploaded?.toLocaleString()}개 적재 | ${ev.duration_seconds}초`;
    case "gold_build_start":
    case "gold_build_done":
      return ev.message ?? "";
    case "error":
      return `오류 [${ev.source || ""}]: ${ev.message}`;
    case "stream_end":
      return "스트림 종료";
    default:
      return JSON.stringify(ev);
  }
}

// ---------------------------------------------------------------------------
// 메인 컴포넌트
// ---------------------------------------------------------------------------

export default function CrawlerPage() {
  const [selectedSources, setSelectedSources] = useState<string[]>(["naver", "daum", "krx"]);
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>(["KOSPI", "KOSDAQ"]);
  const [maxPages, setMaxPages] = useState(4);
  const [qdrantUrl, setQdrantUrl] = useState("http://qdrant:6333");

  const [status, setStatus] = useState<JobStatus>("idle");
  const [jobId, setJobId] = useState<string | null>(null);
  const [events, setEvents] = useState<CrawlEvent[]>([]);
  const [summary, setSummary] = useState<{ total: number; duration: number } | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [recommendationsRefreshKey, setRecommendationsRefreshKey] = useState(0);

  const esRef = useRef<EventSource | null>(null);
  const logEndRef = useRef<HTMLDivElement | null>(null);

  const scrollToBottom = useCallback(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  const toggleOption = (list: string[], item: string, setter: (v: string[]) => void) => {
    setter(list.includes(item) ? list.filter((x) => x !== item) : [...list, item]);
  };

  const startCrawl = async () => {
    setEvents([]);
    setSummary(null);
    setErrorMsg(null);
    setStatus("running");

    try {
      const res = await fetch("/api/crawler/start", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sources: selectedSources,
          markets: selectedMarkets,
          max_pages: maxPages,
          qdrant_url: qdrantUrl,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || "잡 시작 실패");
      }

      const { job_id } = await res.json();
      setJobId(job_id);

      // SSE 연결
      const es = new EventSource(`/api/crawler/events?job_id=${job_id}`);
      esRef.current = es;

      es.onmessage = (e) => {
        const ev: CrawlEvent = JSON.parse(e.data);
        setEvents((prev) => [...prev, ev]);
        scrollToBottom();

        if (ev.type === "complete") {
          setSummary({
            total: ev.total_uploaded ?? 0,
            duration: ev.duration_seconds ?? 0,
          });
        }
        if (ev.type === "stream_end") {
          setStatus(events.some((x) => x.type === "error") ? "failed" : "completed");
          es.close();
        }
        if (ev.type === "error") {
          setErrorMsg(ev.message ?? "알 수 없는 오류");
        }
        if (ev.type === "gold_build_done") {
          setRecommendationsRefreshKey((k) => k + 1);
        }
      };

      es.onerror = () => {
        setStatus("failed");
        setErrorMsg("SSE 연결이 끊어졌습니다.");
        es.close();
      };
    } catch (err: unknown) {
      setStatus("failed");
      setErrorMsg(err instanceof Error ? err.message : String(err));
    }
  };

  const stopCrawl = () => {
    esRef.current?.close();
    setStatus("idle");
  };

  const statusBadge = {
    idle: { label: "대기", cls: "bg-gray-200 text-gray-700" },
    running: { label: "실행 중", cls: "bg-blue-100 text-blue-700 animate-pulse" },
    completed: { label: "완료", cls: "bg-green-100 text-green-700" },
    failed: { label: "실패", cls: "bg-red-100 text-red-700" },
  }[status];

  return (
    <div className="max-w-4xl mx-auto py-8">
      <h1 className="text-2xl font-bold mb-1">금융 데이터 크롤러</h1>
      <p className="text-gray-500 text-sm mb-6">
        Naver Finance / Daum Finance / KRX 데이터를 수집하여 Qdrant에 적재합니다.
      </p>

      {/* 설정 패널 */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-6">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">

          {/* 소스 선택 */}
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">크롤링 소스</label>
            <div className="flex gap-2 flex-wrap">
              {SOURCE_OPTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => toggleOption(selectedSources, s, setSelectedSources)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium border transition-colors ${
                    selectedSources.includes(s)
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white text-gray-600 border-gray-300 hover:border-blue-400"
                  }`}
                >
                  {s.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          {/* 시장 선택 */}
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">대상 시장</label>
            <div className="flex gap-2 flex-wrap">
              {MARKET_OPTIONS.map((m) => (
                <button
                  key={m}
                  onClick={() => toggleOption(selectedMarkets, m, setSelectedMarkets)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium border transition-colors ${
                    selectedMarkets.includes(m)
                      ? "bg-indigo-600 text-white border-indigo-600"
                      : "bg-white text-gray-600 border-gray-300 hover:border-indigo-400"
                  }`}
                >
                  {m}
                </button>
              ))}
            </div>
          </div>

          {/* 최대 페이지 */}
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              최대 페이지 수 <span className="text-gray-400 font-normal">(페이지당 ~50종목)</span>
            </label>
            <input
              type="number"
              min={1}
              max={50}
              value={maxPages}
              onChange={(e) => setMaxPages(Number(e.target.value))}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-24 focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>

          {/* Qdrant URL */}
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Qdrant URL</label>
            <input
              type="text"
              value={qdrantUrl}
              onChange={(e) => setQdrantUrl(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-full focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>
        </div>

        {/* 실행 버튼 */}
        <div className="mt-6 flex items-center gap-4">
          <button
            onClick={startCrawl}
            disabled={status === "running" || selectedSources.length === 0 || selectedMarkets.length === 0}
            className="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            크롤링 시작
          </button>
          {status === "running" && (
            <button
              onClick={stopCrawl}
              className="px-4 py-2.5 border border-gray-300 text-gray-600 rounded-lg text-sm hover:bg-gray-50 transition-colors"
            >
              중단
            </button>
          )}
          <span className={`px-3 py-1 rounded-full text-xs font-semibold ${statusBadge.cls}`}>
            {statusBadge.label}
          </span>
          {jobId && (
            <span className="text-xs text-gray-400 font-mono">job: {jobId.slice(0, 8)}…</span>
          )}
        </div>
      </div>

      {/* 요약 카드 */}
      {summary && (
        <div className="bg-emerald-50 border border-emerald-200 rounded-xl p-5 mb-6 flex items-center gap-6">
          <div>
            <div className="text-3xl font-bold text-emerald-700">
              {summary.total.toLocaleString()}
            </div>
            <div className="text-sm text-emerald-600">종목 적재 완료</div>
          </div>
          <div>
            <div className="text-xl font-semibold text-emerald-700">{summary.duration}초</div>
            <div className="text-sm text-emerald-600">소요 시간</div>
          </div>
        </div>
      )}

      {/* 오류 */}
      {errorMsg && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 mb-6 text-red-700 text-sm">
          오류: {errorMsg}
        </div>
      )}

      {/* 실시간 로그 */}
      {events.length > 0 && (
        <div className="bg-gray-900 rounded-xl border border-gray-700 overflow-hidden">
          <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-700">
            <span className="text-gray-300 text-sm font-semibold">실시간 로그</span>
            <span className="text-gray-500 text-xs">{events.length}개 이벤트</span>
          </div>
          <div className="p-4 h-96 overflow-y-auto font-mono text-xs space-y-1">
            {events.map((ev, i) => (
              <div key={i} className="flex gap-3">
                <span className="text-gray-500 shrink-0 w-16 text-right">
                  {EVENT_LABELS[ev.type] ?? ev.type}
                </span>
                <span className={EVENT_COLORS[ev.type] ?? "text-gray-300"}>
                  {formatEvent(ev)}
                </span>
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        </div>
      )}

      {/* 추천 종목 */}
      <div className="mt-6">
        <RecommendationsPanel refreshKey={recommendationsRefreshKey} />
      </div>
    </div>
  );
}
