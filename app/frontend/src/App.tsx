import { CSSProperties, FormEvent, PointerEvent as ReactPointerEvent, useEffect, useMemo, useRef, useState } from "react";
import type {
  ApiErrorResponse,
  GameSummary,
  GraphApiResponse,
  GraphNode,
  GraphPayload,
  NlParseMeta,
  QueryPresetId,
  RecommendRequestBody,
  SearchMeta,
  SearchSortField
} from "../../shared/contracts";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:4000";

interface RecommendFilters {
  keyword: string;
  players: string;
  maxTime: string;
  maxPrice: string;
  minRating: string;
  preset: string;
  sort: SearchSortField | "";
}

interface Position {
  x: number;
  y: number;
  scale: number;
  opacity: number;
}

type NodeShape = "center" | "primary" | "secondary";

interface LayoutNode extends Position {
  shape: NodeShape;
  primaryId?: string;
}

function getCenterNode(graph: GraphPayload): GraphNode | null {
  return graph.nodes.find((node) => node.kind === "center") || graph.nodes[0] || null;
}

function applySearchMetaToGraph(graph: GraphPayload, searchMeta: SearchMeta | null | undefined): GraphPayload {
  if (!searchMeta?.topHit) return graph;
  const { game, explain } = searchMeta.topHit;
  return {
    ...graph,
    nodes: graph.nodes.map((n) => {
      if (n.id !== game.id || n.kind !== "center") return n;
      const fromExplain = explain?.meanPrice;
      const mergedEst =
        fromExplain != null && Number.isFinite(fromExplain)
          ? fromExplain
          : game.estimatedPrice != null && Number.isFinite(game.estimatedPrice)
            ? game.estimatedPrice
            : n.estimatedPrice;
      return {
        ...n,
        ...game,
        searchExplain: explain,
        estimatedPrice: mergedEst != null && Number.isFinite(mergedEst) ? mergedEst : n.estimatedPrice ?? null
      };
    })
  };
}

const demoCatalog: Array<Pick<GameSummary, "id" | "name" | "rating" | "usersRated">> = [
  { id: "demo-1", name: "Suggestible Spire", rating: 8.9, usersRated: 1243 },
  { id: "demo-2", name: "Pastel Portals", rating: 8.2, usersRated: 934 },
  { id: "demo-3", name: "Starlit Market", rating: 8.1, usersRated: 802 },
  { id: "demo-4", name: "Cloud Harbor", rating: 7.9, usersRated: 710 },
  { id: "demo-5", name: "Moss & Monuments", rating: 7.8, usersRated: 602 },
  { id: "demo-6", name: "Velvet Vale", rating: 7.7, usersRated: 581 }
];

function buildDemoGraph(centerId = demoCatalog[0].id): GraphPayload {
  const centerIndex = demoCatalog.findIndex((node) => node.id === centerId);
  const safeIndex = centerIndex >= 0 ? centerIndex : 0;
  const ordered = [
    demoCatalog[safeIndex],
    ...demoCatalog.slice(safeIndex + 1),
    ...demoCatalog.slice(0, safeIndex)
  ].slice(0, 6);

  const center: GraphNode = { ...ordered[0], kind: "center" };
  const neighbors: GraphNode[] = ordered.slice(1).map((node, index) => ({
    ...node,
    kind: "neighbor",
    similarity: 0.95 - index * 0.05
  }));

  return {
    centerId: center.id,
    neighborMode: "similarity",
    nodes: [center, ...neighbors],
    edges: neighbors.map((neighbor) => ({
      id: `${center.id}->${neighbor.id}`,
      source: center.id,
      target: neighbor.id,
      weight: neighbor.similarity ?? 0
    }))
  };
}

const defaultFilters: RecommendFilters = {
  keyword: "",
  players: "",
  maxTime: "",
  maxPrice: "",
  minRating: "",
  preset: "",
  sort: ""
};

const PRESET_CHIPS: { id: QueryPresetId; label: string }[] = [
  { id: "best_under_budget", label: "Best under $30" },
  { id: "value_for_price", label: "Value for price" },
  { id: "highly_rated_cheap", label: "Highly rated + cheap" },
  { id: "undervalued", label: "Undervalued" },
  { id: "overpriced", label: "Overpriced" },
  { id: "high_want_low_own", label: "High want, low own" },
  { id: "frequently_traded", label: "Frequently traded" },
  { id: "rating_per_dollar", label: "Rating / $ " },
  { id: "composite_demo", label: "Smart demo" }
];

const SORT_OPTIONS: { value: SearchSortField; label: string }[] = [
  { value: "rating", label: "Geek rating" },
  { value: "mean_price", label: "Price (latest mean)" },
  { value: "rating_per_dollar", label: "Rating per $" },
  { value: "rank_value_asc", label: "BGG rank" },
  { value: "want_minus_own", label: "Wants minus owns" },
  { value: "wtt", label: "Want-to-trade count" },
  { value: "wants", label: "Want count" },
  { value: "value_score", label: "Value score (on graph)" },
  { value: "price_drop", label: "Price drop vs window" }
];

function toRequestFilters(filters: RecommendFilters): RecommendRequestBody["filters"] {
  return {
    keyword: filters.keyword || undefined,
    players: filters.players ? Number(filters.players) : null,
    maxTime: filters.maxTime ? Number(filters.maxTime) : null,
    maxPrice: filters.maxPrice ? Number(filters.maxPrice) : null,
    minRating: filters.minRating ? Number(filters.minRating) : null,
    preset: (filters.preset as QueryPresetId) || null,
    sort: filters.sort || undefined
  };
}

function formatStat(value: string | number | null | undefined, suffix = ""): string {
  if (value == null || value === "") return "-";
  return `${value}${suffix}`;
}

function formatPlayTimeBlock(g: GameSummary): string {
  if (g.minPlaytime != null || g.maxPlaytime != null) {
    const a = g.minPlaytime ?? g.maxPlaytime;
    const b = g.maxPlaytime ?? g.minPlaytime;
    if (a != null && b != null && a !== b) return `${a}–${b} min`;
    if (a != null) return `${a} min`;
    return "-";
  }
  return formatStat(g.playTime, " min");
}

function formatBool(v: boolean | null | undefined): string {
  if (v === true) return "Yes";
  if (v === false) return "No";
  return "—";
}

function formatEstPrice(g: GameSummary): string {
  const p = g.searchExplain?.meanPrice ?? g.estimatedPrice;
  if (p == null || !Number.isFinite(p)) return "—";
  return `$${p.toFixed(2)}`;
}

/** Latest mean price for display: search explain, then graph est. (same as formatEstPrice, number). */
function coalesceEstPriceNumber(g: GameSummary): number | null {
  const p = g.searchExplain?.meanPrice ?? g.estimatedPrice;
  if (p == null || !Number.isFinite(p)) return null;
  return p;
}

function estPriceLabelSuffix(g: GameSummary): string {
  const p = coalesceEstPriceNumber(g);
  if (p == null) return "";
  return ` · $${p.toFixed(0)}`;
}

/** Orbit line under name: search rank vs profile-similarity % (browse graph). */
function neighborOrbitCaption(graph: GraphPayload, node: GraphNode): string {
  if (graph.neighborMode === "search_hits" && node.queryResultRank != null && node.queryResultRank >= 2) {
    return `#${node.queryResultRank} in results`;
  }
  return `${Math.round((node.similarity ?? 0) * 100)}% match`;
}

function stableUnit(id: string, salt = 0): number {
  let hash = 2166136261 ^ salt;

  for (let index = 0; index < id.length; index += 1) {
    hash ^= id.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0) / 4294967295;
}

function asApiError(payload: unknown): ApiErrorResponse {
  if (typeof payload === "object" && payload !== null && "error" in payload) {
    return payload as ApiErrorResponse;
  }
  return {};
}

interface GraphCanvasProps {
  graph: GraphPayload;
  activeNodeId: string;
  onNodeClick: (node: GraphNode) => void;
  /** When set, center node shows this BGG id (prompt reference game) instead of "Recommended". */
  promptAnchorBggId?: string | null;
}

function GraphCanvas({ graph, activeNodeId, onNodeClick, promptAnchorBggId = null }: GraphCanvasProps) {
  const [focusOffset, setFocusOffset] = useState<Position>({ x: 0, y: 0, scale: 1, opacity: 1 });
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const suppressClickUntilRef = useRef<number>(0);
  const slideTimerRef = useRef<number | null>(null);
  const dragStateRef = useRef<{
    pointerId: number;
    startX: number;
    startY: number;
    originX: number;
    originY: number;
    moved: boolean;
  } | null>(null);

  const center = graph.nodes.find((node) => node.kind === "center") || graph.nodes[0];
  useEffect(() => {
    if (slideTimerRef.current != null) {
      window.clearTimeout(slideTimerRef.current);
      slideTimerRef.current = null;
    }

    setFocusOffset({ x: 0, y: 0, scale: 1, opacity: 1 });
    setDragOffset({ x: 0, y: 0 });
  }, [graph.centerId]);

  if (!center) {
    return <div className="graph-shell" />;
  }

  const neighbors = graph.nodes
    .filter((node) => node.id !== center.id)
    .sort((left, right) => {
      if (graph.neighborMode === "search_hits") {
        return (left.queryResultRank ?? 0) - (right.queryResultRank ?? 0);
      }
      return (right.similarity ?? 0) - (left.similarity ?? 0);
    });
  const layout = useMemo(() => {
    interface MutableOrbitNode {
      id: string;
      x: number;
      y: number;
      anchorX: number;
      anchorY: number;
      shape: NodeShape;
      primaryId?: string;
      radius: number;
    }

    const positions = new Map<string, LayoutNode>();
    const primaryCount = Math.min(6, neighbors.length);
    const primaryNodes = neighbors.slice(0, primaryCount);
    const secondaryGroups = neighbors.slice(primaryCount);
    const primaryAngles = new Map<string, number>();
    const secondaryCounts = new Map<string, number>();
    const orbitNodes: MutableOrbitNode[] = [];
    const baseRotation = stableUnit(center.id, 11) * Math.PI * 2;
    const centerRadius = 104;
    const orbitLimitX = 520;
    const orbitLimitY = 420;

    positions.set(center.id, { x: 0, y: 0, scale: 1.42, opacity: 1, shape: "center" });

    primaryNodes.forEach((node, index) => {
      const seed = stableUnit(node.id, 17);
      const angle = baseRotation + (Math.PI * 2 * index) / Math.max(primaryCount, 1) + (seed - 0.5) * 0.24;
      const baseRadius = 286 + seed * 34;
      const slot = {
        x: Math.cos(angle) * baseRadius,
        y: Math.sin(angle) * (baseRadius * 0.97)
      };

      primaryAngles.set(node.id, angle);

      orbitNodes.push({
        id: node.id,
        x: slot.x,
        y: slot.y,
        anchorX: slot.x,
        anchorY: slot.y,
        shape: "primary",
        radius: 74
      });
    });

    secondaryGroups.forEach((node, index) => {
      if (!primaryNodes.length) return;

      const primaryId = primaryNodes[index % primaryNodes.length].id;
      const localIndex = secondaryCounts.get(primaryId) ?? 0;
      secondaryCounts.set(primaryId, localIndex + 1);

      const anchorAngle = primaryAngles.get(primaryId) ?? 0;
      const seedA = stableUnit(node.id, 23);
      const seedB = stableUnit(node.id, 29);
      const seedC = stableUnit(node.id, 37);
      const seedD = stableUnit(node.id, 41);
      const seedE = stableUnit(node.id, 43);
      const tier = Math.floor(localIndex / 5);
      const goldenAngle = 2.399963229728653;
      const localTwist = Math.sin((localIndex + 1) * (1.7 + seedD) + seedE * Math.PI) * 0.42;
      const localAngle = localIndex * goldenAngle + seedA * Math.PI * 2 + (seedC - 0.5) * 1.5 + localTwist;
      const localRadius = 74 + Math.sqrt(localIndex + 1) * (34 + seedD * 18) + tier * (12 + seedE * 8) + seedC * 24;
      const clusterRingRadius = 388 + tier * (64 + seedA * 26) + seedB * 90 + (seedD - 0.5) * 48;
      const clusterAngle = anchorAngle + (seedE - 0.5) * 0.72 + Math.sin((localIndex + 1) * 0.91 + seedC * 4.8) * 0.3;
      const clusterX = Math.cos(clusterAngle) * clusterRingRadius;
      const clusterY = Math.sin(clusterAngle) * clusterRingRadius;
      const jitterX = (seedA - 0.5) * 34;
      const jitterY = (seedB - 0.5) * 28;
      const x = clusterX + Math.cos(localAngle) * localRadius + jitterX;
      const y = clusterY + Math.sin(localAngle) * localRadius * 0.96 + jitterY;

      orbitNodes.push({
        id: node.id,
        x,
        y,
        anchorX: x,
        anchorY: y,
        shape: "secondary",
        radius: 13,
        primaryId
      });
    });

    for (let iteration = 0; iteration < 130; iteration += 1) {
      for (let leftIndex = 0; leftIndex < orbitNodes.length; leftIndex += 1) {
        const left = orbitNodes[leftIndex];

        for (let rightIndex = leftIndex + 1; rightIndex < orbitNodes.length; rightIndex += 1) {
          const right = orbitNodes[rightIndex];
          const dx = right.x - left.x;
          const dy = right.y - left.y;
          const distance = Math.max(Math.sqrt(dx * dx + dy * dy), 0.001);
          const gap = left.shape === "secondary" && right.shape === "secondary" ? 18 : 28;
          const minDistance = left.radius + right.radius + gap;

          if (distance >= minDistance) continue;

          const push = (minDistance - distance) * 0.5;
          const ux = dx / distance;
          const uy = dy / distance;

          left.x -= ux * push;
          left.y -= uy * push;
          right.x += ux * push;
          right.y += uy * push;
        }
      }

      orbitNodes.forEach((node) => {
        const centerDistance = Math.max(Math.sqrt(node.x * node.x + node.y * node.y), 0.001);
        const minCenterDistance = centerRadius + node.radius + 18;

        if (centerDistance < minCenterDistance) {
          const push = minCenterDistance - centerDistance;
          node.x += (node.x / centerDistance) * push;
          node.y += (node.y / centerDistance) * push;
        }

        const spring = node.shape === "primary" ? 0.12 : 0.06;
        node.x += (node.anchorX - node.x) * spring;
        node.y += (node.anchorY - node.y) * spring;

        const boundary = Math.sqrt((node.x * node.x) / (orbitLimitX * orbitLimitX) + (node.y * node.y) / (orbitLimitY * orbitLimitY));
        if (boundary > 1) {
          node.x /= boundary;
          node.y /= boundary;
        }
      });
    }

    orbitNodes.forEach((node) => {
      positions.set(node.id, {
        x: node.x,
        y: node.y,
        scale: node.shape === "primary" ? (activeNodeId === node.id ? 1.12 : 1.02) : activeNodeId === node.id ? 0.76 : 0.48,
        opacity: node.shape === "primary" ? 1 : 0.92,
        shape: node.shape,
        primaryId: node.primaryId
      });
    });

    return positions;
  }, [activeNodeId, center.id, graph.neighborMode, neighbors]);

  function rotateAndSelectNode(node: GraphNode): void {
    if (Date.now() < suppressClickUntilRef.current) return;
    const nodePosition = layout.get(node.id);
    if (!nodePosition) {
      onNodeClick(node);
      return;
    }

    if (slideTimerRef.current != null) {
      window.clearTimeout(slideTimerRef.current);
    }

    const targetPan = {
      x: -nodePosition.x * 0.72,
      y: -nodePosition.y * 0.72,
      scale: 1,
      opacity: 1
    };

    setFocusOffset(targetPan);

    slideTimerRef.current = window.setTimeout(() => {
      slideTimerRef.current = null;
      onNodeClick(node);
    }, 520);
  }

  function nodeRadius(shape: NodeShape): number {
    if (shape === "center") return 102;
    if (shape === "primary") return 72;
    return 10;
  }

  function lineEndpoints(source: LayoutNode, target: LayoutNode): { x1: number; y1: number; x2: number; y2: number } {
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const distance = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
    const sourceRadius = nodeRadius(source.shape);
    const targetRadius = nodeRadius(target.shape);
    const ux = dx / distance;
    const uy = dy / distance;

    return {
      x1: 600 + source.x + ux * sourceRadius,
      y1: 450 + source.y + uy * sourceRadius,
      x2: 600 + target.x - ux * targetRadius,
      y2: 450 + target.y - uy * targetRadius
    };
  }

  function estSuffix(n: GraphNode): string {
    const p = n.searchExplain?.meanPrice ?? n.estimatedPrice;
    if (p == null || !Number.isFinite(p)) return "";
    return ` · est. $${p.toFixed(0)}`;
  }

  function nodeTooltip(node: GraphNode, position: LayoutNode): string {
    if (position.shape === "secondary") {
      const sub =
        graph.neighborMode === "search_hits" && node.queryResultRank != null
          ? ` #${node.queryResultRank} in results`
          : " outer related node";
      return `${node.name} |${sub}${estSuffix(node)}`;
    }

    if (position.shape === "primary") {
      return `${node.name} | ${neighborOrbitCaption(graph, node)}${estSuffix(node)}`;
    }

    return `${node.name} | center node${estSuffix(node)}`;
  }

  function beginDrag(event: ReactPointerEvent<HTMLDivElement>): void {
    if (event.button !== 0) return;
    const target = event.target as HTMLElement | null;
    if (target?.closest(".graph-node")) return;

    dragStateRef.current = {
      pointerId: event.pointerId,
      startX: event.clientX,
      startY: event.clientY,
      originX: dragOffset.x,
      originY: dragOffset.y,
      moved: false
    };

    event.currentTarget.setPointerCapture(event.pointerId);
  }

  function moveDrag(event: ReactPointerEvent<HTMLDivElement>): void {
    const dragState = dragStateRef.current;
    if (!dragState || dragState.pointerId !== event.pointerId) return;

    const deltaX = event.clientX - dragState.startX;
    const deltaY = event.clientY - dragState.startY;

    if (Math.abs(deltaX) + Math.abs(deltaY) > 6) {
      dragState.moved = true;
    }

    if (dragState.moved) {
      setDragOffset({
        x: dragState.originX + deltaX,
        y: dragState.originY + deltaY
      });
    }
  }

  function endDrag(event: ReactPointerEvent<HTMLDivElement>): void {
    const dragState = dragStateRef.current;
    if (!dragState || dragState.pointerId !== event.pointerId) return;

    if (dragState.moved) {
      suppressClickUntilRef.current = Date.now() + 180;
    }

    dragStateRef.current = null;
    event.currentTarget.releasePointerCapture(event.pointerId);
  }

  return (
    <div className="graph-shell">
      <div className="graph-orbit" onPointerDown={beginDrag} onPointerMove={moveDrag} onPointerUp={endDrag} onPointerCancel={endDrag}>
        <div className="graph-stage" style={{ transform: `translate3d(${dragOffset.x + focusOffset.x}px, ${dragOffset.y + focusOffset.y}px, 0)` }}>
          <svg className="graph-lines" viewBox="0 0 1200 900" aria-hidden="true">
            {graph.nodes
              .filter((node) => node.kind !== "center")
              .map((node) => {
                const target = layout.get(node.id);
                if (!target) return null;

                const parentId = target.shape === "primary" ? center.id : target.primaryId;
                const source = parentId ? layout.get(parentId) : null;
                if (!source) return null;

                const endpoints = lineEndpoints(source, target);

                return (
                  <line
                    key={`line-${node.id}`}
                    x1={endpoints.x1}
                    y1={endpoints.y1}
                    x2={endpoints.x2}
                    y2={endpoints.y2}
                    stroke="rgba(122, 156, 255, 0.34)"
                    strokeWidth={2}
                    strokeLinecap="round"
                  />
                );
              })}
          </svg>

          {graph.nodes.map((node) => {
            const position = layout.get(node.id);
            if (!position) return null;

            const isCenter = node.kind === "center";
            const isActive = node.id === activeNodeId;
            const motionSeed = stableUnit(node.id, 31);
            const motionStyle: CSSProperties = {
              transform: `translate(-50%, -50%) translate3d(${position.x}px, ${position.y}px, 0) scale(${position.scale})`,
              opacity: position.opacity,
              zIndex: Math.round(500 - Math.abs(position.y)),
              ["--bob-duration" as string]: `${Math.round(3600 + motionSeed * 2200)}ms`,
              ["--bob-delay" as string]: `-${Math.round(motionSeed * 1800)}ms`,
              ["--bob-range" as string]: `${position.shape === "secondary" ? 6 : position.shape === "primary" ? 4 : 2}px`
            };

            return (
              <button
                key={node.id}
                className={`graph-node ${position.shape} ${isActive ? "active" : ""}`}
                title={nodeTooltip(node, position)}
                aria-label={nodeTooltip(node, position)}
                style={motionStyle}
                onClick={() => rotateAndSelectNode(node)}
                type="button"
              >
                <span className="node-inner">
                  <span className="node-glow" />
                  {isCenter || position.shape === "primary" ? <span className="node-name">{node.name}</span> : <span className="node-dot" aria-hidden="true" />}
                  {isCenter || position.shape === "primary" ? (
                    <span className="node-meta">
                      {isCenter
                        ? promptAnchorBggId
                          ? `BGG ${promptAnchorBggId}`
                          : graph.neighborMode === "search_hits"
                            ? "Top result"
                            : "Recommended"
                        : neighborOrbitCaption(graph, node)}
                      {estPriceLabelSuffix(node)}
                    </span>
                  ) : null}
                </span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function App() {
  const initialGraph = buildDemoGraph();
  const [graph, setGraph] = useState<GraphPayload>(initialGraph);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");
  const [prompt, setPrompt] = useState<string>("");
  const [filters, setFilters] = useState<RecommendFilters>(defaultFilters);
  const [activeNodeId, setActiveNodeId] = useState<string>(initialGraph.centerId);
  const [selectedGame, setSelectedGame] = useState<GraphNode | null>(getCenterNode(initialGraph));
  const [status, setStatus] = useState<string>("Showing a starter graph. Ask for a recommendation or click a node.");
  const [filtersOpen, setFiltersOpen] = useState<boolean>(false);
  const [chatOpen, setChatOpen] = useState<boolean>(false);
  /** Set when /api/recommend returns nlParse (prompt anchor BGG id for similarity queries). */
  const [promptNlParse, setPromptNlParse] = useState<NlParseMeta | null>(null);

  useEffect(() => {
    let ignore = false;

    async function loadDefault() {
      try {
        const response = await fetch(`${API_URL}/api/graph/default`);
        if (!response.ok) {
          throw new Error(`Default graph request failed (${response.status})`);
        }

        const payload = (await response.json()) as GraphApiResponse;
        if (ignore) return;

        setGraph(payload.graph);
        setActiveNodeId(payload.graph.centerId);
        setSelectedGame(getCenterNode(payload.graph));
        setPromptNlParse(null);
        setStatus("Loaded a Neo4j-backed starter recommendation.");
      } catch (fetchError) {
        if (ignore) return;
        const demo = buildDemoGraph();
        setGraph(demo);
        setActiveNodeId(demo.centerId);
        setSelectedGame(getCenterNode(demo));
        setError(fetchError instanceof Error ? fetchError.message : "Failed to load graph.");
        setStatus("Using the fallback demo graph while the backend is unavailable.");
      } finally {
        if (!ignore) setLoading(false);
      }
    }

    loadDefault();

    return () => {
      ignore = true;
    };
  }, []);

  async function loadRecommendation(body: RecommendRequestBody, note: string): Promise<void> {
    setLoading(true);
    setError("");

    try {
      const response = await fetch(`${API_URL}/api/recommend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });

      if (!response.ok) {
        const payload = asApiError(await response.json().catch(() => ({})));
        throw new Error(payload.error || `Recommendation request failed (${response.status})`);
      }

      const payload = (await response.json()) as GraphApiResponse;
      setPromptNlParse(payload.nlParse ?? null);

      let nextGraph = applySearchMetaToGraph(payload.graph, payload.searchMeta ?? null);

      const anchorBgg = payload.nlParse?.anchorBggId;
      const pk = payload.nlParse?.promptKind;
      const recenterOnAnchor =
        Boolean(anchorBgg) && (pk === "similar_to_game" || pk === "both");

      if (recenterOnAnchor) {
        try {
          const anchorRes = await fetch(
            `${API_URL}/api/graph/bgg/${encodeURIComponent(String(anchorBgg))}`
          );
          if (anchorRes.ok) {
            const anchorPayload = (await anchorRes.json()) as GraphApiResponse;
            nextGraph = anchorPayload.graph;
          }
        } catch {
          /* keep search-ranked center graph */
        }
      }

      setGraph(nextGraph);
      setActiveNodeId(nextGraph.centerId);
      setSelectedGame(getCenterNode(nextGraph));
      setStatus(note);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Recommendation failed.");
      setStatus("The backend did not return a recommendation, so the graph stayed in place.");
    } finally {
      setLoading(false);
    }
  }

  async function handlePromptSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    await loadRecommendation(
      { message: prompt },
      "Prompt-based recommendation loaded."
    );
    setChatOpen(false);
  }

  async function handleFilterSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    await loadRecommendation(
      {
        filters: toRequestFilters(filters)
      },
      "Filter-based recommendation loaded."
    );
    setFiltersOpen(false);
  }

  async function handleNodeClick(node: GraphNode): Promise<void> {
    if (!node.id || node.id === activeNodeId) return;

    setSelectedGame(node);
    setLoading(true);
    setError("");

    try {
      if (node.id.startsWith("demo-")) {
        const nextGraph = buildDemoGraph(node.id);
        setGraph(nextGraph);
        setActiveNodeId(nextGraph.centerId);
        setSelectedGame(getCenterNode(nextGraph));
        setPromptNlParse(null);
        setStatus(`Animated toward ${node.name} in the demo graph.`);
        return;
      }

      const response = await fetch(`${API_URL}/api/graph/node/${encodeURIComponent(node.id)}`);
      if (!response.ok) {
        const payload = asApiError(await response.json().catch(() => ({})));
        throw new Error(payload.error || `Node request failed (${response.status})`);
      }

      const payload = (await response.json()) as GraphApiResponse;
      setGraph(payload.graph);
      setActiveNodeId(payload.graph.centerId);
      setSelectedGame(getCenterNode(payload.graph));
      setPromptNlParse(null);
      setStatus(`Graph shifted to ${node.name}.`);
    } catch (clickError) {
      setError(clickError instanceof Error ? clickError.message : "Node click failed.");
    } finally {
      setLoading(false);
    }
  }

  const centerNode = graph.nodes.find((node) => node.kind === "center") || graph.nodes[0];
  const neighborCount = graph.nodes.length - 1;

  return (
    <div className="app-shell">
      <div className="backdrop backdrop-a" />
      <div className="backdrop backdrop-b" />

      <header className="hero-bar">
        <div>
          <p className="eyebrow">Knowledge graph explorer</p>
          <h1>Explore and find games that are worth it for you.</h1>
          <p className="lede">
            Search, filter, and jump across the graph. The center node matches your current filters best and similar games
            orbit around it. Click any node to recenter the graph there, explore its neighbors, and read details in the side panel. 
            Filter games through the "Filters" button, or ask for a recommendation in your own words with the "Chat" button.
          </p>
        </div>

        <div className="hero-stats compact">
          {promptNlParse?.anchorBggId ? (
            <article className="hero-anchor-bgg">
              <span>Prompt reference · BGG ID</span>
              <strong className="hero-bgg-id">{promptNlParse.anchorBggId}</strong>
              {promptNlParse.similarToGame ? (
                <span className="hero-anchor-title">{promptNlParse.similarToGame}</span>
              ) : null}
            </article>
          ) : null}
          <article>
            <span>Center game</span>
            <strong>{centerNode?.name || "Unknown"}</strong>
            {promptNlParse?.anchorBggId &&
            centerNode?.bggId &&
            String(centerNode.bggId) === String(promptNlParse.anchorBggId) ? (
              <span className="hero-center-bgg">BGG {centerNode.bggId}</span>
            ) : null}
          </article>
          <article>
            <span>Neighbors</span>
            <strong>{neighborCount}</strong>
          </article>
          <article>
            <span>Status</span>
            <strong>{loading ? "Updating" : "Ready"}</strong>
          </article>
        </div>
      </header>

      <main className="graph-workspace">
        <aside className={`game-info-panel ${selectedGame ? "open" : ""}`}>
          <div className="drawer-top">
            <div>
              <p className="drawer-kicker">Game info</p>
              {promptNlParse?.anchorBggId &&
              selectedGame?.bggId &&
              String(selectedGame.bggId) === String(promptNlParse.anchorBggId) ? (
                <p className="panel-anchor-kicker">
                  Prompt reference · BGG ID <strong>{promptNlParse.anchorBggId}</strong>
                </p>
              ) : null}
              <h2>{selectedGame?.name || "Select a game"}</h2>
            </div>
          </div>

          <div className="game-info-scroll">
            {selectedGame ? (
              <>
                <div className="info-stat-grid">
                  <article>
                    <span>Rating</span>
                    <strong>{selectedGame.rating == null ? "-" : selectedGame.rating.toFixed(1)}</strong>
                  </article>
                  <article>
                    <span>Players</span>
                    <strong>
                      {formatStat(selectedGame.minPlayers)} - {formatStat(selectedGame.maxPlayers)}
                    </strong>
                  </article>
                  <article>
                    <span>Play time</span>
                    <strong>{formatPlayTimeBlock(selectedGame)}</strong>
                  </article>
                  <article>
                    <span>Users rated</span>
                    <strong>{formatStat(selectedGame.usersRated?.toLocaleString() ?? selectedGame.usersRated)}</strong>
                  </article>
                  <article>
                    <span>Complexity</span>
                    <strong>
                      {selectedGame.complexity == null ? "—" : selectedGame.complexity.toFixed(2)}
                    </strong>
                  </article>
                  <article>
                    <span>Min age</span>
                    <strong>{formatStat(selectedGame.minAge)}</strong>
                  </article>
                  <article>
                    <span>Est. price</span>
                    <strong>{formatEstPrice(selectedGame)}</strong>
                  </article>
                </div>

                {selectedGame.searchExplain ? (
                  <div className="info-stat-grid">
                    <article>
                      <span>$/rating</span>
                      <strong>
                        {selectedGame.searchExplain.ratingPerDollar == null
                          ? "—"
                          : selectedGame.searchExplain.ratingPerDollar.toFixed(3)}
                      </strong>
                    </article>
                    <article>
                      <span>Wants / owns</span>
                      <strong>
                        {selectedGame.searchExplain.wants} / {selectedGame.searchExplain.owns}
                      </strong>
                    </article>
                    <article>
                      <span>WTB / WTT</span>
                      <strong>
                        {selectedGame.searchExplain.wtb} / {selectedGame.searchExplain.wtt}
                      </strong>
                    </article>
                  </div>
                ) : null}

                <div className="info-section">
                  <h3>Details</h3>
                  <p className="panel-context-line">
                    {selectedGame.kind === "center"
                      ? "Current focus game in the graph."
                      : selectedGame.kind === "neighbor"
                        ? "Nearby recommendation in this graph view."
                        : "Selected from the knowledge graph."}
                  </p>
                  {selectedGame.bggId ? (
                    <p className="bgg-external">
                      <a
                        href={`https://boardgamegeek.com/boardgame/${selectedGame.bggId}`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open on BoardGameGeek →
                      </a>
                    </p>
                  ) : null}
                  <dl className="detail-dl-rich">
                    <div>
                      <dt>BGG ID</dt>
                      <dd>{selectedGame.bggId || "—"}</dd>
                    </div>
                    <div>
                      <dt>Year</dt>
                      <dd>{formatStat(selectedGame.yearPublished)}</dd>
                    </div>
                    {selectedGame.rank != null ? (
                      <div>
                        <dt>BGG rank</dt>
                        <dd>{formatStat(selectedGame.rank)}</dd>
                      </div>
                    ) : null}
                    <div>
                      <dt>{graph.neighborMode === "search_hits" ? "Result rank" : "Similarity"}</dt>
                      <dd>
                        {selectedGame.kind === "center"
                          ? graph.neighborMode === "search_hits" && selectedGame.queryResultRank != null
                            ? `#${selectedGame.queryResultRank} (current query list)`
                            : "—"
                          : graph.neighborMode === "search_hits" && selectedGame.queryResultRank != null
                            ? `#${selectedGame.queryResultRank} of this query’s list`
                            : formatStat(Math.round((selectedGame.similarity ?? 0) * 100), "%")}
                      </dd>
                    </div>
                    {selectedGame.geekRating != null ? (
                      <div>
                        <dt>Geek rating</dt>
                        <dd>{selectedGame.geekRating.toFixed(3)}</dd>
                      </div>
                    ) : null}
                    {selectedGame.averageRating != null ? (
                      <div>
                        <dt>Average rating</dt>
                        <dd>{selectedGame.averageRating.toFixed(3)}</dd>
                      </div>
                    ) : null}
                    {selectedGame.numVoters != null ? (
                      <div>
                        <dt>Avg. rating voters</dt>
                        <dd>{selectedGame.numVoters.toLocaleString()}</dd>
                      </div>
                    ) : null}
                    {selectedGame.bestMinPlayers != null || selectedGame.bestMaxPlayers != null ? (
                      <div>
                        <dt>Best at</dt>
                        <dd>
                          {formatStat(selectedGame.bestMinPlayers)} – {formatStat(selectedGame.bestMaxPlayers)} players
                        </dd>
                      </div>
                    ) : null}
                    <div>
                      <dt>Expansion</dt>
                      <dd>{formatBool(selectedGame.isExpansion)}</dd>
                    </div>
                  </dl>
                </div>

                {(selectedGame.categories?.length ?? 0) > 0 ? (
                  <div className="info-section">
                    <h3>Categories</h3>
                    <div className="tag-row">
                      {selectedGame.categories!.map((c) => (
                        <span key={c} className="bgg-chip">
                          {c}
                        </span>
                      ))}
                    </div>
                  </div>
                ) : null}

                {(selectedGame.mechanisms?.length ?? 0) > 0 ? (
                  <div className="info-section">
                    <h3>Mechanisms</h3>
                    <div className="tag-row">
                      {selectedGame.mechanisms!.map((m) => (
                        <span key={m} className="bgg-chip">
                          {m}
                        </span>
                      ))}
                    </div>
                  </div>
                ) : null}

                {selectedGame.description ? (
                  <div className="info-section info-section-tall">
                    <h3>Description</h3>
                    <div className="game-description">{selectedGame.description}</div>
                  </div>
                ) : null}
              </>
            ) : (
              <p className="panel-note">Click a node to open game details here.</p>
            )}
          </div>
        </aside>

        <section className="graph-stage-panel">
          <div className="panel-head graph-head">
            <div>
              <h2>Knowledge graph</h2>
              <p>Click the main node or any neighbor to recenter the graph.</p>
            </div>
          </div>

          <GraphCanvas
            graph={graph}
            activeNodeId={activeNodeId}
            onNodeClick={handleNodeClick}
            promptAnchorBggId={promptNlParse?.anchorBggId ?? null}
          />

          <div className="neighbor-strip">
            {graph.nodes
              .filter((node) => node.kind !== "center")
              .map((node) => (
                <button key={node.id} className="neighbor-pill" onClick={() => handleNodeClick(node)} type="button">
                  <span>{node.name}</span>
                  <small>
                    {(() => {
                      const cap =
                        graph.neighborMode === "search_hits" && node.queryResultRank != null
                          ? `#${node.queryResultRank}`
                          : formatStat(Math.round((node.similarity ?? 0) * 100), "%");
                      const p = node.searchExplain?.meanPrice ?? node.estimatedPrice;
                      if (p != null && Number.isFinite(p)) return `${cap} · $${p.toFixed(0)}`;
                      return cap;
                    })()}
                  </small>
                </button>
              ))}
          </div>
        </section>

        <aside className={`drawer ${filtersOpen ? "open" : ""}`}>
          <div className="drawer-top">
            <div>
              <p className="drawer-kicker">Filters</p>
              <h2>Refine the recommendation</h2>
            </div>
            <button className="icon-button" type="button" onClick={() => setFiltersOpen(false)} aria-label="Close filters">
              ×
            </button>
          </div>

          <form className="filter-grid" onSubmit={handleFilterSubmit}>
            <label>
              Keyword
              <input
                value={filters.keyword}
                onChange={(event) => setFilters((current) => ({ ...current, keyword: event.target.value }))}
                placeholder="engine building, co-op, dungeon"
              />
            </label>
            <label>
              Players
              <input
                value={filters.players}
                onChange={(event) => setFilters((current) => ({ ...current, players: event.target.value }))}
                inputMode="numeric"
                placeholder="4"
              />
            </label>
            <label>
              Max time (min)
              <input
                value={filters.maxTime}
                onChange={(event) => setFilters((current) => ({ ...current, maxTime: event.target.value }))}
                inputMode="numeric"
                placeholder="90"
              />
            </label>
            <label>
              Max price ($)
              <input
                value={filters.maxPrice}
                onChange={(event) => setFilters((current) => ({ ...current, maxPrice: event.target.value }))}
                inputMode="decimal"
                placeholder="30"
              />
            </label>
            <label>
              Min rating
              <input
                value={filters.minRating}
                onChange={(event) => setFilters((current) => ({ ...current, minRating: event.target.value }))}
                inputMode="decimal"
                placeholder="7.0"
              />
            </label>
            <label>
              Sort by
              <select
                value={filters.sort}
                onChange={(event) =>
                  setFilters((current) => ({ ...current, sort: event.target.value as SearchSortField | "" }))
                }
              >
                <option value="">Default</option>
                {SORT_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </label>
            <div className="preset-chips">
              <p className="chips-label">Presets</p>
              <div className="chip-row">
                {PRESET_CHIPS.map((c) => (
                  <button
                    key={c.id}
                    type="button"
                    className={`preset-chip ${filters.preset === c.id ? "active" : ""}`}
                    onClick={() =>
                      setFilters((cur) => ({
                        ...cur,
                        preset: cur.preset === c.id ? "" : c.id
                      }))
                    }
                  >
                    {c.label}
                  </button>
                ))}
              </div>
            </div>
            <button type="submit">Recommend from filters</button>
          </form>

          <div className="panel-note">
            <p>{status}</p>
            {error ? <p className="error-text">{error}</p> : null}
          </div>
        </aside>

        <button className="floating-tools filters-toggle" type="button" onClick={() => setFiltersOpen(true)}>
          Filters
        </button>

        <div className="chat-anchor">
          {chatOpen ? (
            <div className="chat-popup">
              <div className="drawer-top">
                <div>
                  <p className="drawer-kicker">Prompt</p>
                  <h2>Ask for a game</h2>
                </div>
                <button className="icon-button" type="button" onClick={() => setChatOpen(false)} aria-label="Close prompt">
                  ×
                </button>
              </div>

              <form
                className="form-stack"
                onSubmit={handlePromptSubmit}
              >
                <label>
                  Message
                  <textarea
                    rows={3}
                    value={prompt}
                    onChange={(event) => setPrompt(event.target.value)}
                    placeholder='Try: "strategy game for 4 players under 90 minutes"'
                  />
                </label>
                <button type="submit">Send</button>
              </form>
            </div>
          ) : null}

          <button className="floating-tools chat-toggle" type="button" onClick={() => setChatOpen((current) => !current)} aria-label="Open prompt">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path d="M20 4H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h3v3.5a.5.5 0 0 0 .8.4L12.5 18H20a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2Zm0 12h-8l-3.2 2.6V16H4V6h16v10Z" />
            </svg>
          </button>
        </div>
      </main>
    </div>
  );
}

export default App;