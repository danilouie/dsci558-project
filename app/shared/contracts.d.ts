export type NodeKind = "center" | "neighbor";

export interface GameSummary {
  id: string;
  bggId?: string | null;
  name: string;
  yearPublished?: number | null;
  minPlayers?: number | null;
  maxPlayers?: number | null;
  playTime?: number | null;
  rating?: number | null;
  usersRated?: number | null;
  similarity?: number;
}

export interface GraphNode extends GameSummary {
  kind: NodeKind;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  weight: number;
}

export interface GraphPayload {
  centerId: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface RecommendCriteria {
  keyword: string;
  players: number | null;
  maxTime: number | null;
}

export interface RecommendRequestBody {
  message?: string;
  filters?: {
    keyword?: string;
    players?: number | null;
    maxTime?: number | null;
  };
}

export interface GraphApiResponse {
  source: string;
  graph: GraphPayload;
}

export interface ApiErrorResponse {
  error?: string;
}