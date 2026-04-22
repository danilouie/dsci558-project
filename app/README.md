# Board Game Recommendation App

This folder contains the fullstack app:

- `backend`: Node.js + Express API for Neo4j queries
- `frontend`: React + Vite graph UI

## Prerequisites

- Node.js 18+
- npm
- A running Neo4j instance with your data loaded

## 1) Install Dependencies

From the `app` folder:

```bash
npm install
```

## 2) Configure Environment Files

### Frontend env

Create `app/frontend/.env` (or edit if it already exists):

```env
VITE_API_URL=http://localhost:4000
```

### Backend env

Create or update `app/backend/.env`:

```env
PORT=4000
FRONTEND_ORIGIN=http://localhost:5173

NEO4J_URI=bolt://localhost:7687
NEO4J_DATABASE=neo4j

# Use "basic" for username/password auth, or "none" if auth is disabled.
NEO4J_AUTH_MODE=basic
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Optional: set to false to silence step-by-step request/search logs in the backend console
# TRACE_STEPS=false
```

## 3) Run the App

From the `app` folder, start backend + frontend together:

```bash
npm run dev
```

App URLs:

- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:4000`

## Optional: Run Services Separately

From the `app` folder:

```bash
npm run dev:backend
```

```bash
npm run dev:frontend
```

## Build Frontend

From the `app` folder:

```bash
npm run build
```

## Troubleshooting

- If frontend cannot fetch data, check `VITE_API_URL` and backend port.
- If backend cannot connect to Neo4j, verify `NEO4J_URI`, auth mode, and credentials.
- If CORS errors appear in browser dev tools, check `FRONTEND_ORIGIN` in `app/backend/.env`.