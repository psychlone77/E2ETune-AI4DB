# GenKnob Tuner — Comprehensive Application Documentation

> **Version:** 0.0.1 | **Author:** Madushan Suriyabandara | **Repository:** `Madushansuriyabandara/Gen-Knob-Tuner`
> **Date:** May 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement & Motivation](#2-problem-statement--motivation)
3. [Application Overview](#3-application-overview)
4. [System Architecture](#4-system-architecture)
5. [Technology Stack](#5-technology-stack)
6. [Frontend Architecture](#6-frontend-architecture)
7. [Backend Architecture](#7-backend-architecture)
8. [AI/ML Pipeline](#8-aiml-pipeline)
9. [Multi-Engine Support](#9-multi-engine-support)
10. [Real-Time Communication](#10-real-time-communication)
11. [Security](#11-security)
12. [Build & Deployment](#12-build--deployment)
13. [API Reference](#13-api-reference)
14. [Project Structure](#14-project-structure)
15. [Configuration Reference](#15-configuration-reference)
16. [Key Workflows](#16-key-workflows)

---

## 1. Executive Summary

**GenKnob Tuner** is a cross-platform desktop application that uses AI-driven inference to automatically optimize database configuration knobs for PostgreSQL and MySQL. It is built as a final-year project (FYP) that bridges the gap between complex database tuning expertise and everyday DBA workflows.

The application analyses a user's SQL workload, collects live database internal metrics, extracts query execution plans, and feeds these features into a fine-tuned Small Language Model (SLM) hosted on a remote GPU. The model predicts optimal configuration bucket values which are decoded into real database settings and can be applied to the target server with one click.

---

## 2. Problem Statement & Motivation

### The Problem
Database performance tuning is notoriously complex. Systems like PostgreSQL expose **40+ tunable knobs** (e.g. `shared_buffers`, `work_mem`, `effective_cache_size`) whose optimal values depend on hardware specs, workload patterns, and data characteristics. Misconfiguration causes severe performance degradation, yet manual tuning requires deep expertise.

### Why GenKnob Tuner?
- **Automates expert-level tuning** — replaces days of manual experimentation with a single click.
- **AI-powered** — uses a fine-tuned 7B-parameter language model trained on real database benchmarks.
- **Workload-aware** — analyses actual SQL queries, not just static hardware rules.
- **Multi-engine** — supports both PostgreSQL and MySQL from a unified interface.
- **Graceful degradation** — falls back to deterministic rule-based tuning if the GPU server is offline.
- **Desktop-native** — runs as a standalone Electron app with no browser dependency.

---

## 3. Application Overview

### What It Does (End-to-End Flow)
1. User connects to a PostgreSQL or MySQL database.
2. User provides an SQL workload (paste or file upload).
3. User enters hardware specs (CPU cores, RAM, storage type).
4. The app parses the workload, collects internal metrics from the DB, and extracts query plans.
5. All features are sent to a remote GPU server running the fine-tuned SLM.
6. The model returns predicted configuration buckets (e.g. `"70% to 80%"`).
7. Buckets are decoded into real configuration values using knob min/max bounds.
8. Results are displayed and can be applied to the database via `ALTER SYSTEM` (PG) or `SET PERSIST_ONLY` (MySQL).
9. The user is notified if a database restart is required.

### Key Features
| Feature | Description |
|---|---|
| Workload Analysis | Parses SQL into individual statements, classifies query types (SELECT/INSERT/UPDATE/DELETE), extracts tables |
| Workload Feature Extraction | Uses `sqlglot` to compute read_ratio, join counts, filter_ratio, group_by_ratio, etc. |
| Internal Metrics Collection | Queries `pg_stat_database`, `pg_statio_all_tables`, `pg_stat_bgwriter` (PG) or `SHOW GLOBAL STATUS` (MySQL) |
| Query Plan Extraction | Runs `EXPLAIN (FORMAT JSON)` on each query, flattens and averages operator costs |
| AI Inference | Sends structured prompt to GPU-hosted SLM, receives bucket predictions |
| Bin Decoding | Converts percentage-range bucket labels back to real values via `value = min + midpoint × (max − min)` |
| Rule-Based Fallback | Deterministic formulas based on RAM/CPU when GPU is unavailable |
| One-Click Apply | Applies knobs via `ALTER SYSTEM SET` (PG) or `SET PERSIST_ONLY` (MySQL) |
| Connection Manager | Save, load, and delete database connection profiles |
| Real-Time Logs | WebSocket-based live log streaming during optimization |
| Model Management | Check model status, trigger background downloads from Hugging Face |
| GPU Status Monitoring | Header badges show backend + GPU connectivity with smart caching |

---

## 4. System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Electron Shell                        │
│  ┌───────────────────────────────────────────────────┐  │
│  │              React Frontend (Vite)                 │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────┐  │  │
│  │  │ Workload │ │ DB Creds │ │ Optimized Knobs  │  │  │
│  │  │ Section  │ │ Section  │ │ Display + Apply  │  │  │
│  │  └────┬─────┘ └────┬─────┘ └────────▲─────────┘  │  │
│  │       │             │                │            │  │
│  │       └──────┬──────┘                │            │  │
│  │              │ HTTP/REST             │            │  │
│  │              ▼                       │            │  │
│  │  ┌───────────────────────────────────┘            │  │
│  └──┼────────────────────────────────────────────────┘  │
│     │                                                    │
│     │  localhost:8000                                     │
│     ▼                                                    │
│  ┌────────────────────────────────────────────────┐     │
│  │          FastAPI Backend (Python)               │     │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐   │     │
│  │  │ Workload │ │ DB Engine│ │ SLM Inference│   │     │
│  │  │ Analyzer │ │ Layer    │ │ Client       │   │     │
│  │  └──────────┘ └──────────┘ └──────┬───────┘   │     │
│  │                                    │           │     │
│  │  ┌──────────┐ ┌──────────┐        │           │     │
│  │  │ Knob Mgr │ │ WebSocket│        │           │     │
│  │  │ Validator│ │ Manager  │        │           │     │
│  │  └──────────┘ └──────────┘        │           │     │
│  └───────────────────────────────────┼───────────┘     │
│                                      │                  │
└──────────────────────────────────────┼──────────────────┘
                                       │ HTTP POST
                                       ▼
                        ┌──────────────────────────┐
                        │  Remote GPU Droplet (DO)  │
                        │  FastAPI + PyTorch        │
                        │  Qwen2.5-Coder-7B +      │
                        │  genknob-tuner LoRA       │
                        └──────────────────────────┘
```

### Three-Tier Design
1. **Electron Shell** — Manages window lifecycle, spawns backend process in production.
2. **FastAPI Backend** — All business logic, DB connectivity, workload analysis, remote inference orchestration.
3. **GPU Inference Server** — Runs on a DigitalOcean GPU droplet; loads the fine-tuned model and serves predictions over HTTP.

---

## 5. Technology Stack

### Frontend
| Technology | Purpose |
|---|---|
| React 18 | UI framework |
| TypeScript | Type safety |
| Vite 6 | Build tool and dev server |
| TailwindCSS 4 | Utility-first styling |
| Radix UI | Accessible primitive components (Dialog, Tabs, Select, etc.) |
| Lucide React | Icon library |
| Recharts | Data visualization |
| react-resizable-panels | Split-pane layout |
| Sonner | Toast notifications |
| Motion (Framer) | Animations |
| Axios | HTTP client |

### Backend
| Technology | Purpose |
|---|---|
| Python 3.10+ | Runtime |
| FastAPI | REST API framework |
| Uvicorn | ASGI server |
| Pydantic v2 | Request/response validation |
| psycopg2 | PostgreSQL driver |
| asyncpg | Async PostgreSQL driver |
| mysql-connector-python | MySQL driver |
| sqlglot | SQL parsing and cross-dialect transpilation |
| cryptography (Fernet) | Credential encryption |
| transformers + peft | Local model loading (optional) |
| huggingface_hub | Model download management |
| PyInstaller | Production executable packaging |

### Desktop
| Technology | Purpose |
|---|---|
| Electron 28 | Desktop shell |
| electron-builder | Installer generation (NSIS) |

### AI/ML
| Technology | Purpose |
|---|---|
| Qwen2.5-Coder-7B-Instruct | Base LLM |
| LoRA (PEFT) | Fine-tuned adapter (`NisithDissanayake/genknob-tuner`) |
| PyTorch | Inference runtime |

---

## 6. Frontend Architecture

### Entry Point
- `index.html` → `src/main.tsx` → `src/app/App.tsx`

### Layout Structure
The UI mimics a professional database IDE (similar to pgAdmin/DBeaver):

- **AppMenuBar** — Top bar with branding, Optimize button, connection status badges, settings.
- **BrowserTree** (left panel, 20% width) — Tree view listing saved database connections with right-click delete.
- **TabbedWorkspace** (right panel, 80%) — Tabbed interface containing:
  - *Workload* tab — SQL paste area or drag-and-drop file upload.
  - *Connection* tab — Database credential form (host, port, database, username, password, engine selector).
  - *Hardware* tab — CPU cores, RAM GB, storage type (SSD/HDD/NVMe).
  - *Results* tab — Optimized knobs display with copy/download/apply actions.
  - *Logs* tab — Real-time operation log viewer.
- **StatusBar** — Bottom bar showing ready state.

### Component Hierarchy
```
App.tsx
├── MainLayout
│   ├── AppMenuBar
│   │   └── ConnectionStatus (Backend + GPU badges)
│   ├── BrowserTree (saved connections sidebar)
│   └── TabbedWorkspace
│       ├── WorkloadSection (paste/upload SQL)
│       ├── DatabaseCredentials (connection form)
│       ├── SystemSpecs (hardware inputs)
│       ├── OptimizedKnobs (results + apply button)
│       └── LogsViewer (real-time logs)
├── SettingsModal (auto-apply toggle)
├── ModelDownloadModal (model status/download)
├── SavedDatabasesModal
└── Toaster (notifications)
```

### State Management
All state is managed via React `useState` hooks in `App.tsx` and passed down as props. Saved connections and settings are persisted to `localStorage` under the key `dbOptimizerSettings`.

### API Layer (`src/app/api/`)
- **`optimizer.ts`** — `optimizeDatabase()`, `applyConfiguration()`, `testConnection()` — all using Axios with configurable timeouts.
- **`model.ts`** — `getModelStatus()`, `downloadModel()` — model lifecycle management.

### Custom Hooks
- **`useWebSocket.ts`** — Connects to `ws://localhost:8000/ws/logs`, parses incoming JSON messages, provides `isConnected`, `messages`, and `clearMessages`.

---

## 7. Backend Architecture

### Package Structure
```
gen_knob_backend/gen_knob_backend/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app, all route definitions
│   ├── core/
│   │   ├── config.py         # Pydantic Settings (env-based config)
│   │   └── logging.py        # Rotating file + console logger
│   ├── schemas/
│   │   ├── requests.py       # Pydantic models for incoming requests
│   │   └── responses.py      # Pydantic models for outgoing responses
│   ├── services/
│   │   ├── db_connection.py   # Connection pooling (psycopg2 + asyncpg)
│   │   ├── db_engines.py      # Engine abstraction (Postgres / MySQL)
│   │   ├── workload_analyzer.py       # SQL parsing, query type analysis, EXPLAIN plans
│   │   ├── workload_feature_extractor.py  # sqlglot-based feature extraction
│   │   ├── knob_manager.py   # Validation, application, audit logging
│   │   ├── slm_inference.py   # Remote GPU inference + bin decoding + rule-based fallback
│   │   ├── model_manager.py   # HuggingFace model download lifecycle
│   │   └── websocket_manager.py  # WS broadcast, log buffering
│   └── utils/
│       └── encryption.py      # Fernet-based credential encryption
├── models/                    # Local model storage (if downloaded)
├── requirements.txt
└── .env.example
```

### Service Layer Details

#### `db_engines.py` — Engine Abstraction
Implements a Strategy pattern via `BaseDatabaseEngine` ABC with two concrete implementations:
- **PostgresEngine** — Uses `psycopg2`, `ALTER SYSTEM SET`, `pg_reload_conf()`, `EXPLAIN (FORMAT JSON)`.
- **MysqlEngine** — Uses `mysql.connector`, `SET PERSIST_ONLY`, `EXPLAIN FORMAT=JSON`, and `sqlglot.transpile()` for cross-dialect query translation.

#### `workload_feature_extractor.py` — Feature Engineering
Uses `sqlglot` to parse SQL and extract:
- `size` — total query count
- `read_ratio` — fraction of SELECT statements
- `group_by_ratio`, `order_by_ratio` — fraction with GROUP BY / ORDER BY
- `avg_query_length` — average character length
- `number_of_joins` — average JOIN count per query
- `filter_ratio` — fraction with WHERE or HAVING

#### `slm_inference.py` — AI Inference
The core inference module that:
1. Constructs a Mistral-format instruction prompt incorporating workload features, internal metrics, query plans, and hardware specs.
2. Sends it via HTTP POST to the remote GPU server's `/generate` endpoint.
3. Extracts the JSON response containing predicted bucket labels (e.g. `"70% to 80%"`).
4. Decodes buckets to real values using `_decode_bins_heuristic()`:
   - Parses bucket label → midpoint fraction (0.0–1.0)
   - Applies inverse formula: `value = min + midpoint × (max − min)`
   - Formats values with appropriate units (GB, MB, kB for memory knobs)
5. Falls back to `_rule_based_recommendations()` if GPU is unreachable.

#### `knob_manager.py` — Validation & Application
- **KnobValidator** — Validates knob values against safe ranges (min/max bounds for 15 key PostgreSQL knobs). Identifies restart-required knobs (`shared_buffers`, `wal_buffers`, `max_worker_processes`, etc.).
- **KnobApplicator** — Delegates to the appropriate engine to apply knobs.
- **AuditLogger** — Logs all optimization and application events as structured JSON.

---

## 8. AI/ML Pipeline

### Training (External — E2ETune-AI4DB)
The model is trained in a separate research project (`E2ETune-AI4DB`) using:
- **Base Model:** `Qwen/Qwen2.5-Coder-7B-Instruct` (also referenced as `springhxm/E2ETune`)
- **Fine-tuning:** LoRA (Low-Rank Adaptation) via PEFT library
- **Training Data:** Database benchmark configurations from various workloads (TPC-H, TPC-C, etc.) across PostgreSQL and MySQL on different hardware specs
- **Binning Strategy:** Continuous knob values are discretized into 10 percentage-range text buckets (`"0% to 10%"`, ..., `"90% to 100%"`) using explicit hardware-specific min/max bounds from `knob_config.json`
- **Published Adapter:** `NisithDissanayake/genknob-tuner` on Hugging Face

### Inference (This App)
```
User Input                    Feature Extraction              Model Prompt
───────────                   ──────────────────              ────────────
SQL Workload ──────┐
                   ├──► sqlglot features ────────┐
DB Connection ─────┤                             │
                   ├──► pg_stat metrics ─────────┼──► Mistral Instruct Prompt
                   ├──► EXPLAIN plans ───────────┤
Hardware Specs ────┘                             │
                                                 ▼
                                    Remote GPU (/generate)
                                         │
                                         ▼
                                  Bucket Predictions
                                  {"shared_buffers": "70% to 80%", ...}
                                         │
                                         ▼
                                  Bin Decoder (heuristic)
                                  {"shared_buffers": "12GB", ...}
```

### Knob Configuration Schemas
Three JSON schemas define min/max bounds for each knob:
- `knob_config.json` — PostgreSQL (38 knobs)
- `mysql32_config.json` — MySQL 32GB RAM profile
- `mysql64_config.json` — MySQL 64GB RAM profile

### GPU Server Deployment
Three deployment options exist:
1. **DigitalOcean GPU Droplet** (`server_deploy.py`) — Production: static IP, `nvidia-smi` health check.
2. **Kaggle Notebook** (`kaggle_server.py`) — Development: uses ngrok tunnel.
3. **Direct Remote** (`remote_server.py`) — Alternative with Qwen prompt format.

---

## 9. Multi-Engine Support

| Aspect | PostgreSQL | MySQL |
|---|---|---|
| Connection | `psycopg2` | `mysql.connector` |
| Apply Knobs | `ALTER SYSTEM SET` | `SET PERSIST_ONLY` |
| Reload Config | `SELECT pg_reload_conf()` | Requires restart |
| Explain Plans | `EXPLAIN (FORMAT JSON)` | `EXPLAIN FORMAT=JSON` |
| Internal Metrics | `pg_stat_database`, `pg_statio_all_tables`, `pg_stat_bgwriter` | `SHOW GLOBAL STATUS` |
| Query Transpilation | Native | `sqlglot.transpile(read="postgres", write="mysql")` |
| Knob Schemas | `knob_config.json` (38 knobs) | `mysql32_config.json` / `mysql64_config.json` |

---

## 10. Real-Time Communication

### WebSocket Architecture
- **Endpoint:** `ws://localhost:8000/ws/logs`
- **Manager:** `WebSocketManager` — thread-safe with asyncio Lock, supports multiple concurrent clients.
- **Buffer:** Up to 1000 messages buffered; new connections receive full history.
- **Message Types:** `log`, `status`, `error`, `result`
- **Log Streaming Context:** `LogStreamingContext` wraps operations to automatically broadcast log messages at INFO/WARNING/ERROR levels.

### Frontend Hook
`useWebSocket.ts` provides reactive state for connection status and message stream, auto-reconnects on URL change.

### Connection Status Badges
`ConnectionStatus.tsx` polls two endpoints:
- Backend health (`/health`) every 30 seconds
- GPU status (`/api/gpu-status`) every 60 seconds

GPU status uses smart caching: if inference is in-flight or last healthy check was <5 min ago, returns cached result to avoid false "offline" reports.

---

## 11. Security

- **Credential Encryption:** Fernet symmetric encryption via PBKDF2 key derivation (100,000 iterations, SHA-256).
- **CORS:** Restricted to `localhost:5173` and `localhost:3000` origins.
- **Input Validation:** All requests validated via Pydantic models with field constraints (port range, positive integers, non-empty strings).
- **Request Limits:** Max workload size 10MB, request timeout 5 minutes.
- **Context Isolation:** Electron's `contextIsolation: true`, `nodeIntegration: false`.
- **Knob Safety:** All knobs validated against safe min/max ranges before application.

---

## 12. Build & Deployment

### Development Mode
```bash
# Terminal 1: Backend
cd gen_knob_backend/gen_knob_backend
python -m uvicorn app.main:app --reload --port 8000

# Terminal 2: Frontend + Electron
npm run dev
```

### Production Build

**Step 1 — Backend Executable:**
```bash
python build_backend_system.py
```
Uses PyInstaller to compile FastAPI + all dependencies into a single `gen_knob_engine.exe` (~100+ MB). Checks and installs missing dependencies automatically.

**Step 2 — Electron Installer:**
```bash
Build-Installer.bat  # Run as Administrator
```
Runs `npm run build` which:
1. `vite build` — Bundles React app into `dist/`
2. `tsc -p electron/tsconfig.json` — Compiles Electron TypeScript into `dist-electron/`
3. `electron-builder` — Packages everything into NSIS installer (`release/GenKnob Tuner Setup 0.0.1.exe`)

The backend executable is bundled as an `extraResource` and spawned as a child process on app launch.

### Database Setup
`Setup-Test-DB.bat` automates:
1. Starting PostgreSQL service
2. Creating `gen_knob_test` database
3. Creating `gen_knob_tester` superuser
4. Populating sample `users` and `orders` tables

---

## 13. API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check — returns `{status, version}` |
| GET | `/status` | Backend status — SLM availability, WS connections |
| GET | `/api/gpu-status` | GPU server reachability (with caching) |
| POST | `/api/optimize` | Main optimization endpoint |
| POST | `/api/apply-config` | Apply knobs to database |
| POST | `/api/test-connection` | Test database connectivity |
| GET | `/api/model/status` | Check SLM model download status |
| POST | `/api/model/download` | Trigger background model download |
| WS | `/ws/logs` | Real-time log streaming |

### Key Request/Response Schemas

**OptimizationRequest:**
```json
{
  "workload": "SELECT * FROM users; ...",
  "dbCredentials": {
    "engine": "postgresql",
    "host": "localhost",
    "port": "5432",
    "database": "mydb",
    "username": "user",
    "password": "pass"
  },
  "systemSpecs": {
    "cpu": "8",
    "ram": "32",
    "hardDriveType": "SSD"
  }
}
```

**OptimizationResponse:**
```json
{
  "knobs": {
    "shared_buffers": "8GB",
    "effective_cache_size": "24GB",
    "work_mem": "256MB"
  },
  "message": "Configuration optimized successfully"
}
```

---

## 14. Project Structure

```
Gen-Knob-Tuner/
├── electron/                      # Electron main + preload
│   ├── main.ts                    # Window creation, backend lifecycle
│   └── preload.ts                 # Context bridge
├── src/                           # React frontend
│   ├── main.tsx                   # React entry point
│   ├── app/
│   │   ├── App.tsx                # Root component, global state
│   │   ├── types.ts               # TypeScript interfaces
│   │   ├── api/                   # HTTP client layer
│   │   └── components/            # Feature components + layout + UI library
│   ├── components/                # Shared components (ConnectionStatus)
│   ├── hooks/                     # Custom hooks (useWebSocket)
│   └── styles/                    # CSS (theme, fonts, tailwind)
├── gen_knob_backend/              # Python backend
│   ├── gen_knob_backend/
│   │   ├── app/                   # FastAPI application
│   │   │   ├── main.py            # Routes + WebSocket
│   │   │   ├── core/              # Config + Logging
│   │   │   ├── schemas/           # Pydantic models
│   │   │   ├── services/          # Business logic
│   │   │   └── utils/             # Encryption
│   │   └── requirements.txt
│   ├── kaggle_server.py           # Kaggle GPU deployment
│   └── server_deploy.py           # DO GPU deployment
├── Support Docs/                  # Knob configs, binning script
├── docs/                          # Additional documentation
├── remote_server.py               # Alternative GPU server
├── build_backend_system.py        # PyInstaller build script
├── Build-Installer.bat            # Electron installer build
├── Setup-Test-DB.bat              # PostgreSQL test DB setup
├── package.json                   # Node dependencies + Electron builder config
├── vite.config.ts                 # Vite configuration
└── tsconfig.json                  # TypeScript configuration
```

---

## 15. Configuration Reference

### Environment Variables (`.env`)
| Variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Backend bind address |
| `PORT` | `8000` | Backend port |
| `DEBUG` | `true` | Debug mode |
| `FRONTEND_URL` | `http://localhost:5173` | Frontend origin |
| `DATABASE_POOL_SIZE` | `10` | Connection pool size |
| `ENCRYPTION_KEY` | (default) | Master key for Fernet |
| `SLM_REPO_ID` | `NisithDissanayake/genknob-tuner` | HuggingFace model ID |
| `SLM_MODEL_PATH` | `models/NisithDissanayake/genknob-tuner` | Local model path |
| `SLM_CONTEXT_SIZE` | `2048` | Model context window |
| `LOG_LEVEL` | `INFO` | Logging level |
| `MAX_WORKLOAD_SIZE` | `10485760` (10MB) | Max workload input size |

### PostgreSQL Knobs Managed (15 validated knobs)
`shared_buffers`, `effective_cache_size`, `maintenance_work_mem`, `work_mem`, `wal_buffers`, `checkpoint_completion_target`, `default_statistics_target`, `random_page_cost`, `effective_io_concurrency`, `max_worker_processes`, `max_parallel_workers`, `max_parallel_workers_per_gather`, `max_parallel_maintenance_workers`, `min_wal_size`, `max_wal_size`

### PostgreSQL Knobs in Config Schema (38 total)
Includes the above plus: `max_wal_senders`, `autovacuum_max_workers`, `max_connections`, `autovacuum_*` family, `bgwriter_*` family, `checkpoint_*` family, `commit_*`, `cursor_tuple_fraction`, `deadlock_timeout`, `geqo_*` family, `join_collapse_limit`, `from_collapse_limit`, `temp_buffers`, `temp_file_limit`, `vacuum_cost_*` family, `wal_writer_delay`

---

## 16. Key Workflows

### Optimization Workflow
1. Frontend validates required fields (workload, host, database).
2. `POST /api/optimize` sends workload, credentials, and hardware specs.
3. Backend parses SQL → extracts features with sqlglot → collects DB metrics → extracts EXPLAIN plans.
4. Constructs AI prompt and calls remote GPU `/generate` endpoint.
5. Parses model's JSON response → decodes bins to real values → validates knobs.
6. Returns knob recommendations to frontend.
7. All steps broadcast via WebSocket for real-time log display.

### Apply Configuration Workflow
1. Frontend sends `POST /api/apply-config` with knobs and credentials.
2. Backend validates each knob against safe ranges.
3. Executes engine-specific commands (`ALTER SYSTEM SET` / `SET PERSIST_ONLY`).
4. Reloads configuration (`pg_reload_conf()` for PostgreSQL).
5. Returns success/failure with restart-required knob list.
6. Frontend shows restart command (`sudo systemctl restart postgresql`) if needed.

### Graceful Fallback Workflow
1. Backend attempts to reach GPU server with 180-second timeout.
2. If `RequestException` occurs, logs warning and invokes `_rule_based_recommendations()`.
3. Rule-based logic computes: `shared_buffers = RAM/4`, `effective_cache_size = RAM×3/4`, `work_mem = RAM/(CPU×4)`, etc.
4. User receives recommendations regardless of GPU availability.

---

> **Note:** This document reflects the codebase as of May 2026. The application is under active development as part of a Final Year Project.
