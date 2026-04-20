# Dynamic Data Pipeline

A self-managing Spring Boot 3 pipeline that automatically discovers BigQuery tables, syncs data to Azure Cosmos DB, and serves it via a REST API with sub-millisecond EhCache reads.

---

## What It Does

```
Google BigQuery  →  Azure Cosmos DB  →  EhCache (JVM heap)  →  REST API
  source of truth    persistent store     <1ms reads           clients
```

- **Auto-discovery** — detects new BigQuery tables at 11 PM nightly, no code changes needed
- **Nightly sync** — bulk-upserts all records to Cosmos DB by 1 AM
- **Cache warm-up** — loads 100K+ records into memory after every sync
- **Self-healing** — rebuilds cache automatically on restart
- **Zero downtime** — `last_sync_time` strategy keeps old data live during sync

---

## Architecture

```
11:00 PM  TableDiscoveryService
            └── scans BigQuery datasets
            └── detects schema + partition key automatically
            └── registers new tables
            └── creates Cosmos containers

11:30 PM  GenericDataSyncService
            └── Step 1:  BigQuery fetch (FULL or INCREMENTAL)
            └── Step 2A: inject last_sync_time into every document
            └── Step 2B: Cosmos bulk upsert (vendor partition key)
            └── Step 2C: delete orphaned records from previous sync
            └── Step 3:  clear + reload EhCache (100% warm)

On restart  CacheWarmUpService
            └── ApplicationReadyEvent → stream Cosmos → EhCache
            └── continuation tokens, pages of 1000, ~10 min
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Framework | Spring Boot 3.2.4 / Java 17 | Auto-configuration, scheduler, actuator |
| Source | Google BigQuery SDK | Columnar data, INFORMATION_SCHEMA discovery |
| Store | Azure Cosmos DB SDK 4.65.0 | Schemaless JSON, bulk executor API |
| Cache | EhCache 3 (native API) | Heap-only, noExpiration, full control |
| Async | Project Reactor (Flux) | Required by Cosmos bulk + continuation APIs |
| Resilience | Resilience4j | Circuit breaker + retry on BQ calls |
| Observability | Micrometer + Actuator | Cache hit/miss metrics, health endpoints |
| Docs | SpringDoc OpenAPI | Auto-generated Swagger UI |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Google Cloud project with BigQuery enabled
- Azure Cosmos DB account (1000 RU/s minimum)
- GCP service account JSON key with BigQuery read permissions

---

## Configuration

All configuration lives in `src/main/resources/application.yml`.

### Cosmos DB

```yaml
azure:
  cosmos:
    endpoint: https://your-account.documents.azure.com:443/
    key: your-primary-key
    database: DynamicPipeline

spring:
  cloud:
    azure:
      cosmos:
        endpoint: https://your-account.documents.azure.com:443/
        key: your-primary-key
        content-response-on-write-enabled: false
```

### BigQuery

```yaml
bigquery:
  project-id: your-gcp-project-id
  scan-datasets:
    - records_datasets
  excluded-tables:
    - temp_table
    - staging_table
```

### Scheduler

```yaml
scheduler:
  discovery:
    cron: "0 0 23 * * *"      # 11:00 PM — scan BQ for new tables
  sync:
    cron: "0 30 23 * * *"     # 11:30 PM — sync all tables
    run-on-startup: true       # sync immediately on app start
```

### Cache

```yaml
ehcache:
  generic:
    heap-entries: 350000       # must exceed total record count × 1.2
```

### Cosmos Throughput

```yaml
cosmos:
  default-throughput: 400      # RU/s per container
                               # total across all containers < account limit
```

---

## Running the Application

```bash
# Clone
git clone https://github.com/your-org/dynamic-data-pipeline.git
cd dynamic-data-pipeline

# Set credentials (or hardcode in application.yml for local dev)
export COSMOS_ENDPOINT=https://your-account.documents.azure.com:443/
export COSMOS_KEY=your-primary-key
export BIGQUERY_PROJECT_ID=your-gcp-project-id

# Run
mvn spring-boot:run

# Or build and run jar
mvn clean package -DskipTests
java -Xmx3g -jar target/dynamic-data-pipeline-*.jar
```

> **JVM Heap:** Set `-Xmx3g` minimum. EhCache holds 350K records in heap alongside the BQ list during sync.

---

## API Reference

### Records

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/records` | List all registered tables |
| `GET` | `/api/v1/records/{table}/{id}` | Get record by ID (cache-aside) |
| `GET` | `/api/v1/records/{table}?page=0&size=20` | Paginated listing |
| `GET` | `/api/v1/records/{table}/search?field=value` | Field search |

### Admin

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/admin/tables` | List all tables with sync status |
| `POST` | `/api/v1/admin/tables/register` | Manually register a table |
| `POST` | `/api/v1/admin/tables/{table}/sync` | Trigger sync now |
| `POST` | `/api/v1/admin/tables/{table}/sync?fullReload=true` | Force full sync |
| `POST` | `/api/v1/admin/tables/discover` | Trigger discovery scan |
| `DELETE` | `/api/v1/admin/tables/{table}` | Deregister a table |

### Observability

| Endpoint | Description |
|---|---|
| `GET /actuator/health` | App health + circuit breaker state |
| `GET /actuator/metrics` | Micrometer metrics |
| `GET /actuator/caches` | EhCache stats |
| `GET /swagger-ui.html` | Interactive API docs |

---

## How It Works

### Dynamic Table Discovery

The app never needs to know about a table in advance. When discovery runs:

1. Queries `INFORMATION_SCHEMA.TABLES` in each configured BQ dataset
2. For each new table — fetches column schema from `INFORMATION_SCHEMA.COLUMNS`
3. Auto-detects the `id` field (column named `id` → `{table}_id` → first STRING ending in `id`)
4. Auto-selects partition key by priority: `vendor` → `region` → `category` → `tenant_id` → `id`
5. Registers the table, creates Cosmos container, triggers full sync in background

### Sync Modes

| Condition | Mode | Query |
|---|---|---|
| `lastSyncTime = null` | FULL | `SELECT * FROM table` |
| Has `updated_at` column | INCREMENTAL | `WHERE updated_at > lastSyncTime - 25hr` |
| No `updated_at` column | FULL | `SELECT * FROM table` (safe fallback) |

### last_sync_time Strategy (Zero Downtime)

Every sync injects `last_sync_time = syncStartTime` into each document before upserting. After upsert completes with zero failures, documents with older timestamps (orphans from previous sync runs) are bulk-deleted. Old records serve cache-miss fallbacks during upsert — no empty window.

```
Step 2B: upsert new records → old records still in Cosmos ✅
Step 2C: delete WHERE last_sync_time != today → orphans removed ✅
Result:  Cosmos mirrors BigQuery exactly, zero downtime ✅
```

### Cache Key Format

```
"tableName:documentId"

software table:  "software:prod-000001"
hardware table:  "hardware:hw-000001"
```

Single EhCache instance serves all tables — no key collision possible.

---

## Data Requirements

### BigQuery Table Schema

Your table must have:
- An `id` column (STRING) — used as the Cosmos document ID
- A high-cardinality field for partition key — ideally `vendor`, `region`, or `category`

Optional but recommended:
- An `updated_at` column (TIMESTAMP) — enables incremental sync instead of full sync daily

### Example Schema

```sql
CREATE TABLE `project.dataset.software` (
  id           STRING    NOT NULL,
  product      STRING,
  price        FLOAT64,
  type         STRING,
  availability STRING,
  vendor       STRING,   -- used as Cosmos partition key
  region       STRING,
  category     STRING,
  updated_at   TIMESTAMP -- enables incremental sync
);
```

---

## Partition Key Rules

A good partition key must have **high cardinality** (many distinct values) and **not change** after a document is created. The app auto-detects in this priority order:

```
vendor    → 30 values  ✅ ideal
region    → 12 values  ✅ good
category  → 18 values  ✅ good
tenant_id → varies     ✅ good if present
id        → sequential ⚠️  causes write hotspots (fallback only)
```

Sequential IDs as partition keys cause all writes to pile onto the last partition range after ~80% of records are written, collapsing throughput from 150/s to 55/s.

---

## Throughput Planning

```
Account RU/s limit = sum of all container RU/s

Example with 1000 RU/s account limit:
  software container:    400 RU/s
  hardware container:    400 RU/s
  table-configs system:  ~minimal
  Total:                 ~800 RU/s  ← under 1000 limit ✅

Sync time estimate at 400 RU/s (0.38 RU per upsert, 100K records):
  Upsert:  100K × 0.38 ÷ 400 = ~95 seconds
  Delete:  100K × 7 RU ÷ 400  = ~1,750 seconds (~29 min)
  Total:   ~32 minutes per table
```

---

## Nightly Window

```
11:00 PM   Discovery    ~5 min
11:30 PM   Sync starts
             Step 1  BQ fetch         ~1 min
             Step 2A inject timestamp  ~1 sec
             Step 2B Cosmos upsert    ~32 min
             Step 2C delete orphans   ~29 min
             Step 3  cache warm        ~5 sec
~00:32 AM  Complete ✅  (1.5hr buffer before 3AM deadline)
```

---

## Environment Variables

Override any `application.yml` default without editing the file:

```bash
COSMOS_ENDPOINT=https://...
COSMOS_KEY=...
COSMOS_DATABASE=DynamicPipeline
BIGQUERY_PROJECT_ID=...
BIGQUERY_DATASET=records_datasets
DISCOVERY_ENABLED=true
DISCOVERY_CRON="0 0 23 * * *"
SYNC_ENABLED=true
SYNC_CRON="0 30 23 * * *"
SYNC_ON_STARTUP=true
CACHE_HEAP_ENTRIES=350000
COSMOS_INIT_CONTAINERS=true
```

---

## Project Structure

```
src/main/java/com/dataplatform/dynamicdatapipeline/
├── config/
│   ├── CosmosDbConfig.java              # sync + async Cosmos clients
│   ├── DynamicCosmosContainerManager.java  # auto-create containers
│   ├── EhCacheConfig.java               # heap cache, noExpiration
│   └── GlobalExceptionHandler.java      # RFC 7807 error responses
├── controller/
│   ├── GenericRecordController.java     # read API endpoints
│   └── TableAdminController.java        # admin + sync endpoints
├── dto/
│   ├── SyncResult.java                  # sync outcome
│   └── DiscoveryResult.java             # discovery outcome
├── model/
│   └── TableConfig.java                 # table metadata
├── scheduler/
│   ├── TableDiscoveryScheduler.java     # 11PM cron
│   └── GenericSyncScheduler.java        # 11:30PM cron
└── service/
    ├── TableRegistry.java               # in-memory ConcurrentHashMap
    ├── TableDiscoveryService.java       # BQ scan + auto-register
    ├── GenericBigQueryService.java      # fetch + schema detection
    ├── GenericCosmosService.java        # upsertBulk + deleteByLastSyncTime
    ├── GenericDataSyncService.java      # 3-step pipeline orchestrator
    ├── GenericCacheService.java         # EhCache get/put/bulkLoad
    ├── CacheWarmUpService.java          # restart recovery
    └── SchemaValidationService.java     # pre-write validation
```

---

## Common Issues

| Error | Cause | Fix |
|---|---|---|
| `total throughput limit` on container create | Account RU/s ceiling hit | Reduce `default-throughput` in yml, lower existing container in Azure Portal |
| Cache MISS after sync | `heap-entries` < record count | Increase `heap-entries` to `records × 1.2` |
| 500 on admin sync API | `blockLast()` on NIO thread | Already fixed via `publishOn(boundedElastic())` |
| Sync returns 0 records | No `updated_at` column, incremental attempted | Already fixed — falls back to FULL if no `updated_at` |
| Slow sync after 80% records | Partition key hotspot | Ensure `vendor` or `region` is used, not `id` |

---
