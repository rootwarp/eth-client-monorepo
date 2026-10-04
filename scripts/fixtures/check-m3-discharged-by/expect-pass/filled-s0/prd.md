# fixture — S0/S1/S2-claimed cells filled; later-stage patches may stay blank

## 5. Requirement ledger

### 5.1 P0 — ship-blocking

| ID | Requirement | Disposition | Discharged by |
|---|---|---|---|
| **P0-01** | drop publishes | `patch @ S0` | `aaaaaaaaaaaa` |
| **P0-15** | engine deadlines | `patch @ S0 → deleted @ S1` | `aaaaaaaaaaaa` → S1 |
| **P0-16** | DA signaling | `wire @ S3` | |
| **P0-25** | kzg none | `patch @ S1` | `eeeeeeeeeeee` |

#### 5.1.2 P0-19 — disposition detail

| Q3 change | Size | Disposition | Discharged by |
|---|---|---|---|
| 1 — top-up | S | `patch @ S0` | `bbbbbbbbbbbb` |
| 3 — move cache | M | `patch @ S2` | `ffffffffffff` |

### 5.2 P1 — required

#### P1-A — Medium correctness

| # | Location | Requirement | Disposition | Discharged by |
|---|---|---|---|---|
| 1 | serve.rs | frontier bind | `patch @ S0` | `S0-B-10` |
| 2 | backfill.rs | admission | `patch @ S2` | `S2-B-07` |

#### P1-B — Medium quality

| # | Location | Requirement | Disposition | Discharged by |
|---|---|---|---|---|
| 8 | column.rs | fork-schedule walks | `patch @ S4` | `cccccccccccc` (C-11, at S0) |
| 9 | storage_client.rs | invalidate | `deleted @ S2` | `S2` |

#### P1-F — Decision records

| ID | Requirement | Disposition | Discharged by |
|---|---|---|---|
| 1 | slashing ADR | **write @ S0** (the ADR); implement @ S5 | `dddddddddddd` |
