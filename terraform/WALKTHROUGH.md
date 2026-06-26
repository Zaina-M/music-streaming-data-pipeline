# Music Streaming ETL Pipeline — A Beginner's Walkthrough

A complete plain-English guide to what this project does, why it's built the way it is, and how to talk about it to someone who has never written a line of code.

---

## Table of Contents

1. [The 30-second pitch](#the-30-second-pitch)
2. [The story behind the project](#the-story-behind-the-project)
3. [The restaurant analogy](#the-restaurant-analogy-the-whole-system-in-one-picture)
4. [Reading the architecture diagram](#reading-the-architecture-diagram)
5. [The architecture, piece by piece](#the-architecture-piece-by-piece)
6. [The serialization layer (why it exists)](#the-serialization-layer-why-it-exists)
7. [Partition pruning: how Load stays fast forever](#partition-pruning-how-load-stays-fast-forever)
8. [Failure handling: the DLQ and alarms](#failure-handling-the-dlq-and-alarms)
9. [Quality: tests and hardening](#quality-tests-and-hardening)
10. [Why these specific choices?](#why-these-specific-choices)
11. [What you need to know](#what-you-need-to-know-to-work-with-this)
12. [How to deploy it](#how-to-deploy-it-step-by-step)
13. [How to test it](#how-to-test-the-pipeline)
14. [What success looks like](#what-success-looks-like)
15. [Troubleshooting](#troubleshooting-common-issues)
16. [How to explain this to a non-technical person](#how-to-explain-this-to-a-non-technical-person)
17. [Future improvements](#future-improvements)
18. [Glossary](#glossary-jargon-translated)

---

## The 30-second pitch

> Imagine a music streaming app like Spotify. Every time a user listens to a song, a record gets created: *who listened, to what, when*. By the end of the day there are millions of these records. The business wants daily reports answering questions like *"what was the most-listened-to genre today?"* and *"which songs are trending in pop?"*
>
> This project is the automated machine that takes those raw listening records, joins them with information about the songs and the users, calculates the daily statistics, and stores the results in a place where the rest of the company can read them — all without anyone clicking a button.

**The entire system runs itself. You drop a file in. Hours later, reports come out.**

---

## The story behind the project

Picture a music streaming company. Three times a day — at 10 AM, 2:30 PM, and 7:45 PM — their app exports a file listing every song every user played in the last few hours. We call these **stream files**: `streams1.csv`, `streams2.csv`, `streams3.csv`.

These files alone aren't useful. They look like this:

```
user_id, track_id, listen_time
26213,   abc123,   2024-06-25 17:43:13
6937,    xyz789,   2024-06-25 07:26:00
```

Just IDs and timestamps. The business doesn't care which user ID listened to which track ID — they want **insights**:

- "What genre got the most listens today?"
- "What are the top 10 songs in rock this week?"
- "Are users in their 20s listening to different music than users in their 50s?"

To answer those questions, the IDs need to be **joined** with two reference files:

- **songs.csv** — for each track ID, tells you the artist, album, genre, danceability, etc.
- **users.csv** — for each user ID, tells you their name, age, country.

Once joined, you can compute the daily statistics ("KPIs" — Key Performance Indicators) and store them somewhere the business intelligence team can read.

**That whole journey — from raw files to readable insights — is what this project automates.**

---

## The restaurant analogy: the whole system in one picture

To picture the pipeline, imagine a busy restaurant kitchen:

| Restaurant role | What it represents in our system |
|---|---|
| **Loading dock** where deliveries arrive | The S3 "raw" bucket (where stream files land) |
| **Pantry** with permanent ingredients (salt, oil, flour) | The S3 "reference" prefix (songs.csv, users.csv — they don't change much) |
| **The bell** that rings when a delivery arrives | EventBridge (notifies the system) |
| **The ticket queue** at the kitchen window | SQS FIFO queue (one ticket at a time) |
| **The expediter** who hands tickets to the chef in order | Pipeline-trigger Lambda |
| **The head chef** who decides the cooking order | Step Functions (orchestrator) |
| **The receiving clerk** who inspects the delivery | Glue Validate job (data quality check) |
| **The prep cook** who chops & mixes ingredients | Glue Transform job (joins streams with songs/users) |
| **The plating chef** who finishes the dish | Glue Load job (computes KPIs, writes to database) |
| **The menu board** showing today's specials | DynamoDB (the 3 KPI tables) |
| **The freezer** where used packaging goes | S3 archive bucket (processed files moved here) |
| **The smoke alarm** | CloudWatch + SNS (alerts when something fails) |

When a delivery arrives at the loading dock, the bell rings. The head chef hears it and assigns work in order: inspect → prep → cook → plate. If anything goes wrong, the smoke alarm sounds. After the meal is plated, the packaging gets tossed in the freezer for storage.

That's the entire system. Everything below is the detailed version.

---

## Reading the architecture diagram

The architecture diagram at the top of the README isn't decorative — every visual choice in it carries information. If you can read the diagram, you understand the pipeline. Here's the vocabulary.

![Pipeline architecture](docs/pipeline-architecture.png)

### The five zones (left to right, then bottom strip)

The diagram is split into colour-coded zones, each one a layer of responsibility:

| Zone | Colour | What lives here | What it does |
|---|---|---|---|
| **1. Ingest** | Blue | Raw bucket, Reference bucket, EventBridge (S3 object created) | The "front door" — where files arrive and how the system notices |
| **2. Serialization** | Yellow | SQS FIFO, DLQ, Trigger Lambda, Dispatcher Lambda, EventBridge (SFN status change) | The one-at-a-time guarantee — turns a chaotic stream of arrivals into an orderly single-file queue |
| **3. Orchestration + Compute** | Green | Step Functions, 3 Glue jobs, Archive Lambda, Scripts bucket | The actual work — the four-step pipeline that runs per file |
| **4. Storage + Output** | Grey | Processed bucket, 3 DynamoDB tables, Archive bucket | Where results land — Parquet for replay, DynamoDB for queries, Archive for done-files |
| **5. Observability** | Pink (full-width strip across the bottom) | CloudWatch, SNS Alerts | Watches the four zones above; not on the data path |

The zones are arranged so a stream file's journey reads **left to right** — Zone 1 receives it, Zone 2 queues it, Zone 3 processes it, Zone 4 stores the results. Zone 5 sits underneath, watching everything but never blocking the flow.

### The three arrow styles

Once you know the zones, the arrows tell you *what kind of relationship* connects two boxes:

| Style | Meaning | Example |
|---|---|---|
| **Solid black, 90° elbows** | Data flow — a file, message, or write is being passed | `Raw bucket → EventBridge → SQS FIFO`, `Transform → Processed`, `Load → DynamoDB` |
| **Solid black, curved arcs** | Side-loop — a control signal, not a data hand-off | The three arrows around the Dispatcher Lambda: `Step Functions → EventBridge (status change) → Dispatcher → SQS`. Visually distinct so it doesn't read as "main flow goes backwards." |
| **Dashed red** | Observability — an alarm or metric, not part of the pipeline | DLQ depth, Step Functions ExecutionsFailed, Glue failed-task all feed CloudWatch as dashed red lines; CloudWatch → SNS is also dashed |

If you trace only the solid black orthogonal arrows from left to right, you'll get the happy-path journey of one stream file from upload to KPI. The curved arrows show *how the system stays responsive* (the dispatcher closes the latency gap). The dashed arrows show *how the system tells humans when something breaks*.

### The labels that carry the most weight

Most arrows are unlabelled because their endpoints make the meaning obvious. The labels that *are* present mark the moments where something non-obvious happens:

- **`StartExecution`** (Trigger Lambda → Step Functions) — the actual API call that kicks off a pipeline run.
- **`redrive after 20 retries`** (SQS FIFO → DLQ) — encodes the failure-tolerance policy. After 20 failed deliveries (~3 hours of self-recovery), the message gives up and lands in the DLQ.
- **`on SUCCEEDED / FAILED / TIMED_OUT`** (SFN → EventBridge status change) — the trigger conditions for the dispatcher wake-up.
- **`long-poll 20s`** (Dispatcher → SQS) — the technique that closes the visibility-timeout gap.
- **`Parquet + manifest`** (Transform → Processed) — Transform writes two things, not one. The manifest is the partition-pruning hand-off (see below).
- **`read manifest, prune partitions`** (Processed → Load) — Load's input list is the manifest, not the whole bucket. This is what keeps Load's runtime constant as history grows.
- **`upsert`** (Load → DynamoDB) — last-writer-wins semantics; combined with the serialization layer, this gives correct overwrites with no race.
- **`songs + users join`** (Reference bucket → Transform) — the only inbound arrow into the Glue cluster from outside Zone 3; reminds the reader that reference data is read at Transform time, not pre-loaded.

### What the diagram does *not* show (deliberately)

A few real components are excluded to keep the diagram readable:

- **IAM roles.** Every Lambda, Glue job, and Step Functions execution runs under a least-privilege IAM role. Drawing them would double the box count without adding flow information. They live in `modules/iam/main.tf`.
- **VPC / networking.** All services here use AWS-managed endpoints. There's no custom VPC to draw.
- **The state bucket.** Terraform state lives in its own S3 bucket (bootstrapped by `scripts/setup-backend.{sh,ps1}`). It's not part of the runtime pipeline, so it sits outside the diagram.
- **Per-day Parquet partitions** (`listen_date=YYYY-MM-DD/`). They're inside the Processed bucket — represented by one icon, but in reality one folder per day.

Knowing what's *not* drawn matters as much as knowing what is — it's how you'd answer a reviewer who asks *"but where's IAM in your diagram?"* without scrambling.

---

## The architecture, piece by piece

### 1. S3 — the storage warehouses

**What S3 is**: Amazon's file storage service. Think of it as Google Drive but for programs, not people. You don't browse files visually — your code reads and writes them.

We have **four** S3 buckets (a "bucket" is just a top-level folder):

1. **Raw bucket** (`music-streaming-raw`) — where new stream files land. Also holds the static `songs.csv` and `users.csv` under a `reference/` prefix.
2. **Processed bucket** (`music-streaming-processed`) — where the joined, enriched data is saved in a fast format called **Parquet**.
3. **Archive bucket** (`music-streaming-archive`) — where stream files go *after* they've been successfully processed. Like a "done" folder.
4. **Scripts bucket** (`music-streaming-scripts`) — where the Python code that runs the ETL is stored. Glue downloads the code from here when it runs.

**Why four buckets and not one?**

- **Security**: each bucket has different permissions. The Lambda function can only write to `archive`, not `raw`. The Glue jobs can read but not delete from `raw`. Bugs are contained.
- **Clarity**: looking at the bucket name tells you immediately what kind of data lives there.
- **Lifecycle policies**: the archive bucket can be set to move old files to cheap "cold storage" automatically; the processed bucket keeps things hot for queries. Different rules for different needs.

---

### 2. EventBridge — the doorbell

**What EventBridge is**: AWS's notification system. It listens for "events" happening across AWS (like "a new file was uploaded to S3") and lets you say *"when X happens, do Y."*

In our system, EventBridge has one job: **when a new file appears in the raw bucket, drop a message onto the SQS FIFO queue.**

(In a simpler version of this system, EventBridge would talk directly to Step Functions. We added an SQS queue in between for a specific reason — see [The serialization layer](#the-serialization-layer-why-it-exists) below.)

**Why use EventBridge instead of just running on a schedule?**

A schedule (e.g., "run at 11 AM, 3 PM, 8 PM") would be brittle — if the stream file arrives late at 10:15 instead of 10:00, the pipeline starts before the file is there and crashes. EventBridge waits for the *actual file arrival* and reacts instantly. This is called an **event-driven** architecture.

---

### 3. SQS FIFO Queue + Pipeline-Trigger Lambda — the kitchen ticket window

**What SQS is**: Amazon's queue service. Producers drop messages onto the queue; consumers take them off, one at a time. Think of a deli with a "take a ticket" dispenser at the door.

**What FIFO means**: "First In, First Out." Messages come out in the same order they went in, and crucially, **only one message is "in flight" at a time per group**. Standard SQS doesn't guarantee order or one-at-a-time — FIFO does.

The flow now:

```
EventBridge  →  SQS FIFO  →  pipeline-trigger Lambda  →  Step Functions
```

The Lambda's only job is to peel one message off the queue and start a Step Functions execution — only if no other execution is already running. If one *is* running, the Lambda tells SQS *"hold this message, I'll try again later."*

Why all this just to call Step Functions? See the next section.

---

### 4. Step Functions — the head chef

**What Step Functions is**: A "state machine." Think of it as a flowchart that AWS actually runs for you. You define the steps and the rules ("if step 2 fails, jump to step 6"), and AWS executes them, remembering where it is even if something crashes.

Our state machine has 4 steps in order:

```
Validate → Transform → Load → Archive
```

If any step fails, the whole pipeline stops and an alert fires. If all 4 succeed, the stream file gets moved to the archive bucket.

**Why use Step Functions instead of just chaining the scripts together?**

Because **failures are normal**. A Glue job might run out of memory. Network blips happen. Without an orchestrator, a failure halfway through leaves your data in a half-finished state with no clear way to recover. Step Functions gives you:

- **Visibility** — a visual dashboard showing exactly which step ran, when, and how long it took.
- **Retries** — automatic re-attempts on transient failures.
- **Audit trail** — every execution is logged. You can answer "what happened on June 25?" months later.

---

### 5. AWS Glue — the cooks

**What Glue is**: AWS's managed data processing service. You write a Python script that says *"read this CSV, do these transformations, write the result here,"* and Glue spins up a cluster of computers to run it, then shuts them down when done. You only pay for the runtime.

We have **3 Glue jobs**, each with one responsibility:

#### Glue Job 1: Validate (the receiving clerk)
- **Type**: Python shell (lightweight — runs on a fraction of a single computer).
- **Job**: Open the new stream file and check it carefully. Are the column names right? Is anything blank? Do the dates parse correctly? If anything is wrong, refuse the delivery immediately and alert the chef.
- **Why a separate validation job?**: Catching bad data *before* spinning up an expensive Spark cluster saves money and makes errors easier to debug.

#### Glue Job 2: Transform (the prep cook)
- **Type**: PySpark (heavy compute — runs on a cluster).
- **Job**: Read the stream file. Read `songs.csv` and `users.csv` from the reference folder. **Join** them — meaning, for each row in the stream file, look up the song details and the user details and merge them into one wide row. Then save the result in a fast file format called Parquet, organized by date.
- **Side effect**: Right before finishing, Transform writes a tiny JSON file called a **dates-touched manifest** to `s3://<processed_bucket>/manifests/<file>.json`. It just lists which `listen_date` partitions this run produced. Load reads that file next, so it knows which folders to refresh and which to leave alone. This is the **partition-pruning hand-off** (see below).
- **Why PySpark?**: Joining millions of rows is heavy work. PySpark (Apache Spark in Python) splits the work across multiple computers in parallel.

#### Glue Job 3: Load (the plating chef)
- **Type**: PySpark.
- **Job**: Read the manifest Transform just wrote, open ONLY the date folders it lists, then compute the 3 KPI summaries:
  - **Daily genre KPIs** per (genre, date): listen count, unique listeners, total listening time, and average listening time per user.
  - **Top 3 songs per genre per day**.
  - **Top 5 genres per day**.
  - Then write each summary into its DynamoDB table.
- **Why the manifest detour?**: Without it, Load would have to scan the *entire* `processed/` bucket every run — fine on day 1, slow by month 3, broken by year 1. The manifest tells Load exactly which days actually changed, so its runtime stays **constant** regardless of how much history accumulates. See "Partition pruning" below for the full picture.

**Why separate the 3 jobs instead of one big script?**

- **Single responsibility**: if Validate fails, you know it's a data quality issue. If Transform fails, you know it's a join issue. Diagnosis becomes fast.
- **Cost**: Validate uses cheap Python shell. Only Transform/Load pay for Spark clusters.
- **Retry granularity**: if Load fails, you don't have to re-do the expensive Transform — Step Functions can retry Load alone.

---

### 6. DynamoDB — the menu board

**What DynamoDB is**: Amazon's "NoSQL" database. Unlike a normal database where you write SQL queries, DynamoDB is a giant key-value store: you give it a key, it gives you back a value. Extremely fast, scales automatically, and you only pay for the requests you make.

We have **3 tables**, each holding one of the KPI summaries:

1. **daily-genre-kpis** — answers *"how did genre X perform on date Y?"*
2. **top-songs-per-genre** — answers *"what are the top songs in genre X?"*
3. **top-genres-daily** — answers *"what were the top genres on date Y?"*

**Why DynamoDB instead of a regular SQL database?**

- The downstream readers (a dashboard, a recommendation engine, an analytics team) want **fast lookups** by key (e.g. *"give me top songs for rock"*). DynamoDB does that in single-digit milliseconds.
- No server to manage. No capacity to size. AWS handles it.
- It costs almost nothing when idle, which matches our usage pattern: a burst of writes 3 times a day, then quiet.

**What is "upsert"?**

If the Load job runs twice for the same day (say, you re-ran the pipeline to fix a bug), we don't want two copies of every row in the database. Instead, the second run should **overwrite** the first. That's an *upsert* — "update if exists, insert if not." DynamoDB does this automatically as long as the keys match, which is why we carefully chose the key schemas to encode date + genre.

---

### 7. Lambda (Archive) — the cleanup crew

**What Lambda is**: A way to run a small piece of code on demand without managing any server. You write a function, AWS runs it when called, and you pay only for the milliseconds it actually executes.

Our Lambda has one tiny job: when Step Functions says *"all steps succeeded,"* the Lambda copies the processed stream file from the raw bucket to the archive bucket, then deletes the original.

**Why Lambda for this and not another Glue job?**

- Moving a small file is way too cheap to justify spinning up Glue.
- Lambdas start in milliseconds. Glue takes a minute or two to bootstrap.

---

### 8. CloudWatch + SNS — the smoke alarm

**What CloudWatch is**: AWS's monitoring service. Every other AWS service automatically publishes metrics (like "number of failed Glue jobs") to CloudWatch. You can set **alarms** — *"if metric X goes above value Y, do Z."*

**What SNS is**: Amazon's notification service. Send a message to SNS, it fans the message out to all subscribers (email, SMS, another Lambda, whatever).

In our pipeline:
- CloudWatch watches the Glue jobs and the Step Functions state machine.
- If anything fails, CloudWatch publishes to our SNS topic.
- SNS forwards the message to the email address you configured.

**Why a separate alerting layer?**

Because **silent failures are the worst kind**. If a Glue job dies and nobody knows, you're sitting on broken data thinking everything is fine. The smoke alarm ensures humans get a poke when something needs attention.

---

## The serialization layer (why it exists)

This is the most subtle and important design detail in the whole project. If you understand this section, you understand the difference between a hobby pipeline and a production-grade one.

### The problem

Imagine three stream files arrive in S3 within 30 seconds of each other (`streams1.csv` at 10:00:00, `streams2.csv` at 10:00:15, `streams3.csv` at 10:00:25).

EventBridge fires three events almost simultaneously. If those events went **directly** to Step Functions, you'd get **three pipeline executions running in parallel**. They would all:

1. Read different stream files (fine)
2. Read the same `songs.csv` and `users.csv` (fine — S3 handles concurrent reads)
3. Write Parquet to the same date partition (fine — each Spark task gets a unique filename)
4. Compute KPIs from the same partition (problem)
5. Upsert to DynamoDB with the **same primary keys** (e.g., `genre="pop", date="2024-06-25"`) — **catastrophe**

DynamoDB's `PutItem` is last-writer-wins. Whichever Load job happens to finish last completely overwrites the others. **Streams2 and streams3's data silently disappears from the daily KPIs.**

No error. No alert. Just wrong numbers.

This is the #1 way real ETL pipelines produce subtly broken data.

### The fix: serialize triggers with SQS FIFO

We sit a **FIFO queue** between EventBridge and Step Functions. SQS FIFO promises two things:

1. **Order is preserved** — messages come out in the exact order they went in.
2. **One message in flight at a time** — per "MessageGroupId," SQS will never deliver a second message until the first is acknowledged.

We assign every message the same MessageGroupId (`"pipeline"`), so all S3 events end up in the same single-threaded queue.

### The layers of defense

| Layer | What it does | When it's active |
|---|---|---|
| **1. SQS FIFO + single MessageGroupId** | Only one message in flight at a time | Always — primary guarantee |
| **2. Lambda reserved concurrency = 1** | AWS will never run two copies of the trigger Lambda at once | **Optional** — disabled by default because new AWS accounts have a Lambda quota of 10 and AWS won't let you reserve concurrency below an unreserved floor of 10. Set `var.reserved_concurrency = 1` if your account quota is ≥ 11 |
| **3. ListExecutions check inside the Lambda** | Lambda peeks at Step Functions before starting; defers if anything is running | Always — catches manual console runs or CLI-started executions |

Layer 1 alone is sufficient for SQS-driven traffic: FIFO with `batch_size=1` simply will not deliver a second message until the first is acknowledged. Layer 2 is belt-and-braces in case AWS were to ever spin up parallel Lambda instances for the same source. Layer 3 catches the only attack vector layer 1 can't see — someone bypassing SQS and triggering Step Functions directly.

### What actually happens now when 3 files arrive at once

```
10:00:00  streams1.csv arrives → EventBridge → SQS message #1 → in flight
10:00:01  Lambda receives msg #1 → no execution running → starts Pipeline #1
          → returns success → SQS deletes msg #1

10:00:15  streams2.csv arrives → EventBridge → SQS message #2 → in flight
10:00:16  Lambda receives msg #2 → sees Pipeline #1 RUNNING → throws
          → SQS holds msg #2 (invisible for 10 minutes)

10:00:25  streams3.csv arrives → EventBridge → SQS message #3 → queued behind #2

10:06:00  Pipeline #1 finishes

10:10:16  msg #2's visibility timeout expires → SQS redelivers
10:10:17  Lambda receives msg #2 → no execution running → starts Pipeline #2
          → returns success → SQS deletes msg #2

10:16:30  Pipeline #2 finishes

10:16:31  Lambda receives msg #3 → no execution running → starts Pipeline #3
          → returns success → SQS deletes msg #3
```

**All three files get processed, in order, with no overlap, no data loss, no human intervention.** That's the whole purpose of the serialization layer.

### The latency trade-off (and how the dispatcher Lambda fixes it)

The naïve version of this pattern has a real cost: when the trigger Lambda sees an execution running and raises, SQS hides the message for the **visibility timeout** before retrying it. With a 10-minute visibility timeout, a queued file could sit invisible for 10 minutes *after* the previous pipeline finished. Pure waste.

We close that gap with a second Lambda — the **dispatcher** — wired to an EventBridge rule on `Step Functions Execution Status Change`. The instant a pipeline reaches a terminal state (SUCCEEDED, FAILED, TIMED_OUT, ABORTED), EventBridge fires, the dispatcher wakes up, and it **long-polls SQS for up to 20 seconds** — catching the queued message the moment SQS releases it.

Combined with a 60-second visibility timeout (instead of 10 minutes), the worst-case wait after a pipeline ends drops from **~10 minutes to ~40 seconds**, and the common case is essentially instant.

```
                                  EventBridge rule
                                 (status change to
                                  SUCCEEDED / FAILED /
                                  TIMED_OUT / ABORTED)
                                          │
   Step Functions ──ends──────────────────┘
        ↓
        └─► Dispatcher Lambda
                ↓
                ├─► sfn.list_executions  (defense in depth: skip if already running)
                ├─► sqs.receive_message  (long-poll, 20s)
                ├─► sfn.start_execution  (only if a message was returned)
                └─► sqs.delete_message
```

Two consumers now share the queue — the original SQS event-source-mapped trigger Lambda *and* the EventBridge-driven dispatcher. They never fight over the same message because SQS FIFO guarantees exactly one in-flight delivery per `MessageGroupId`. Whichever Lambda asks first wins; the other gets an empty response.

---

## Partition pruning: how Load stays fast forever

### The problem we'd hit without it

The processed bucket has one folder per day:

```
processed/enriched_streams/
├── listen_date=2024-06-25/
├── listen_date=2024-06-26/
├── listen_date=2024-06-27/
└── ... (one folder per day forever)
```

Every time a new file arrives, Load needs to recompute KPIs for the day(s) that file contributed to. A naive Load would just open the **whole folder tree** and process every date it finds. That's fine on day 1 (one folder). On day 365 it's reading 365 folders to do work for one. Eventually a single Load takes longer than the gap between files, and the queue backs up forever.

### The fix: tell Load exactly which folders to open

Transform already knows which dates its file produced rows for — it just wrote those Parquet files. So at the end of Transform, we collect those dates and drop them in a tiny JSON file:

```
s3://<processed_bucket>/manifests/streams/streams1.csv.json
   { "dates": ["2024-06-26"] }
```

Step Functions then passes the same manifest path to Load. Load:

1. Reads the manifest → knows the dates to refresh
2. Filters its Parquet scan: `WHERE listen_date IN (…dates from manifest…)`
3. Spark's **partition pruning** kicks in — because the Parquet was written with `partitionBy("listen_date")`, the filter physically prunes the S3 listing. Other days' folders are never opened.

```
   New file → Transform writes folder 2024-06-26
                       │
                       ├─► writes manifest:  manifests/<file>.json  →  {"dates":["2024-06-26"]}
                       │
                       ▼
                     Load
                       │
                       ├─► reads manifest → ["2024-06-26"]
                       │
                       ├─► open processed/enriched_streams/
                       │     ✖️ skip listen_date=2024-06-25
                       │     ✅  read listen_date=2024-06-26
                       │     ✖️ skip listen_date=2024-06-27
                       │     ...
                       │
                       └─► compute KPIs for 2024-06-26 → upsert DynamoDB
```

### What this costs you in scale

| History size | Load read (without pruning) | Load read (with pruning) |
|---|---|---|
| 7 days | 350 MB | ~50 MB |
| 30 days | 1.5 GB | ~50 MB |
| 365 days | 18 GB | ~50 MB |
| 5 years | 90 GB | ~50 MB |

Load runtime stays **constant regardless of how much history accumulates** — only the per-day file count matters, and that grows much more slowly.

### Why we recount the whole day rather than incrementally adding

When a new file arrives for a day that already had data, Load doesn't just add the new file's contribution on top of yesterday's totals — it recomputes the whole day from the full folder. Why?

Because some KPIs aren't additive across batches:

| KPI | Adds across batches? |
|---|---|
| `listen_count` | ✅ yes |
| `total_listening_time_ms` | ✅ yes |
| `unique_listeners` | ❌ no — same user across batches counts once, not twice |
| `avg_listening_time_per_user_ms` | ❌ no — derived from the two above |
| Top 3 songs / Top 5 genres | ❌ no — need full per-day ranking |

So we use one consistent rule: **read the whole bucket for the manifested dates, recompute every KPI from scratch, overwrite the DynamoDB row**. Always correct, no special cases per KPI.

### Why this is safe to ship

- **No race condition**: each Load only ever writes to its own day's DynamoDB rows. Pure overwrite semantics, no read-modify-write.
- **Crash-safe**: if Load fails mid-run, Step Functions retries it. The retry reads the same manifest, recomputes from the same folder, overwrites the same rows. Idempotent.
- **Manifest as contract**: Load never guesses which dates to touch. If the manifest is missing, Load fails loudly instead of silently defaulting to a full scan.

### One unlock this enables for later

With partition pruning in place, each Load only touches its own day's row in DynamoDB. That means *different days* can be processed in parallel safely — same-day work still has to serialize (race rule), but two files for two different days could run side by side. The change to enable that is a one-line edit to `MessageGroupId` (from `"pipeline"` to the file's date). Documented as a follow-up; not enabled today.

---

## Failure handling: the DLQ and alarms

A pipeline that quietly drops failures is worse than no pipeline at all. This section explains how the system makes sure that **every failure leads to a human being told**.

### What is a Dead-Letter Queue (DLQ)?

A DLQ is a "graveyard" queue. When a message in the main queue fails to be processed too many times, SQS automatically moves it to the DLQ. The main queue stays clean; the failed messages don't keep retrying forever; and a human can later go look at the DLQ to figure out what went wrong.

Think of it as a hospital's "needs further investigation" tray. Routine cases go through normally. Anything weird gets set aside for a specialist to look at later.

### How it's wired up

```
S3 → EventBridge → SQS FIFO ──→ Trigger Lambda → Step Functions
                       │
                       └─(after N failed retries)─→ DLQ
                                                     │
                                                     └─→ CloudWatch alarm
                                                          │
                                                          └─→ SNS email
```

When does a message get sent to the DLQ?
- The trigger Lambda raises an exception (most often because a previous pipeline is still running).
- SQS re-delivers the message after the visibility timeout.
- This repeats up to `maxReceiveCount` times (set to 20 in our config).
- After that, SQS gives up and moves the message to the DLQ.

20 retries × 10-minute visibility timeout = ~3 hours of self-recovery before we ask a human to step in. That's deliberate: routine "queue backed up" situations heal themselves; only true bugs end up in the DLQ.

### Three layers of failure alerting

There are three places the pipeline can break, and we alarm on each one:

| What breaks | What detects it | Where the alert comes from |
|---|---|---|
| A Glue job runs and reports failed tasks | CloudWatch metric `glue.driver.aggregate.numFailedTasks` per job | Per-job alarm → SNS |
| The Step Functions execution as a whole fails (timeout, Lambda crash, retry exhaustion) | CloudWatch metric `ExecutionsFailed` | State-machine alarm → SNS |
| The trigger Lambda can't dispatch — message ends up in DLQ | CloudWatch metric `ApproximateNumberOfMessagesVisible` on the DLQ | DLQ alarm → SNS |

All three alarms feed the same SNS topic, so you only manage one email subscription. The alarm names tell you immediately which layer broke when you get an email.

### The recovery flow

When you get a DLQ alarm:
1. Open the AWS Console → SQS → the `*-pipeline-dlq` queue
2. Use "Send and receive messages" → "Poll for messages" to inspect the failed message body
3. The body is the original EventBridge event, so you know exactly which S3 file was the trigger
4. Decide: replay the file (re-upload it), discard it (it was a bad upload), or fix a bug and replay
5. After fixing, purge the DLQ — you'll get an "OK" email from CloudWatch confirming the alarm cleared

The "OK" notification is important. Without it, you'd never know the system was healthy again.

---

## Quality: tests and hardening

Two things separate a working pipeline from a *trusted* pipeline: **unit tests** that catch regressions, and **defensive coding** that limits the blast radius of bugs and misconfiguration.

### Unit tests

There's a `tests/` folder at the project root with full coverage of the testable logic:

| Component | Test file | What it checks |
|---|---|---|
| Archive Lambda | `tests/lambda_tests/test_archive.py` | S3 move semantics with moto-mocked AWS |
| Trigger Lambda | `tests/lambda_tests/test_pipeline_trigger.py` | Idle → starts execution; busy → defers |
| Validate Glue job | `tests/glue_tests/test_validate.py` | Header / row / timestamp validation |
| Transform Glue job | `tests/glue_tests/test_transform.py` | Inner join correctness, listen_date derivation |
| Load Glue job | `tests/glue_tests/test_load.py` | KPI math for all 3 DynamoDB tables |

How they're structured:
- **No AWS account required** — `moto` mocks boto3 in-process for the Lambdas.
- **No Glue runtime required** — the production scripts have been refactored so the business logic lives in pure functions (`validate_csv`, `enrich_streams`, `compute_daily_genre_kpis`, etc.) that take inputs in and return outputs out, with no I/O.
- **PySpark tests use a local SparkSession** — slower (~10s cold start) but real, so they catch genuine PySpark behavior issues.

Running them is one command (identical in PowerShell, Git Bash, and WSL):

```bash
pytest tests                   # everything
pytest tests/lambda_tests      # fast — ~2 seconds
pytest tests -m "not spark"    # skip Spark tests in CI
```

The full guide lives at [tests/README.md](tests/README.md).

### Defensive coding in the production scripts

Even with good tests, **production code should be defensive**. Two specific guardrails are baked into the Lambdas:

#### 1. Explicit boto3 timeouts (fail fast)

By default, every boto3 client waits up to 60 seconds for a TCP connect and another 60 seconds for a response. If S3 has a bad day, your Lambda could hang for 2 minutes on a single API call — by which point the function's own timeout has fired and you've spent money for nothing useful.

We override these defaults:

```python
_BOTO_CONFIG = Config(
    connect_timeout=5,   # 5s instead of 60s
    read_timeout=10,     # 10s instead of 60s
    retries={"max_attempts": 3, "mode": "standard"},
)
s3 = boto3.client("s3", config=_BOTO_CONFIG)
```

If S3 is slow, we fail in ~15 seconds (5s connect + 10s read) instead of 2 minutes, and our 3 standard-mode retries give us a fair shot at success on a single transient blip without amplifying the wait.

#### 2. `ExpectedBucketOwner` (confused-deputy protection)

Every S3 call passes `ExpectedBucketOwner=<our account ID>`. If somehow the event payload pointed us at a bucket name that lives in a different AWS account (bucket-sniping, misconfiguration, malicious event injection), the call **fails immediately** instead of reading from or writing to the wrong account.

```python
s3.copy_object(
    CopySource=copy_source,
    Bucket=archive_bucket,
    Key=object_key,
    ExpectedBucketOwner=_ACCOUNT_ID,
    ExpectedSourceBucketOwner=_ACCOUNT_ID,
)
```

This is the recommended hardening for cross-account-safe S3 code. The account ID is looked up once at Lambda cold start and reused — it's not in the event payload, so a bad event can't forge it.

#### 3. Least-privilege IAM (already covered, restated for completeness)

Each service has a separate role that grants only the actions it actually needs. The trigger Lambda can't write to S3. The archive Lambda can't start Step Functions. If any one component is compromised, the blast radius is bounded by what *that role* could do, not what the whole pipeline could do.

---

## Why these specific choices?

Here are the design decisions you might be asked to defend.

### "Why not just write a Python script on a server that runs every few hours?"

That would work for a weekend project. For a production system you'd lose:
- **Reliability**: if the server crashes, the script never runs. AWS-managed services have built-in retries and durability.
- **Scaling**: a single server can't process a 100GB file. Spark distributes the work.
- **Observability**: no built-in dashboard showing what ran when and what failed.
- **Cost efficiency**: a server runs 24/7 even when idle. This pipeline only spends money during the few minutes per day it's actually working.

### "Why Terraform instead of clicking through the AWS console?"

The AWS console is fine to **explore**. It's terrible to **maintain**:
- Clicks aren't repeatable. If you deploy to a new account, you have to remember every click.
- Clicks aren't reviewable. There's no diff showing what changed last week.
- Clicks aren't reversible safely. You delete a bucket on Wednesday and discover on Friday you actually needed it.

Terraform turns infrastructure into **code** — versionable, reviewable, repeatable. The same `terraform apply` command can build identical environments for dev, staging, and prod.

### "Why split everything into modules?"

Imagine if all 600 lines of Terraform lived in one file. Every change risks breaking something unrelated. By splitting into `s3/`, `glue/`, `iam/`, etc.:
- You read only the bit relevant to your change.
- Modules can be reused in other projects (the `s3` module is generic).
- Permissions for one team to edit one module don't bleed into others.

### "Why least-privilege IAM instead of one role for everything?"

If you give every service `AdministratorAccess`, a single bug or compromised credential can wipe out your entire AWS account. With least privilege, the Lambda can write to `archive` but not `raw`, so even a malicious payload can't delete your raw data. **Security is layered — assume one layer will fail.**

### "Why EventBridge instead of S3 event notifications going straight to SQS?"

S3 can drop events on SQS directly — no EventBridge in the middle. It's simpler in line count. But three things push us to EventBridge for this pipeline:

1. **Module modularity.** `aws_s3_bucket_notification` is a *single* Terraform resource that owns **all** notifications on a bucket. If we put notification config on the bucket, the `s3` module has to know the SQS queue ARN — meaning `modules/s3` depends on `modules/sqs`. With EventBridge, the bucket just flips `eventbridge = true` and stays oblivious to who's listening. The wiring lives in `modules/eventbridge`, which depends on SQS. Clean one-way dependency instead of a cycle, and we can rebuild either module without dragging the other along.

2. **Multiple subscribers without re-wiring.** If a metrics consumer or a second pipeline later wants to listen for `streams/*.csv` arrivals, with S3 notifications you edit the bucket's single notification block (and risk clobbering the existing rule, since it's one resource). With EventBridge you add another rule — independent, no coordination, no risk of stepping on the existing one.

3. **Richer event pattern matching.** S3 notification filters are prefix/suffix only, one filter per event type. EventBridge content patterns let you match on `detail.object.key` with `prefix` / `suffix` / `anything-but`, combine multiple conditions, and target by event name (`Object Created` vs. `Object Deleted` vs. specific PUT vs. multipart-complete). Not needed today, but cheap insurance against future filter changes.

**What we pay for it:** ~half a second of extra latency end-to-end (EventBridge fan-out) and one more service in the failure surface. For a batch ETL pipeline with a 5–8 minute end-to-end SLA, both are noise.

The tipping point would be a latency-critical, single-subscriber, single-team pipeline — there S3 → SQS direct is the simpler call. This pipeline is the opposite shape, so EventBridge wins.

---

## What you need to know to work with this

You don't need to be an AWS expert. But understanding these basics will help:

### Concepts (the "what")
- **What S3 is** — file storage in the cloud.
- **What IAM is** — the permissions system in AWS. "Roles" are like name badges that say what services can do what.
- **What a Glue job is** — a Python script that AWS runs for you on demand.
- **What Step Functions is** — a flowchart that AWS executes for you.
- **What DynamoDB is** — a fast key-value database.

### Tools (the "how")
- **AWS CLI** — the command-line tool for AWS. You use it to upload files, check state, etc.
- **Terraform** — the tool that turns the `.tf` code files into actual AWS resources.
- **Python basics** — enough to read the Glue scripts and understand the flow.
- **PowerShell** (or any shell) — to run commands.

### Skills (the "doing")
- **Reading error messages** — when something breaks, the message usually tells you exactly what's wrong. Don't skim it.
- **Using the AWS console for debugging** — even when deploying with Terraform, the console is invaluable for *looking at* what got built.
- **Following a stack trace** — Glue jobs log to CloudWatch. Knowing how to find and read those logs is essential.

---

## How to deploy it (step by step)

> **Heads up**: these commands cost real money (a few dollars in dev, more in prod). Always run `terraform destroy` when you're done experimenting.

### Step 1: Install the tools

You need a few things on your machine:

1. **Terraform** — download from https://terraform.io
2. **AWS CLI** — download from https://aws.amazon.com/cli
3. **Python 3** on PATH (used by the backend teardown script)
4. **An AWS account** with credentials configured (`aws configure`)

Check they work (same commands in PowerShell, Git Bash, and WSL):

```bash
terraform -version
aws sts get-caller-identity
```

The second command should print your AWS account ID and user. If it doesn't, fix your credentials before going further.

### Step 2: Bootstrap the remote state backend (one-time, per environment)

The project keeps its Terraform state in S3 with native locking. Before the first `terraform init`, run the bootstrap script — it creates the state bucket and writes `envs/develop/backend.tf` with the real values.

**PowerShell (Windows):**
```powershell
cd "C:\Users\YourName\Desktop\LAB_1\terraform"
.\scripts\setup-backend.ps1
# To target another env / region:
# .\scripts\setup-backend.ps1 -Env develop -Region us-east-1
```

**Bash / Git Bash / WSL:**
```bash
cd "C:/Users/YourName/Desktop/LAB_1/terraform"
./scripts/setup-backend.sh
```

The script is idempotent — running it again does nothing if the bucket already exists.

### Step 3: Set your variables

Each environment has its own folder under `envs/`. We work in `develop`:

**PowerShell:**
```powershell
cd envs\develop
Copy-Item terraform.tfvars.example terraform.tfvars
```

**Git Bash / WSL:**
```bash
cd envs/develop
cp terraform.tfvars.example terraform.tfvars
```

Open `terraform.tfvars` and **fill in your email** under `alert_email`. This is where failure notifications will be sent. Everything else has sensible defaults.

### Step 4: Initialize Terraform

Same in either shell — `terraform` commands don't care:

```bash
# Still in envs/develop
terraform init
```

This downloads the AWS provider plugin and configures the remote backend. Run once per environment.

### Step 5: See what will be built (dry run)

```bash
terraform plan
```

This shows a long list of resources Terraform *will* create. **Read it.** You'll see things like:
- `aws_s3_bucket.raw will be created`
- `aws_dynamodb_table.daily_genre_kpis will be created`
- ... around 40 resources in total.

Nothing has been built yet. This is just the preview.

### Step 6: Actually build everything

```bash
terraform apply
```

Type `yes` when prompted. Wait 2-5 minutes while AWS provisions everything.

When it finishes, you'll see green output and a list of `Outputs:` showing your bucket names, state machine ARN, and trigger queue URL.

### Step 7: Confirm your email subscription

Check your inbox. AWS will have sent an email titled *"AWS Notification - Subscription Confirmation."* Click the **"Confirm subscription"** link. **Failure alerts will not be delivered until you do this.**

---

## How to test the pipeline

There are two kinds of testing here. The fast one (no AWS), and the real one (drop a file, watch it work).

### Fast: run the unit tests locally

Same commands in PowerShell, Git Bash, and WSL:

```bash
# From terraform/
pip install -r tests/requirements-test.txt   # one-time

# Lambda tests — fast (~2 seconds), no Spark
pytest tests/lambda_tests

# Glue tests — slower (~15 seconds, Spark startup)
pytest tests/glue_tests

# Everything
pytest tests
```

The Lambda tests use `moto` to fake AWS in-process. The Glue tests use a local PySpark session against the pure functions inside the Glue scripts. **Neither needs an AWS account.** Run these before every deployment to catch regressions in seconds, not minutes.

### Real: trigger the deployed pipeline

Time to drop a file in and watch the system come alive.

**PowerShell:**
```powershell
# From envs\develop, get the raw bucket name from Terraform outputs
$RAW_BUCKET = terraform output -raw raw_bucket_name

# Upload streams1.csv under the streams/ prefix (the EventBridge filter requires it)
aws s3 cp "..\..\..\Project 1 -- ETL with s3, dynamo and glue\data\streams\streams1.csv" `
          "s3://$RAW_BUCKET/streams/streams1.csv"
```

**Git Bash / WSL:**
```bash
# From envs/develop, get the raw bucket name from Terraform outputs
RAW_BUCKET=$(terraform output -raw raw_bucket_name)

# Upload streams1.csv under the streams/ prefix (the EventBridge filter requires it).
# Quote the path because of the spaces in the folder name.
aws s3 cp "../../../Project 1 -- ETL with s3, dynamo and glue/data/streams/streams1.csv" \
          "s3://$RAW_BUCKET/streams/streams1.csv"
```

### Watch it run

Open the AWS Console in your browser. Go to **Step Functions → State machines → music-streaming-dev-pipeline** (or `music-streaming-<your-environment>-pipeline` — the suffix matches the `environment` variable in your tfvars).

You should see a new execution appear within ~15 seconds. Click it. You'll watch the steps light up:

- Validate (grey → blue → green, ~30 seconds)
- Transform (grey → blue → green, ~2 minutes)
- Load (grey → blue → green, ~2 minutes)
- Archive (grey → blue → green, ~5 seconds)

### Verify the output

Go to **DynamoDB → Tables → music-streaming-dev-daily-genre-kpis** and click **Explore items**. You should see rows like:

| genre | date | listen_count | unique_listeners |
|---|---|---|---|
| pop | 2024-06-25 | 1234 | 567 |
| rock | 2024-06-25 | 891 | 432 |

Check the other two tables the same way.

Go to **S3 → buckets → music-streaming-dev-archive-...**. Your `streams1.csv` should now be there. Check the raw bucket — it's gone from `streams/` because the Archive Lambda moved it.

---

## Querying the KPI data

Once data lands in DynamoDB, you'll want to read it. There are three sensible ways, ranked by ease of use:

### 1. The AWS Console (best for browsing)

Open **DynamoDB → Tables → click a table → "Explore items" tab**. You get a built-in spreadsheet view, sortable and filterable, no commands to remember. This is the right tool when you're poking around to see *what's there*.

### 2. The AWS CLI with `--output table` (best for repeatable queries)

The raw CLI output is JSON with annoying type wrappers — every value is `{"S": "pop"}` instead of just `"pop"`. Two flags fix this:

- `--output table` — renders as ASCII table
- `--query 'Items[*].{Header: field.S}'` — JMESPath expression that reaches through the type wrappers and lets you name columns

### Quick reference: the three KPI tables

| Table | Partition key | Sort key | Typical question it answers |
|---|---|---|---|
| `daily-genre-kpis` | `genre` | `date` | "How did pop perform on June 25?" |
| `top-songs-per-genre` | `genre` | `date_rank` | "What are the top 3 rock songs on June 25?" |
| `top-genres-daily` | `date` | `rank` | "What were the top genres on June 25?" |

For the complete cheat sheet — including `get-item` lookups, PowerShell variants, multi-genre loops, and JMESPath tricks — see [docs/sample-queries.md](docs/sample-queries.md).

### Example queries (bash)

#### How did each day go for pop music? (all 4 daily KPIs)

```bash
aws dynamodb query \
  --table-name music-streaming-dev-daily-genre-kpis \
  --key-condition-expression "genre = :g" \
  --expression-attribute-values '{":g":{"S":"pop"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Date: date.S,
                     Listens: listen_count.N,
                     UniqueUsers: unique_listeners.N,
                     TotalMs: total_listening_time_ms.N,
                     AvgMsPerUser: avg_listening_time_per_user_ms.N}'
```

You'll get something like:

```
-------------------------------------------------------------------------------
|                                    Query                                    |
+------------+-----------+--------------+-------------+----------------------+
|    Date    | Listens   | UniqueUsers  |  TotalMs    |    AvgMsPerUser      |
+------------+-----------+--------------+-------------+----------------------+
|  2024-06-25|    4      |     3        |   900000    |       300000         |
|  2024-06-26|    1      |     1        |   200000    |       200000         |
+------------+-----------+--------------+-------------+----------------------+
```

#### What are the top 3 rock songs on a specific date?

The sort key `date_rank` packs the date and the rank position together (e.g. `"2024-06-25#01"`), so a `begins_with` filter selects exactly that day's top-3 in order:

```bash
aws dynamodb query \
  --table-name music-streaming-dev-top-songs-per-genre \
  --key-condition-expression "genre = :g AND begins_with(date_rank, :d)" \
  --expression-attribute-values '{":g":{"S":"rock"}, ":d":{"S":"2024-06-25#"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Rank: rank.N, Song: track_name.S, Listens: listen_count.N}'
```

#### Which genres dominated on June 25?

```bash
aws dynamodb query \
  --table-name music-streaming-dev-top-genres-daily \
  --key-condition-expression "#d = :d" \
  --expression-attribute-names '{"#d":"date"}' \
  --expression-attribute-values '{":d":{"S":"2024-06-25"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Date: date.S, Rank: rank.S, Genre: genre.S, Listens: listen_count.N}'
```

> **Why the weird `#d` thing?** `date` is a reserved word in DynamoDB's expression language — the same as `SELECT` or `FROM` in SQL. You can't use it directly. `ExpressionAttributeNames` aliases it: `#d` in the expression resolves to `"date"` in the actual call.

#### See everything in a small table

`scan` reads every row in the table. Fine for tiny tables, slow and expensive for big ones:

```bash
aws dynamodb scan \
  --table-name music-streaming-dev-top-genres-daily \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Date: date.S, Rank: rank.S, Genre: genre.S, Listens: listen_count.N}'
```

### PowerShell variants

Identical commands; only the inline JSON quoting changes. The easiest trick is to put each JSON blob in a variable first:

```powershell
$KEY = '{":g":{"S":"pop"}}'
aws dynamodb query `
  --table-name music-streaming-dev-daily-genre-kpis `
  --key-condition-expression "genre = :g" `
  --expression-attribute-values $KEY `
  --region eu-west-1 `
  --output table `
  --query 'Items[*].{Genre: genre.S, Date: date.S, Listens: listen_count.N, Users: unique_listeners.N}'
```

### 3. boto3 (Python) — the cleanest for scripts

If you find yourself running the same query repeatedly, write a small Python script. boto3's `Table` resource auto-unwraps the type tags, so `item["rank"]` is just `"01"` instead of `{"S": "01"}`:

```python
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")

# Top genres for one date
table = dynamodb.Table("music-streaming-dev-top-genres-daily")
resp = table.query(KeyConditionExpression=Key("date").eq("2024-06-25"))
for item in resp["Items"]:
    print(f"#{item['rank']} {item['genre']:10s} {item['listen_count']:>6} listens")
```

Output:

```
#01 pop              4 listens
#02 rock             2 listens
#03 jazz             1 listens
```

### When to use which

| You want to... | Use |
|---|---|
| Just see what's there | **Console** → Explore items |
| Run the same query repeatedly | **CLI with `--output table`** |
| Build a dashboard or report | **boto3** in a Python script |
| Run analytics joins across tables | None of the above — export to S3 / Athena |

---

## Performance and latency expectations

The brief calls this "real-time," but the inputs arrive as discrete batch files at irregular intervals. That makes the pipeline **micro-batch / event-driven**, not true streaming. There's no Kinesis here, and that's the right call — true streaming infrastructure for the described data shape would be over-engineered.

What "real-time" *does* mean here, concretely:

| Metric | Target |
|---|---|
| **Trigger latency** — S3 upload → Step Functions execution start | < 30 seconds (EventBridge + SQS + Lambda dispatch) |
| **Pipeline duration** — Validate → Transform → Load → Archive end-to-end | 5–8 minutes for typical stream files (~1 MB CSV, ~50k rows) |
| **End-to-end SLA** — S3 upload → KPIs queryable in DynamoDB | **< 10 minutes** for a single file with no queue ahead of it |
| **Queue-drain latency** — additional file behind a running pipeline | < 1 minute after the running pipeline ends (thanks to the SFN-complete dispatcher) |

Where most of the time goes:

```
Validate     ~30s    (Python shell, lightweight)
Transform    ~2-3min (Spark cluster cold-start dominates)
Load         ~2-3min (Spark cluster cold-start again)
Archive      ~5s     (Lambda)
───────────────────
total        ~5-7 min
```

About 80% of the pipeline duration is Glue cluster boot — the actual data work takes seconds. At higher data volumes (10×–100× more rows), the boot cost stays the same; only the data-processing portion grows. So the pipeline scales much better than the small-sample timings suggest.

### When this becomes a problem

If you start expecting **bursts** of many files arriving simultaneously (e.g. backfilling a month of history), the serialized-execution model means total time scales linearly with file count. At that point, switch to a **Step Functions Distributed Map** so a single pipeline run processes many files in parallel — see `Future improvements` for the design.

---

## What success looks like

When everything works, every file you drop into the raw bucket produces:

1. A green Step Functions execution within ~5 minutes.
2. Parquet files appearing in the processed bucket under `enriched_streams/listen_date=YYYY-MM-DD/`.
3. New rows in all 3 DynamoDB tables.
4. The original CSV moved from raw → archive.
5. **No** email alerts.

If you got all 5 of those, you have a working data pipeline.

---

## Troubleshooting common issues

### "Access Denied" errors during apply

Your AWS credentials don't have enough permissions to create some resource. Run `aws sts get-caller-identity` to confirm which user/role is being used, then make sure that identity has `AdministratorAccess` (for development) or the specific permissions for IAM, S3, Glue, etc.

### Step Functions execution stuck on "Validate" with a red X

Click the failed Validate task → click "Step output" or "CloudWatch logs." The error message will tell you what column was missing or what value was malformed. Fix the source CSV and re-upload.

### No execution appears after uploading a file

Two likely causes:
1. You uploaded outside the `streams/` prefix. EventBridge only triggers on `streams/*` keys.
2. The S3 → EventBridge notification didn't get enabled. Re-run `terraform apply` to make sure.

### "I never got the SNS confirmation email"

Check spam. If still missing, go to **SNS → Topics → music-streaming-dev-alerts → Subscriptions** in the console and re-trigger the confirmation manually.

### Glue job fails with "Out of memory"

The default 2 Spark workers may not be enough for your data volume. Increase `number_of_workers` in `modules/glue/main.tf` and re-apply.

---

## How to explain this to a non-technical person

Here's a script you can use almost verbatim:

> "It's an automated system that takes raw listening data from our app — basically a list of who-played-what — and turns it into business reports.
>
> Three times a day, the app dumps a file of all the recent song plays into a cloud folder. The moment the file lands, our system notices and **drops a ticket into a queue**. A small dispatcher hands those tickets to the main pipeline **one at a time**, in order. The pipeline then runs a four-step process:
>
> 1. **Check the file** to make sure it's not corrupted.
> 2. **Look up the details** of each song and each user, so we're not just dealing with anonymous IDs.
> 3. **Calculate the day's statistics** — top songs per genre, busiest genres, listening trends.
> 4. **Save those statistics** in a fast database that powers our dashboards and reports.
>
> The queue and the one-at-a-time rule are important: without them, two files landing seconds apart would cause two pipelines to run at the same time and silently overwrite each other's results. With them, the system is guaranteed to produce correct numbers regardless of how files arrive.
>
> The whole thing runs by itself. We don't push a button. We don't sit and watch. If anything goes wrong, we get an email. Otherwise, by the end of each day, the latest numbers are sitting in the database waiting for the analytics team.
>
> It's all built using infrastructure-as-code, which means the entire system — every server, every database, every permission — is described in text files. We can rebuild the whole thing from scratch in another region in 10 minutes if we had to."

That's the elevator pitch. Adjust to the audience.

---

## Future improvements

These are things a real team would do over time. The list is shorter now than it used to be — items already in the codebase are marked **DONE** for context, the rest are next steps.

### Reliability
- **DONE — Dead-Letter Queue + CloudWatch alarm**: messages that fail max retries land in a DLQ, and an alarm pages the on-call when that happens.
- **DONE — Pipeline serialization**: SQS FIFO + reserved Lambda concurrency ensures one execution at a time, preventing the parallel-run race condition.
- **Add input checksumming**: verify the stream file wasn't truncated mid-upload by comparing checksums.
- **Add a Step Functions DLQ**: separate from the SQS DLQ — when the *state machine itself* fails terminally, route the input to a queue for replay.

### Cost
- **Use Glue auto-scaling**: instead of fixed 2 workers, let Glue scale up and down based on data size.
- **Compress everything**: stream files arrive as plain CSV; compressing to gzip on upload would cut S3 storage by ~70%.

### Observability
- **Add a Grafana dashboard**: pull metrics from CloudWatch into a single pane showing pipeline health over time.
- **Add data quality metrics**: track *how much* data each run processed, so you can spot anomalies (e.g. "today's file is half the usual size").
- **Add structured JSON logging**: instead of plain print statements, emit JSON logs so CloudWatch Logs Insights queries are easier.

### Security
- **DONE — Least-privilege IAM**: each service has its own role scoped to exactly the actions it needs.
- **DONE — `ExpectedBucketOwner` on every S3 call**: confused-deputy guardrail; calls fail if the bucket lives in a different account.
- **DONE — Explicit boto3 timeouts**: Lambdas fail fast on slow API calls instead of burning their function timeout waiting.
- **Use customer-managed KMS keys** instead of AWS-managed encryption — lets you rotate keys on your own schedule.
- **VPC-isolate the Glue jobs** — run them inside a private network so they can never reach the public internet by accident.

### Scaling
- **Use Kinesis Firehose** for streaming ingestion instead of batch files — gets latency from hours down to minutes.
- **Add a real-time path** parallel to the batch path: Kinesis → Lambda → DynamoDB for "live" KPIs alongside the historical ones.

### Code quality
- **DONE — Unit tests** for Lambdas (moto) and Glue scripts (pure functions + local PySpark). See `tests/` and `tests/README.md`.
- **DONE — Multi-environment layout**: `envs/develop/` ready to copy to `envs/staging/` or `envs/prod/`.
- **DONE — Remote state with native locking**: bootstrapped via `scripts/setup-backend.{sh,ps1}`.
- **Set up CI/CD**: have GitHub Actions run `pytest` + `terraform plan` on every pull request so reviewers see both the test result and the infra diff.
- **Add `tflint` and `checkov`**: static analysis catches bad Terraform patterns and security issues before they land in main.

---

## Glossary: jargon translated

| Term | Plain English |
|---|---|
| **ETL** | "Extract, Transform, Load" — the three steps of moving data from messy source to clean destination. |
| **IaC** | Infrastructure as Code — describing your servers/databases/etc. in text files instead of clicking around. |
| **Bucket** | An S3 top-level folder. |
| **Object** | A file in S3. |
| **Prefix** | A folder path inside a bucket (e.g. `streams/`). |
| **ARN** | "Amazon Resource Name" — a unique identifier for any AWS resource (looks like `arn:aws:s3:::my-bucket`). |
| **Role** | A set of permissions that AWS services use to access other AWS services. |
| **Trust policy** | The rule that says *who is allowed to assume a role*. |
| **State machine** | A flowchart that Step Functions executes. |
| **SQS** | Amazon's queue service. Messages get put in by producers, taken out by consumers. |
| **FIFO** | "First In, First Out." A queue that preserves order and processes one message at a time per group. |
| **MessageGroupId** | An SQS FIFO label that says *"these messages must be processed in order, one at a time."* Different group IDs can be processed in parallel. |
| **Visibility timeout** | After SQS hands a message to a consumer, how long it stays hidden from other consumers. If the consumer doesn't ack in time, the message becomes visible again. |
| **DLQ** | Dead-letter queue — where messages go if they fail too many times. A staging area for manual investigation. |
| **redrive policy** | The SQS setting that says "after N failed receives, move this message to the DLQ." Our N is 20. |
| **moto** | Python library that mocks AWS APIs in-process. Lets you run boto3 code locally with no AWS account. |
| **Confused deputy** | A security vulnerability where one service is tricked into doing something on behalf of an attacker against another service. `ExpectedBucketOwner` is the standard guardrail. |
| **ExpectedBucketOwner** | An S3 API parameter that says "fail unless this bucket lives in account X." Stops cross-account mistakes / attacks. |
| **botocore.Config** | The boto3 settings object — used here to set explicit connect/read timeouts and retry behavior on AWS clients. |
| **fail fast** | A design philosophy: when something goes wrong, raise an error immediately instead of degrading silently. Easier to diagnose. |
| **Reserved concurrency** | A Lambda setting that caps how many copies of the function can run simultaneously. Setting it to 1 = strict serialization. |
| **Race condition** | A bug where two operations running in parallel produce wrong results depending on which finishes first. Always non-deterministic, always painful to debug. |
| **Glue job** | A Python script that AWS Glue runs for you, on a cluster it manages. |
| **DPU** | "Data Processing Unit" — a unit of compute capacity in Glue. |
| **Parquet** | A column-oriented file format. Much faster to query than CSV when you only need some of the columns. |
| **Partition** | A sub-folder of data organized by some key (e.g. by date). Lets queries scan only the relevant subset. |
| **PySpark** | Apache Spark, controlled from Python. The standard way to do big-data processing. |
| **Broadcast join** | A Spark join where the smaller table is sent in full to every worker, avoiding a slow data shuffle. |
| **Upsert** | "Update if exists, insert if not." Avoids duplicates when the same logical row is written twice. |
| **NoSQL** | A category of database that doesn't use SQL. DynamoDB is the most popular AWS NoSQL service. |
| **Event-driven** | A system that reacts to things happening, rather than running on a fixed timer. |
| **Idempotent** | An operation you can safely run multiple times — the second run doesn't break anything or duplicate data. |
| **Least privilege** | Security principle: give each component only the permissions it strictly needs, nothing more. |
| **Module** | A reusable chunk of Terraform code with its own variables and outputs. |
| **State file** | A file Terraform keeps that maps your `.tf` code to the actual AWS resources it created. |
| **Apply / Destroy** | The two main Terraform commands. Apply creates/updates resources; destroy removes them. |
| **Plan** | A dry-run of what Terraform *would* do if you applied. Always read it before applying. |

---

**That's the whole project.** If you read this top to bottom and ran the deployment steps, you now understand a production-grade AWS data pipeline — not as a black box, but as a system of cooperating parts where every piece has a reason for being there.
