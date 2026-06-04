# Sample DynamoDB Queries

A cheat sheet for retrieving the KPIs the pipeline writes. Aligned with the project brief's "Sample queries for retrieving insights from DynamoDB" deliverable.

All examples assume you're running against the develop environment in `eu-west-1`. Adjust table names and region as needed.

---

## Table reference

| Table | Partition key | Sort key | What it stores |
|---|---|---|---|
| `music-streaming-dev-daily-genre-kpis` | `genre` (S) | `date` (S) | Four daily KPIs: `listen_count`, `unique_listeners`, `total_listening_time_ms`, `avg_listening_time_per_user_ms` |
| `music-streaming-dev-top-songs-per-genre` | `genre` (S) | `date_rank` (S, e.g. `"2024-06-25#01"`) | Top 3 songs per (genre, date) — fields: `track_id`, `track_name`, `listen_count`, `rank`, `date` |
| `music-streaming-dev-top-genres-daily` | `date` (S) | `rank` (S, e.g. `"01"`) | Top 5 genres per day — fields: `genre`, `listen_count` |

The `--output table --query ...` pattern flattens DynamoDB's type wrappers (`{"S": "pop"}` → `"pop"`) so you get clean ASCII tables instead of verbose JSON.

---

## Daily genre KPIs — all four metrics

### How did pop perform on a specific day?

```bash
aws dynamodb get-item \
  --table-name music-streaming-dev-daily-genre-kpis \
  --key '{"genre":{"S":"pop"},"date":{"S":"2024-06-25"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Item.{Genre: genre.S,
                  Date: date.S,
                  Listens: listen_count.N,
                  UniqueUsers: unique_listeners.N,
                  TotalMs: total_listening_time_ms.N,
                  AvgMsPerUser: avg_listening_time_per_user_ms.N}'
```

### Every date for one genre

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

### Everything in the table (small tables only — full scan)

```bash
aws dynamodb scan \
  --table-name music-streaming-dev-daily-genre-kpis \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Genre: genre.S,
                     Date: date.S,
                     Listens: listen_count.N,
                     UniqueUsers: unique_listeners.N,
                     AvgMsPerUser: avg_listening_time_per_user_ms.N}'
```

---

## Top 3 songs per genre per day

### Top 3 rock songs on a specific day

The sort key `date_rank` packs the date and the rank, so a `begins_with(date_rank, "2024-06-25#")` filter selects exactly that day's top-3 in order.

```bash
aws dynamodb query \
  --table-name music-streaming-dev-top-songs-per-genre \
  --key-condition-expression "genre = :g AND begins_with(date_rank, :d)" \
  --expression-attribute-values '{":g":{"S":"rock"}, ":d":{"S":"2024-06-25#"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Rank: rank.N,
                     Song: track_name.S,
                     TrackId: track_id.S,
                     Listens: listen_count.N,
                     Date: date.S}'
```

### Top song in each known genre on a date (multi-query)

DynamoDB can't query across partition keys in one call. Loop in shell:

```bash
for GENRE in pop rock jazz hip-hop classical; do
  echo "--- $GENRE ---"
  aws dynamodb get-item \
    --table-name music-streaming-dev-top-songs-per-genre \
    --key "{\"genre\":{\"S\":\"$GENRE\"},\"date_rank\":{\"S\":\"2024-06-25#01\"}}" \
    --region eu-west-1 \
    --output table \
    --query 'Item.{Genre: genre.S, Song: track_name.S, Listens: listen_count.N}'
done
```

### All historical top-3s for one genre

```bash
aws dynamodb query \
  --table-name music-streaming-dev-top-songs-per-genre \
  --key-condition-expression "genre = :g" \
  --expression-attribute-values '{":g":{"S":"pop"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Date: date.S, Rank: rank.N, Song: track_name.S, Listens: listen_count.N}'
```

---

## Top 5 genres per day

### Which genres dominated 2024-06-25?

```bash
aws dynamodb query \
  --table-name music-streaming-dev-top-genres-daily \
  --key-condition-expression "#d = :d" \
  --expression-attribute-names '{"#d":"date"}' \
  --expression-attribute-values '{":d":{"S":"2024-06-25"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Items[*].{Rank: rank.S, Genre: genre.S, Listens: listen_count.N}'
```

> `#d` is needed because `date` is a reserved word in DynamoDB's expression language.

### Just the #1 genre on a date

```bash
aws dynamodb get-item \
  --table-name music-streaming-dev-top-genres-daily \
  --key '{"date":{"S":"2024-06-25"},"rank":{"S":"01"}}' \
  --region eu-west-1 \
  --output table \
  --query 'Item.{Date: date.S, Rank: rank.S, Genre: genre.S, Listens: listen_count.N}'
```

---

## PowerShell variants

Identical commands. Only the inline JSON quoting changes — easiest workaround is to put each JSON blob in a variable first:

```powershell
$KEY = '{":g":{"S":"pop"}}'
aws dynamodb query `
  --table-name music-streaming-dev-daily-genre-kpis `
  --key-condition-expression "genre = :g" `
  --expression-attribute-values $KEY `
  --region eu-west-1 `
  --output table `
  --query 'Items[*].{Date: date.S,
                     Listens: listen_count.N,
                     UniqueUsers: unique_listeners.N,
                     AvgMsPerUser: avg_listening_time_per_user_ms.N}'
```

---

## boto3 (Python) — for scripting / dashboards

For anything beyond one-off CLI usage, boto3's `Table` resource auto-unwraps the type tags:

```python
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")

# Top 3 rock songs for a date
table = dynamodb.Table("music-streaming-dev-top-songs-per-genre")
resp = table.query(
    KeyConditionExpression=(
        Key("genre").eq("rock")
        & Key("date_rank").begins_with("2024-06-25#")
    )
)
for item in resp["Items"]:
    print(f"#{int(item['rank']):02d} {item['track_name']:30s} {item['listen_count']:>6} listens")
```

Output:

```
#01 Bohemian Rhapsody              4321 listens
#02 Smells Like Teen Spirit         2987 listens
#03 Stairway to Heaven              1456 listens
```

### Daily KPI dashboard query

```python
# Average listening time across all pop dates
table = dynamodb.Table("music-streaming-dev-daily-genre-kpis")
resp = table.query(KeyConditionExpression=Key("genre").eq("pop"))

for item in resp["Items"]:
    avg_minutes = float(item["avg_listening_time_per_user_ms"]) / 60_000
    print(f"{item['date']}  avg {avg_minutes:.1f} min/user, {item['unique_listeners']} listeners")
```

---

## Common JMESPath tricks (the `--query` expressions)

| Goal | JMESPath snippet |
|---|---|
| Unwrap a string field | `field.S` |
| Unwrap a number field | `field.N` |
| Rename a column in output | `{Header: field.S}` |
| All items as table rows | `Items[*].{...}` |
| Single item (from get-item) | `Item.{...}` |
| Filter to a subset | `Items[?listen_count.N > '\``100'\``]` |

---

## When to use which approach

| You want to... | Best tool |
|---|---|
| Just see what's there | **AWS Console** → DynamoDB → Tables → Explore items |
| Repeatable CLI query | `aws dynamodb` with `--output table --query` |
| Programmatic access from a script | **boto3** (`dynamodb.Table(...)`) |
| Analytics joins across tables | None of these — export Parquet to Athena |
