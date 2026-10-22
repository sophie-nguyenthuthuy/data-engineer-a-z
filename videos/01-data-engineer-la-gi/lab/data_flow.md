# Lab #01 — Sơ đồ luồng dữ liệu của Phin Phố

Điền các ô `TODO` trong sơ đồ dưới. Mỗi mũi tên ghi **công cụ** và **số video** dạy công cụ đó (ví dụ `Spark · #10`).
Tra cứu: [SCHEMA.md](../../../data/generator/SCHEMA.md) cho nguồn, [videos/README.md](../../README.md) cho số video,
docstring trong [bronze.py](../../../platform/ingestion/src/coffee_ingestion/bronze.py) / [silver.py](../../../platform/ingestion/src/coffee_ingestion/silver.py) cho ý nghĩa từng layer.

Render: dán khối mermaid vào https://mermaid.live hoặc xem trực tiếp trên GitHub.

```mermaid
flowchart LR
    subgraph sources["Nguồn (3 hệ thống của Phin Phố)"]
        pos["TODO: tên nguồn 1<br/>dạng: TODO (Postgres? CSV? JSONL?)"]
        app["TODO: tên nguồn 2<br/>dạng: TODO"]
        iot["TODO: tên nguồn 3<br/>dạng: TODO"]
    end

    subgraph lake["Data Lake (MinIO / S3) — bucket lake"]
        bronze["bronze<br/>TODO: layer này làm gì? (1 câu)"]
        silver["silver<br/>TODO: layer này làm gì? (1 câu)"]
        gold["gold<br/>TODO: layer này làm gì? (1 câu)"]
    end

    kafka["TODO: message broker cho app + IoT<br/>video #TODO"]
    warehouse["TODO: nơi BI query (Postgres gold / Trino)<br/>video #TODO"]
    dashboard["Dashboard doanh thu<br/>(ngày × cửa hàng × món × kênh)"]

    %% Batch
    pos -->|"TODO: công cụ ingest batch · #TODO"| bronze
    %% Streaming
    app -->|"TODO: producer · #TODO"| kafka
    iot -->|"TODO: producer · #TODO"| kafka
    kafka -->|"TODO: Structured Streaming · #TODO"| bronze
    pos -.->|"TODO: CDC · #TODO"| kafka

    bronze -->|"TODO: dedup / typing · #TODO"| silver
    silver -->|"TODO: star schema · #TODO"| gold
    gold --> warehouse --> dashboard

    subgraph ops["Vận hành (chạy bên cạnh, không nằm trên đường dữ liệu)"]
        airflow["TODO: orchestrator · #TODO"]
        quality["TODO: data quality · #TODO"]
        lineage["TODO: lineage / observability · #TODO"]
    end
    airflow -.-> bronze
    airflow -.-> silver
    airflow -.-> gold
    quality -.-> silver
    lineage -.-> gold
```

## Câu hỏi tự kiểm (trả lời ngắn ngay dưới đây)

1. Vì sao bronze **không** sửa gì cả, kể cả khi biết dữ liệu sai?
   > TODO

2. Dữ liệu `tz_drift` (ts thiếu `+07:00`) được sửa ở layer nào? Ai phát hiện nếu không sửa?
   > TODO

3. Nếu dashboard sai số, bạn đi ngược sơ đồ từ đâu tới đâu?
   > TODO
