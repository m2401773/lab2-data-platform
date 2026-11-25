# Практическая №2 — вариант 5: платформа данных в одной архитектуре

CDC→SCD2 · Streaming/Batch (Kappa) · Feature Store · RAG — на примере кикшеринга «Вжух».

- **Отчёт:** [отчет.md](отчет.md)
- **Схема:** [architecture.drawio](architecture.drawio) (PNG: [architecture.png](architecture.png))
- **Стенд:** `docker compose up -d`, полный прогон — `bash scripts/run_all.sh`

| Папка | Что внутри |
|---|---|
| `sql/oltp` | источник: схема, данные, «жизнь справочника» |
| `sql/lakehouse` | Iceberg DDL, запросы «на дату», gold-отчёт, проверки, признаки (PIT-join) |
| `pipelines` | CDC→SCD2 (замена Spark SS), поток событий (замена Flink), генератор событий |
| `features` | материализация в Redis, Feast-определения |
| `rag` | ingest → chunking → embedding → pgvector, гибридный поиск, golden set |
| `docs` | выводы `docker ps` / `docker stats` |
