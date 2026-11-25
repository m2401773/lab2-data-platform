"""RAG стадии 4–5: векторный / полнотекстовый / гибридный поиск (RRF) + оценка качества + сборка промпта."""
import json
import os
import sys

import psycopg

from embed import embed, to_pg

DSN = os.getenv("PG_META_DSN", "postgresql://meta:meta@localhost:5434/meta")
K_RRF = 60

SQL_VECTOR = """SELECT chunk_id FROM rag.chunks ORDER BY embedding <=> %(q)s::vector LIMIT %(n)s"""
SQL_FTS = """SELECT chunk_id FROM rag.chunks, websearch_to_tsquery('russian', %(text)s) query
             WHERE tsv @@ query ORDER BY ts_rank_cd(tsv, query) DESC LIMIT %(n)s"""
# если websearch требует все слова и ничего не нашёл — ищем по ИЛИ слов
SQL_FTS_OR = """SELECT chunk_id FROM rag.chunks,
                to_tsquery('russian', array_to_string(tsvector_to_array(to_tsvector('russian', %(text)s)), ' | ')) query
             WHERE tsv @@ query ORDER BY ts_rank_cd(tsv, query) DESC LIMIT %(n)s"""


def ranked(cur, sql, params):
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def search(cur, text, mode="hybrid", k=3):
    p = {"q": to_pg(embed(text)), "text": text, "n": 20}
    vec = ranked(cur, SQL_VECTOR, p) if mode in ("vector", "hybrid") else []
    fts = []
    if mode in ("fts", "hybrid"):
        fts = ranked(cur, SQL_FTS, p) or ranked(cur, SQL_FTS_OR, p)
    if mode != "hybrid":
        return (vec or fts)[:k]
    score = {}
    for lst in (vec, fts):   # Reciprocal Rank Fusion: не нужно сводить шкалы cosine и ts_rank
        for rank, cid in enumerate(lst, 1):
            score[cid] = score.get(cid, 0) + 1 / (K_RRF + rank)
    return sorted(score, key=score.get, reverse=True)[:k]


def doc_of(cur, ids):
    cur.execute("SELECT chunk_id, doc_id FROM rag.chunks WHERE chunk_id = ANY(%s)", (ids,))
    m = dict(cur.fetchall())
    return [m[i] for i in ids]


def evaluate(cur, golden_path="golden_set.json", k=3):
    golden = json.load(open(golden_path, encoding="utf-8"))
    print(f"golden set: {len(golden)} вопросов, метрики Hit@{k} и MRR@{k} (релевантен ли документ)")
    for mode in ("vector", "fts", "hybrid"):
        hits, rr = 0, 0.0
        for g in golden:
            docs = doc_of(cur, search(cur, g["question"], mode, k))
            if g["doc"] in docs:
                hits += 1
                rr += 1 / (docs.index(g["doc"]) + 1)
        print(f"  {mode:7s} Hit@{k}={hits / len(golden):.2f}  MRR@{k}={rr / len(golden):.2f}")


def answer_prompt(cur, question):
    ids = search(cur, question, "hybrid", 3)
    cur.execute("SELECT c.chunk_id, d.title, c.section, c.content FROM rag.chunks c "
                "JOIN rag.documents d USING (doc_id) WHERE chunk_id = ANY(%s)", (ids,))
    ctx = {r[0]: r for r in cur.fetchall()}
    parts = [f"[{n}] {ctx[i][1]} / {ctx[i][2]}: {ctx[i][3]}" for n, i in enumerate(ids, 1)]
    return ("Ответь на вопрос сотрудника ТОЛЬКО по контексту, укажи номера источников. "
            "Если ответа нет — скажи, что не знаешь.\n\nКонтекст:\n" + "\n".join(parts) +
            f"\n\nВопрос: {question}")


if __name__ == "__main__":
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SET hnsw.ef_search = 40")
        if len(sys.argv) > 1:
            print(answer_prompt(cur, " ".join(sys.argv[1:])))
        else:
            evaluate(cur)
