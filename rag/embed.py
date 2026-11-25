"""Заглушка эмбеддинг-модели для стенда (без GPU и гигабайтов весов).

Feature hashing: псевдо-стеммы слов (первые 5 букв) + символьные триграммы -> 384 измерения, L2-нормировка.
В целевой архитектуре здесь сервис эмбеддингов (bge-m3 / multilingual-e5, 1024d) — интерфейс тот же:
текст -> вектор. Вся остальная часть пайплайна (чанкинг, pgvector, HNSW, гибрид, оценка) настоящая.
"""
import hashlib
import math
import re

DIM = 384
WORD = re.compile(r"[а-яёa-z0-9]+", re.I)


def _h(token: str) -> tuple[int, int]:
    d = hashlib.md5(token.encode()).digest()
    return int.from_bytes(d[:4], "little") % DIM, 1 if d[4] % 2 else -1


def embed(text: str) -> list[float]:
    v = [0.0] * DIM
    for w in WORD.findall(text.lower()):
        if len(w) < 3:
            continue
        stem = w[:5]
        i, s = _h("w:" + stem)
        v[i] += 2.0 * s
        for k in range(len(stem) - 2):
            i, s = _h("t:" + stem[k:k + 3])
            v[i] += 0.5 * s
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def to_pg(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.5f}" for x in v) + "]"
