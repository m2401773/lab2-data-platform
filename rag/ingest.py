"""RAG стадии 1–3: ingestion -> chunking -> embedding -> запись в pgvector.

1. ingestion: файлы кладутся в S3-бакет docs-raw (landing), оттуда читаются; sha256 -> только новые/изменённые.
2. chunking: по заголовкам разделов (## ...), длинные разделы режутся окном ~700 символов с перекрытием 120.
3. embedding: батчем, вектор + текст + метаданные (документ, раздел) пишутся в rag.chunks одной транзакцией.
"""
import hashlib
import os
import pathlib
import sys

import boto3
import psycopg

from embed import embed, to_pg

DSN = os.getenv("PG_META_DSN", "postgresql://meta:meta@localhost:5434/meta")
s3 = boto3.client("s3", endpoint_url="http://localhost:8333", aws_access_key_id="lab",
                  aws_secret_access_key="labsecret", region_name="us-east-1")
MAX_CHARS, OVERLAP = 700, 120


def chunk(md: str):
    title, section, buf, out = None, None, [], []

    def flush():
        text = " ".join(buf).strip()
        if not text:
            return
        start = 0
        while start < len(text):   # скользящее окно для длинных разделов
            out.append((section, text[start:start + MAX_CHARS]))
            start += MAX_CHARS - OVERLAP
    for line in md.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
        elif line.startswith("## "):
            flush()
            section, buf = line[3:].strip(), []
        elif line.strip():
            buf.append(line.strip())
    flush()
    return title, out


def main(folder="docs"):
    # landing: загрузка исходников в объектное хранилище
    for p in sorted(pathlib.Path(folder).glob("*.md")):
        s3.put_object(Bucket="docs-raw", Key=f"regulations/{p.name}", Body=p.read_bytes())
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="docs-raw", Prefix="regulations/").get("Contents", [])]

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        stats = {"new": 0, "changed": 0, "unchanged": 0, "chunks": 0}
        for key in keys:
            body = s3.get_object(Bucket="docs-raw", Key=key)["Body"].read()
            sha = hashlib.sha256(body).hexdigest()
            cur.execute("SELECT sha256, version FROM rag.documents WHERE doc_id = %s", (key,))
            row = cur.fetchone()
            if row and row[0] == sha:
                stats["unchanged"] += 1
                continue
            title, chunks = chunk(body.decode("utf-8"))
            version = row[1] + 1 if row else 1
            stats["changed" if row else "new"] += 1
            # переиндексация документа целиком: старые чанки удаляются каскадно
            cur.execute("DELETE FROM rag.documents WHERE doc_id = %s", (key,))
            cur.execute("INSERT INTO rag.documents(doc_id, title, doc_type, sha256, version) VALUES (%s,%s,'md',%s,%s)",
                        (key, title, sha, version))
            for i, (section, text) in enumerate(chunks):
                cur.execute("INSERT INTO rag.chunks(doc_id, chunk_no, section, content, embedding) "
                            "VALUES (%s,%s,%s,%s,%s::vector)",
                            (key, i, section, text, to_pg(embed(f"{title}. {section}. {text}"))))
                stats["chunks"] += 1
        conn.commit()
    print("ingest:", stats)


if __name__ == "__main__":
    main(*sys.argv[1:])
