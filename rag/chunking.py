"""Chunk the internal policy PDF corpus ahead of embedding.

Character-window chunking with overlap, snapping to a nearby sentence or word
boundary so chunks do not cut mid-word.
"""

import glob
import json
import os

from pypdf import PdfReader

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from a PDF, page by page."""
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found at {pdf_path}")

    reader = PdfReader(pdf_path)
    pages = [page.extract_text() for page in reader.pages]
    return "\n\n".join(p for p in pages if p)


def chunk_text_by_chars(
    text: str,
    source_name: str,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
) -> list[dict]:
    """Split text into overlapping character windows."""
    chunks: list[dict] = []
    if not text:
        return chunks

    cleaned_text = " ".join(text.split())
    start = 0

    while start < len(cleaned_text):
        end = start + chunk_size

        # Snap back to a natural boundary within the last 50 characters.
        if end < len(cleaned_text):
            for offset in range(1, min(50, end - start)):
                if cleaned_text[end - offset] in (".", " ", "\n"):
                    end = end - offset + 1
                    break

        chunk = cleaned_text[start:end].strip()
        if chunk:
            chunks.append(
                {
                    "source": source_name,
                    "index": len(chunks),
                    "content": chunk,
                    "length": len(chunk),
                }
            )

        start = end - chunk_overlap
        if start >= len(cleaned_text) or end >= len(cleaned_text):
            break

    return chunks


def chunk_corpus_from_gcs(bucket: str, prefix: str = "policies/") -> int:
    """Download the corpus from GCS, chunk it, and write chunks.json.

    Returns the number of chunks produced. Called by the rag_index_refresh DAG.
    """
    from google.cloud import storage

    client = storage.Client()
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    os.makedirs("/tmp/corpus", exist_ok=True)

    all_chunks: list[dict] = []
    for blob in blobs:
        if not blob.name.lower().endswith(".pdf"):
            continue
        local = os.path.join("/tmp/corpus", os.path.basename(blob.name))
        blob.download_to_filename(local)
        text = extract_text_from_pdf(local)
        all_chunks.extend(chunk_text_by_chars(text, os.path.basename(blob.name)))

    with open("/tmp/chunks.json", "w", encoding="utf-8") as fh:
        json.dump(all_chunks, fh, indent=2, ensure_ascii=False)

    print(f"Wrote {len(all_chunks)} chunks from {len(blobs)} objects.")
    return len(all_chunks)


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    all_chunks = []

    for pdf_path in sorted(glob.glob(os.path.join(current_dir, "corpus", "*.pdf"))):
        name = os.path.basename(pdf_path)
        print(f"Processing {name}...")
        text = extract_text_from_pdf(pdf_path)
        chunks = chunk_text_by_chars(text, name)
        print(f"  {len(text)} chars -> {len(chunks)} chunks")
        all_chunks.extend(chunks)

    out = os.path.join(current_dir, "chunks.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(all_chunks, fh, indent=2, ensure_ascii=False)
    print(f"Saved {len(all_chunks)} chunks to {out}")
