import json
import re
import sys
import ollama
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import os

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()

SYSTEM_PROMPT = """You extract board games that are being recommended in forum posts.
Return ONLY valid JSON, no explanation:
{
  "games": [
    {
      "title": "exact game title as mentioned",
      "recommendation_type": "suggestion | opinion | comparison",
      "sentiment": "positive | negative | neutral",
      "evidence": "brief quote or paraphrase showing the recommendation"
    }
  ]
}
If no games are recommended, return: {"games": []}
Only include games that are clearly being recommended, praised, suggested, or compared."""


MAX_WORKERS    = 8     
TEXT_LIMIT     = 1000  
PROGRESS_EVERY = 500   


def extract_games(post: dict) -> list[dict]:
    text = strip_html(post.get("response", ""))
    if not text:
        return []

    response = ollama.chat(
        model="llama3",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Forum post:\n\n{text[:TEXT_LIMIT]}"}
        ]
    )

    raw = response["message"]["content"].strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    parsed = json.loads(raw)
    games = parsed if isinstance(parsed, list) else parsed.get("games", [])

    return [
        {
            "thread_id":           post["metadata"]["thread_id"],
            "category":            post.get("category"),
            "question":            post.get("question"),
            "response_author":     post["metadata"]["response_author"],
            "game_title":          g["title"],
            "recommendation_type": g["recommendation_type"],
            "sentiment":           g["sentiment"],
            "evidence":            g["evidence"],
        }
        for g in games
    ]


def load_already_processed(output_file: str) -> set:
    """Return thread_ids already written so we can skip them on resume."""
    seen = set()
    if not os.path.exists(output_file):
        return seen
    with open(output_file, encoding="utf-8") as f:
        for line in f:
            try:
                seen.add(json.loads(line)["thread_id"])
            except Exception:
                pass
    return seen


def main():
    if len(sys.argv) < 2:
        input_file = input("Enter path to your JSONL file: ").strip()
    else:
        input_file = sys.argv[1]

    output_file = input_file.replace(".jsonl", "_extracted_games.jsonl")

    already_done = load_already_processed(output_file)
    if already_done:
        print(f"Resuming — {len(already_done):,} thread_ids already done, skipping.")

    print(f"Input:   {input_file}\nOutput:  {output_file}\nWorkers: {MAX_WORKERS}")
    print("Loading posts...\n")

    posts = []
    with open(input_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                post = json.loads(line)
                if post.get("metadata", {}).get("thread_id") not in already_done:
                    posts.append(post)
            except json.JSONDecodeError:
                pass

    print(f"Posts to process: {len(posts):,}  (skipped {len(already_done):,} already done)\n")

    write_lock = Lock()
    counters = {"processed": 0, "skipped": 0, "games": 0}

    def process(post):
        try:
            return extract_games(post)
        except Exception as e:
            return e

    with open(output_file, "a", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process, p): p for p in posts}
            for future in as_completed(futures):
                result = future.result()
                with write_lock:
                    counters["processed"] += 1
                    if isinstance(result, Exception):
                        counters["skipped"] += 1
                    else:
                        for r in result:
                            out_f.write(json.dumps(r) + "\n")
                        counters["games"] += len(result)

                    if counters["processed"] % PROGRESS_EVERY == 0:
                        pct = counters["processed"] / len(posts) * 100
                        print(
                            f"  [{pct:5.1f}%] {counters['processed']:,}/{len(posts):,} posts"
                            f"  |  {counters['games']:,} games  |  {counters['skipped']} errors"
                        )

    print(f"\n✓ Done! Posts: {counters['processed']:,} | Games: {counters['games']:,} | Skipped: {counters['skipped']:,}")
    print(f"  Output: {output_file}")

if __name__ == "__main__":
    main()