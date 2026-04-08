# extract_games.py
import json
import re
import sys
import ollama

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

def extract_games(post: dict) -> list[dict]:
    text = strip_html(post.get("response", ""))
    if not text:
        return []

    response = ollama.chat(
        model="llama3",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Forum post:\n\n{text[:1000]}"}
        ]
    )

    raw = response["message"]["content"].strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    parsed = json.loads(raw)

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
        for g in parsed.get("games", [])
    ]

def main():
    # Get input file from command line, or prompt if not provided
    if len(sys.argv) < 2:
        input_file = input("Enter path to your JSONL file: ").strip()
    else:
        input_file = sys.argv[1]

    # Auto-name output file based on input filename
    output_file = input_file.replace(".jsonl", "_extracted_games.jsonl")

    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print("Starting extraction...\n")

    results = []
    skipped = 0
    total = 0

    with open(input_file) as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                post = json.loads(line)
                games = extract_games(post)
                results.extend(games)
            except (json.JSONDecodeError, KeyError) as e:
                skipped += 1
                print(f"  Line {i+1} error: {e}")
            if (i + 1) % 10 == 0:
                print(f"  Processed {i+1} posts → {len(results)} games found so far...")

    with open(output_file, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    print(f"\n✓ Done!")
    print(f"  Posts processed:      {total}")
    print(f"  Posts skipped:        {skipped}")
    print(f"  Games extracted:      {len(results)}")
    print(f"  Output saved to:      {output_file}")

if __name__ == "__main__":
    main()