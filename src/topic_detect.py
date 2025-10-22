# Minimal keyword-based topic classifier (small, pluggable)
TOPIC_KEYWORDS = {
    "technology": ["technology", "tech", "computer", "software", "hardware", "ai", "machine learning", "cloud"],
    "news": ["breaking", "journal", "report", "newsroom", "headline", "press"],
    "sports": ["score", "match", "team", "player", "goal", "tournament", "scoreboard"],
    "finance": ["stock", "market", "finance", "investment", "bank", "trader", "cryptocurrency", "crypto"],
    "health": ["health", "medical", "doctor", "hospital", "disease", "wellness"],
    "entertainment": ["movie", "music", "concert", "film", "celebrity", "tv show", "series"],
    # add more small topical keyword lists as needed
}

def classify_topic(text: str, min_matches: int = 1) -> str:
    """
    Very small topic classifier:
    - lowercases text and counts keyword matches per topic.
    - returns the topic with the highest match count if >= min_matches.
    - returns '' (empty) if no topic meets min_matches.
    """
    if not text:
        return ''
    s = text.lower()
    best_topic = ''
    best_score = 0
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = 0
        for kw in keywords:
            # simple substring match; could be improved with tokenization/word boundaries
            if kw in s:
                score += 1
        if score > best_score:
            best_score = score
            best_topic = topic
    if best_score >= min_matches:
        return best_topic
    return ''
