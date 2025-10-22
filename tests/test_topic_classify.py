# tests/test_topic_classify.py
from topic_detect import classify_topic

def test_classify_technology():
    txt = "The AI and machine learning industry in cloud computing is booming."
    topic = classify_topic(txt)
    assert topic == "technology"

def test_classify_finance():
    txt = "Stock market traders discuss Bitcoin and cryptocurrency investments."
    topic = classify_topic(txt)
    assert topic == "finance"

def test_classify_none():
    txt = "This is a generic sentence with no strong topical keywords."
    topic = classify_topic(txt)
    assert topic == ""
