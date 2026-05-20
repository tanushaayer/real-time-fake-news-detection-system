from fastapi import FastAPI
from pydantic import BaseModel
from transformers import pipeline

app = FastAPI(title="Fake News Classification Service")

classifier = pipeline(
    "text-classification",
    model="mrm8488/bert-tiny-finetuned-fake-news-detection"
)

class NewsRequest(BaseModel):
    text: str

@app.get("/")
def health_check():
    return {"status": "NLP service is running"}

@app.post("/classify")
def classify_news(request: NewsRequest):
    text = request.text.strip()

    if not text:
        return {
            "label": "UNKNOWN",
            "confidence": 0.0
        }

    result = classifier(text)[0]

    return {
        "label": result["label"],
        "confidence": float(result["score"])
    }