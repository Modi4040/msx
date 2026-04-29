import os
from pathlib import Path

import requests as req
from flask import Flask, jsonify, request, send_from_directory

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
except ImportError:
    pass  # dotenv optional — falls back to system env var

from msx_service import fetch_msx_chart, fetch_msx_dataset


BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__)


@app.get("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/styles.css")
def styles():
    return send_from_directory(BASE_DIR, "styles.css")


@app.get("/app.js")
def script():
    return send_from_directory(BASE_DIR, "app.js")


@app.get("/sample-msx-stocks.csv")
def sample_csv():
    return send_from_directory(BASE_DIR, "sample-msx-stocks.csv")


@app.get("/api/msx/stocks")
def msx_stocks():
    payload = fetch_msx_dataset(BASE_DIR / "sample-msx-stocks.csv")
    return jsonify(payload)


@app.get("/api/msx/stocks/<ticker>/chart")
def msx_stock_chart(ticker: str):
    payload = fetch_msx_chart(ticker.upper())
    return jsonify(payload)


@app.post("/api/ai/analyse")
def ai_analyse():
    ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "ANTHROPIC_API_KEY not found. Create a .env file with ANTHROPIC_API_KEY=sk-ant-... in your project folder."}), 500

    body = request.get_json(force=True, silent=True) or {}
    messages = body.get("messages", [])
    if not messages:
        return jsonify({"error": "No messages provided"}), 400

    try:
        resp = req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": body.get("model", "claude-sonnet-4-20250514"),
                "max_tokens": body.get("max_tokens", 2000),
                "messages": messages,
            },
            timeout=60,
        )
        resp.raise_for_status()
        return jsonify(resp.json())
    except req.exceptions.HTTPError as exc:
        return jsonify({"error": f"Anthropic API error: {exc.response.status_code} — {exc.response.text[:300]}"}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/ai/ollama/status")
def ollama_status():
    """Ollama not available on PythonAnywhere — returns offline."""
    return jsonify({"running": False, "models": [], "message": "Ollama requires a local machine. Use Groq (free) or ChatGPT instead."})


@app.post("/api/ai/chat")
def ai_chat():
    """Ollama not available on PythonAnywhere."""
    return jsonify({"error": "Ollama requires a local machine. Please use Groq (free at console.groq.com) or ChatGPT instead."}), 503


@app.post("/api/ai/chat/groq")
def ai_chat_groq():
    """Proxy to Groq API — free tier, no credit card needed."""
    body = request.get_json(force=True, silent=True) or {}
    api_key = body.get("apiKey", "")
    prompt  = body.get("prompt", "")
    if not api_key:
        return jsonify({"error": "No Groq API key provided. Get one free (no credit card) at console.groq.com"}), 400
    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400
    try:
        resp = req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 512,
                "temperature": 0.7,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        return jsonify({"response": text})
    except req.exceptions.HTTPError as exc:
        try:
            msg = exc.response.json().get("error", {}).get("message", exc.response.text[:300])
        except Exception:
            msg = exc.response.text[:300]
        if "rate" in msg.lower() or "quota" in msg.lower():
            return jsonify({"error": "Groq rate limit hit — wait a moment and try again. Free tier allows ~30 requests/minute."}), 429
        return jsonify({"error": f"Groq error: {msg}"}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.post("/api/ai/chat/openai")
def ai_chat_openai():
    """Proxy to OpenAI ChatGPT API."""
    body = request.get_json(force=True, silent=True) or {}
    api_key = body.get("apiKey", "")
    prompt  = body.get("prompt", "")
    if not api_key:
        return jsonify({"error": "No OpenAI API key provided. Get one at platform.openai.com"}), 400
    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400
    try:
        resp = req.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}], "max_tokens": 600},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        return jsonify({"response": text})
    except req.exceptions.HTTPError as exc:
        msg = exc.response.json().get("error", {}).get("message", exc.response.text[:200])
        return jsonify({"error": f"OpenAI error: {msg}"}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/msx/stock/<ticker>")
def msx_stock_detail(ticker: str):
    """Fetch full data for a specific stock including chart history."""
    ticker = ticker.upper().strip()
    try:
        # Get chart/technical data
        chart = fetch_msx_chart(ticker)

        # Also try to find it in the live dataset
        payload = fetch_msx_dataset(BASE_DIR / "sample-msx-stocks.csv")
        all_stocks = payload.get("stocks", [])
        stock = next((s for s in all_stocks if s.get("ticker") == ticker), None)

        return jsonify({
            "ticker": ticker,
            "stock": stock,
            "chart": chart,
            "found": stock is not None,
        })
    except Exception as exc:
        return jsonify({"ticker": ticker, "error": str(exc), "found": False}), 200


@app.get("/api/ai/status")
def ai_status():
    """Check if server-side AI key is configured."""
    has_groq = bool(os.environ.get("GROQ_API_KEY", ""))
    has_openai = bool(os.environ.get("OPENAI_API_KEY", ""))
    return jsonify({"groq": has_groq, "openai": has_openai})


@app.post("/api/ai/default")
def ai_default():
    """Server-side AI call using GROQ_API_KEY env var — no key needed from browser."""
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
    if not GROQ_API_KEY:
        return jsonify({"error": "GROQ_API_KEY not configured on server."}), 500

    body = request.get_json(force=True, silent=True) or {}
    prompt = body.get("prompt", "")
    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400

    try:
        resp = req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 600,
                "temperature": 0.7,
            },
            timeout=30,
        )
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"]
        return jsonify({"response": text})
    except req.exceptions.HTTPError as exc:
        try:
            msg = exc.response.json().get("error", {}).get("message", exc.response.text[:300])
        except Exception:
            msg = exc.response.text[:300]
        return jsonify({"error": f"Groq error: {msg}"}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    app.run(debug=True)
