# 2–3 minute demo recording — checklist

The rubric wants three terminals side-by-side with predictions printing live.
This is the script I use; it lands a clean 2:30 take.

## Before you hit record

- `pip install -r requirements.txt` inside an activated venv
- `.env` filled with Confluent Cloud creds
- `python -m scripts.create_topics` already run
- `python -m src.train` already run (so `models/fraud_model.joblib` exists)
- Test each terminal command individually once to confirm zero errors
- Resize three terminal windows so they sit side-by-side, ~90 cols each
- Close Slack/Mail/notifications; turn on Do Not Disturb

## Take outline (≈2:30)

**0:00 – 0:20  Intro**
Brief on-camera (or voiceover): "This is my ENGR 5785G Assignment 1 — real-time credit card fraud detection. Kafka on Confluent Cloud, Faust for the streams processor, a Random Forest trained on the Kaggle dataset."

**0:20 – 0:40  Show the repo**
Quick scroll through `src/` in VS Code. Pause on `stream_processor.py` and point at the `@app.agent(raw_topic)` decorator — call out "this is the Faust Streams API."

**0:40 – 1:00  Start the consumer (Terminal 1)**
```
python -m src.consumer
```
Header line appears, "Waiting for predictions…"

**1:00 – 1:20  Start the Faust worker (Terminal 2)**
```
faust -A src.stream_processor worker -l info
```
Wait for the `[stream] loaded model 'random_forest'` line and the Faust ASCII banner.

**1:20 – 1:35  Start the producer (Terminal 3)**
```
python -m src.producer
```
Show messages leaving the producer.

**1:35 – 2:20  Let it run**
Predictions scroll in Terminal 1. Point out:
- The `FRAUD` (red) vs `OK` (green) flag
- `p(fraud)` probability
- `actual=1(OK)` showing model agreed with the ground-truth label

If you want a fraud hit on camera, set `MAX_ROWS=0` and let it run — fraud rows appear roughly every ~600 transactions, so be patient or use `head -n 600` to slice a CSV that starts with a known fraud row.

**2:20 – 2:30  Wrap**
Ctrl-C each terminal in order: producer, processor, consumer. Show the consumer's final `processed=X, flagged=Y` line.

## Recording tools

- macOS: QuickTime → New Screen Recording (record audio from built-in mic)
- Or OBS if you want picture-in-picture webcam
- Upload to YouTube as **Unlisted** and paste the URL into the README

## Submission

1. `git init && git add . && git commit -m "Initial commit"` (after sanity-checking `.gitignore`).
2. Create a public repo on GitHub.
3. `git remote add origin git@github.com:<you>/credit-card-fraud-streaming.git && git push -u origin main`.
4. Update the **Video link** line in `README.md`.
5. Submit the GitHub URL on the course portal.
