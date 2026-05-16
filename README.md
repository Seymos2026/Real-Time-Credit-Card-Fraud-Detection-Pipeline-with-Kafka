# Credit Card Fraud — Real-Time Streaming with Apache Kafka

**ENGR 5785G — Assignment 1**
Real-time fraud detection pipeline built with **Apache Kafka (Confluent Cloud)**, a **Faust** stream processor, and a **scikit-learn** model trained offline.

The pipeline replays the Kaggle Credit Card Fraud dataset as a live event stream, scores each transaction with the trained model, and prints predictions in a separate consumer terminal.

```
   ┌──────────────┐   raw-transactions    ┌────────────────────┐   fraud-predictions   ┌──────────────┐
   │   producer   │ ─────────────────────▶│  Faust processor   │ ─────────────────────▶│   consumer   │
   │ (creditcard. │    JSON @ ~1 row/s    │  loads .joblib     │    JSON w/ score      │ prints rows  │
   │   csv loop)  │                       │  predict_proba()   │                       │ to console   │
   └──────────────┘                       └────────────────────┘                       └──────────────┘
```

---

## 1. Dataset

- **Source:** [Kaggle — Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) (`mlg-ulb/creditcardfraud`).
- **Size:** 284,807 transactions, 30 numeric features (`Time`, `V1`–`V28`, `Amount`) plus a binary `Class` label.
- **Imbalance:** Fraud is **~0.172%** of rows — we use `class_weight="balanced"` to compensate.

Download `creditcard.csv` from Kaggle and drop it into the `data/` folder. The file is gitignored because it's ~150 MB.

```
data/
└── creditcard.csv   <-- put it here
```

---

## 2. Streams library

**Python + Faust (faust-streaming).** The processor uses an `@app.agent` bound to the `raw-transactions` topic — that's the official Faust Streams API, not a hand-rolled consumer loop.

---

## 3. Model

`src/train.py` trains two candidates and saves whichever has the higher F1:

| Model | Why it's here |
|---|---|
| Logistic Regression (`class_weight=balanced`) | Fast baseline, interpretable. |
| Random Forest (120 trees, depth 12) | Usually wins on this dataset. |

Both run inside an `sklearn.Pipeline` with `StandardScaler` so the saved artifact contains the full preprocessing + classifier.

### Performance (test split, 20% stratified)

> After running `python -m src.train`, paste your actual numbers from `models/metrics.json` here. Representative values on this dataset:

| Model | Accuracy | Precision | Recall | F1 | ROC-AUC |
|---|---|---|---|---|---|
| Logistic Regression | 0.9740 | 0.0587 | 0.9184 | 0.1103 | 0.9706 |
| **Random Forest (chosen)** | **0.9996** | **0.9474** | **0.7551** | **0.8404** | **0.9637** |

Replace these placeholders with the real values from your run.

---

## 4. Project layout

```
credit-card-fraud-streaming/
├── README.md
├── requirements.txt
├── .env.example          # copy to .env, fill in Confluent Cloud creds
├── .gitignore
├── data/                 # creditcard.csv lives here (gitignored)
├── models/               # fraud_model.joblib + metrics.json (after training)
├── scripts/
│   └── create_topics.py  # one-shot helper to create the two topics
├── src/
│   ├── config.py             # loads .env, exposes Kafka/Faust config
│   ├── train.py              # offline ML training
│   ├── producer.py           # raw-transactions producer
│   ├── stream_processor.py   # Faust @app.agent --> predictions
│   └── consumer.py           # fraud-predictions output consumer
└── docs/
    └── demo_guide.md     # 3-terminal video walkthrough
```

---

## 5. Setup

### 5.1 Prereqs

- Python **3.10 – 3.11** (Faust isn't happy on 3.12+ yet)
- A Confluent Cloud cluster + an API key/secret with `DeveloperWrite` on the cluster
- The Kaggle dataset (see §1)

### 5.2 Install

```bash
git clone https://github.com/<you>/credit-card-fraud-streaming.git
cd credit-card-fraud-streaming

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 5.3 Configure

```bash
cp .env.example .env
# Open .env and fill in:
#   KAFKA_BOOTSTRAP_SERVERS   from Confluent Cloud cluster -> "Cluster settings"
#   KAFKA_SASL_USERNAME       = your API key
#   KAFKA_SASL_PASSWORD       = your API secret
```

### 5.4 Create topics (one-time)

```bash
python -m scripts.create_topics
# Or do it manually in the Confluent UI: raw-transactions, fraud-predictions
```

### 5.5 Train the model (one-time)

```bash
python -m src.train
# Produces models/fraud_model.joblib and models/metrics.json
```

---

## 6. Run the pipeline (three terminals)

Activate the venv in each terminal first.

**Terminal 1 — output consumer** (start it *first* so you don't miss any predictions):

```bash
python -m src.consumer
```

**Terminal 2 — Faust stream processor:**

```bash
faust -A src.stream_processor worker -l info
```

**Terminal 3 — producer:**

```bash
python -m src.producer
```

You should now see lines like this scrolling in Terminal 1:

```
2026-05-16 14:02:11   OK    $   12.99     0.0021    0(OK)   random_forest
2026-05-16 14:02:12   FRAUD $  978.40     0.8843    1(OK)   random_forest
```

`OK`/`MISS` in the `ACTUAL` column shows whether the prediction matched the ground-truth label that came along on the wire.

---



**Video link:**  https://youtu.be/leSPh4Gnr_Q

---

## 8. Troubleshooting

- **`faust: command not found`** — `pip install faust-streaming` puts the binary in `.venv/bin`. Activate the venv.
- **SASL_SSL handshake errors** — your API key likely doesn't have ACLs on the topics. In Confluent Cloud, give the key `DeveloperWrite` on the cluster, or scope ACLs to those two topics.
- **`ModuleNotFoundError: src`** — run commands from the project root, not from inside `src/`.
- **Faust + Python 3.12** — pin to 3.11.x. `pyenv install 3.11.9 && pyenv local 3.11.9`.
- **Predictions never appear** — the consumer is on `auto.offset.reset=latest`. If you started the producer before the consumer, restart with messages already in flight, or change to `earliest` in `src/consumer.py`.

---


