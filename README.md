# Sentiment Analysis & Bot Detection

A machine learning project for collecting social media posts, detecting bot accounts, and analyzing sentiment trends over time.

## Overview

This repository contains an end-to-end pipeline that:

1. **Collects** public Bluesky posts by keyword and date range
2. **Filters** out bot-generated content using a trained classifier
3. **Analyzes** sentiment of remaining posts with a fine-tuned RoBERTa model
4. **Visualizes** sentiment drift over time

## Project Structure

```
SentimentAnalysis/
├── data/                                    # Raw datasets (CSVs)
├── model_results/                           # Trained model evaluation outputs (JSON)
├── models/                                  # Saved model weights and artifacts
├── notebooks/
│   ├── sentiment_analysis.ipynb             # RoBERTa fine-tuning + Optuna HPO
│   ├── bot_identification.ipynb             # XGBoost bot detection classifier
│   └── bot_detection_sentiment_pipeline.ipynb  # End-to-end pipeline + drift plots
├── scripts/
│   ├── query_bluesky.py                     # Bluesky post collector
│   └── usage.txt                            # Detailed flag documentation
└── sentiment_drift_graphs/                  # Generated sentiment drift visualizations
```

## Quick Start

### 1. Collect Bluesky Posts

Use `scripts/query_bluesky.py` to collect public posts. Bluesky account is required:

```powershell
$env:BLUESKY_HANDLE="your.handle.bsky.social"
$env:BLUESKY_APP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
python .\query_bluesky.py --keyword "climate change" --start-date 2025-01-01 --end-date 2025-01-31 --output-path climate_change_jan_2025.csv --output-format csv --lang en --authenticated
```

See `scripts/usage.txt` for a complete list of all 19 flags and their descriptions.

### 2. Train / Evaluate Models

Open and run the notebooks in order:

- **`notebooks/sentiment_analysis.ipynb`** — Fine-tunes RoBERTa-base on the TweetEval sentiment dataset with Optuna hyperparameter optimization. Saves the best model to disk.
- **`notebooks/bot_identification.ipynb`** — Trains an XGBoost classifier with TF-IDF features (word + char level) to detect bot accounts. Includes 50-candidate RandomizedSearchCV.

### 3. Run the End-to-End Pipeline

- **`notebooks/bot_detection_sentiment_pipeline.ipynb`** — Loads both trained models, processes raw CSV posts, optionally filters out bots, runs sentiment analysis, and generates time-series sentiment drift visualizations with volume bubbles and smoothed trends.

## Requirements

``` shell
pip install -r requirements.txt
```

## Key Results

| Model | Metric | Score |
|-------|--------|-------|
| RoBERTa (sentiment) | Test Accuracy | 71.3% |
| RoBERTa (sentiment) | Test F1 (macro) | 71.3% |
| RoBERTa (sentiment) | Best CV Recall | 75.5% |
| XGBoost (bot detection) | CV Score (range) | 0.77–0.85 |
