# BDA Project: Bitcoin Price Prediction 

**Authors:** Owen BRAUX & Elliot CAMBIER
**Course:** Big Data Analytics - ESIEE 2025-2026
**Date:** 2025-11-25

## Project Overview

The goal of this project is to build an end-to-end Big Data pipeline using **PySpark** to predict Bitcoin price movements. 

Instead of relying solely on market data, we aim to leverage actual transactions from the blockchain to see if network activity has predictive power over the price.

### Objectives
1.  **Ingest raw binary data:** Parse Bitcoin `.dat` block files using Spark RDDs.
2.  **ETL & Aggregation:** Transform raw transactions into time-series features.
3.  **Machine Learning:** Train a classifier to predict if the price will rise in the next 10 minutes.
4.  **Reproducibility:** Ensure the entire pipeline runs via a single script.

## 2. Setup and Execution

We designed the project to be fully reproducible. Below are the steps to set up the environment and run the pipeline.

### Prerequisites

You need `conda` installed on your machine.

**Environment Creation**
```bash
conda create -n bda-env python=3.10 -y
conda activate bda-env
conda install -c conda-forge openjdk=21 maven -y
pip install --upgrade pip jupyterlab kaggle pyspark pyyaml
```

## Data Acquisition Guide

This project requires two main datasets: Bitcoin blockchain blocks and historical market prices.

### Option 1: Using the Provided Dataset

The easiest way to get started is to use the pre-packaged data available on the course Google Drive.

1.  **Download the archive:** `BDA-project-final-data.zip` from the provided Gdrive link.
2.  **Unzip the file** into the `data/` directory of this project.

This will provide the necessary `blocks/` and `prices/` subdirectories.

### Option 2: Fetching from Source

#### A. Market Prices from Kaggle

1.  Ensure the Kaggle API is configured (`~/.kaggle/kaggle.json`).
2.  Run the following command to download the price data:
    ```bash
    conda activate bda-env
    kaggle datasets download -d mczielinski/bitcoin-historical-data -p data/prices --unzip --force
    ```

#### B. Raw Bitcoin Blocks (Bitcoin Core)

The raw `blk*.dat` files were generated using a pruned Bitcoin Core node as described in the project support documents. 
Read -> `BDA_final_project_support_dataset.md`

## Methodology & Implementation

### A. Data Ingestion 

The biggest technical challenge was handling the raw Bitcoin blocks since these are binary files and not CSVs.

*   **Solution:** We used sc.binaryFiles to load the raw bytes into RDDs.
    
*   **Parsing:** We implemented a custom Python parser (class BlockchainParser) mapped over the RDD to decode bytes into a structured dictionary (Hex to readable transaction data).
    
*   **Why Spark?** While our sample is ~1GB, this architecture scales. Using RDDs allows us to parallelize the parsing of terabytes of blockchain data if needed.
    

### B. ETL and Feature Engineering

We faced a granularity mismatch: Prices are available every minute, but blocks are mined approximately every 10 minutes.

*   **Aggregation:** We aggregated transactions into 1-minute windows using Spark SQL window() functions (sum of BTC volume, count of transactions).
    
*   **Join Strategy:** We performed a Left Join of Prices with Transactions. Minutes with no blocks were filled with 0s to maintain time continuity.
    

### C. Advanced Feature Engineering

Our initial baseline model performed poorly (see Results). We hypothesized that the model lacked "context."

*   **Rolling Windows:** We implemented rolling averages over the last hour (60 minutes).
    
*   **Momentum:** We calculated price momentum (Current Price / 1-Hour Average) and transaction volume spikes.
    
*   This allows the model to "see" trends rather than just instantaneous values.
    

## Modeling and Results

We defined the target label as: **1 if the price increases by >0.1% in the next 10 minutes, else 0.**

### Experiment 1: Baseline

*   **Model:** Logistic Regression.
    
*   **Features:** Raw Price, Raw Transaction Volume (instantaneous).
    
*   **Result (AUC):** 0.4834.
    
*   **Analysis:** This is worse than random guessing (0.5). It confirmed that knowing only the current price is insufficient to predict the future.
    

### Experiment 2: Improved Approach

*   **Model:** Gradient-Boosted Trees (GBTClassifier).
    
*   **Features:** Added Rolling Averages, Momentum, and Max Volume (1h).
    
*   **Result (AUC):** 0.6541.
    
*   **Analysis:** A significant improvement. The GBT model successfully captured non-linear relationships and trends from the rolling features.


## AI Usage Declaration

In compliance with the project guidelines, we declare the use of AI tools (Gemini) for the following specific tasks:

1.  **Concept Explanation:** To better understand the difference between Spark RDDs and DataFrames, specifically regarding binary file reading.
    
2.  **Code Refactoring:** To help organize our multiple Jupyter Notebooks into a single, modular main.py script with clean functions.
    
3.  **Debugging:** We used AI to troubleshoot the AnalysisException during the schema definition and to understand the "WindowExec" warnings in the Spark logs.
    

All logic, architecture decisions, and final code validation were performed by us.

## References & Licenses

**Data Sources:**

*   **Bitcoin Historical Data:** [Kaggle - Zielinski](https://www.google.com/url?sa=E&q=https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data)
    
*   **Bitcoin Blockchain:** Raw data generated via Bitcoin Core (MIT License).

**Tools:**

*   **Apache Spark:** Apache License 2.0.
*   **Python:** PSF License.