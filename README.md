# Real-Time Sentiment Analysis with PySpark

This repository contains Python code for performing real-time sentiment analysis on tweet data using PySpark. It includes implementations for both English and French tweets. The analysis leverages the VADER (Valence Aware Dictionary and sEntiment Reasoner) lexicon for sentiment scoring.

## Project Overview

This project demonstrates how to:

- **Process Text Data:** Clean and preprocess tweet text data, removing noise and preparing it for analysis.
- **Perform Sentiment Analysis:** Apply sentiment analysis using the VADER lexicon to classify tweets as positive or negative.
- **Simulate Streaming Data:** Process data in simulated real-time chunks to mimic streaming data analysis.
- **Visualize Results:** Display real-time sentiment analysis results using interactive Plotly charts.
- **Handle Multiple Languages:** Process English and French tweet datasets with specific pre-processing steps.

## Getting Started

### Prerequisites

Before running the code, make sure you have the following installed:

- Python 3.8 or higher
- PySpark
- NLTK
- Pandas
- Plotly
- Libraries listed within the file
  
You can install the necessary libraries using pip.

### Running the Scripts

#### English Tweets Analysis:
1.  Navigate to the project directory.
2.  Execute the English analysis script:

    ```bash
    python sentiment_analysis_spark_en.py
    ```

    This script will:
    - Load the `/content/sentiment_tweets3.csv` dataset.
    - Preprocess text data.
    - Perform VADER sentiment analysis.
    - Calculate and print the model accuracy.
    - Demonstrate both: Aggregated results (on first run) and Real-time visualization (on second run), by using the same simulated streaming function.
    - Save results in the `/output/sentiment_results.csv` file
    - The output will be in the form of a plotly chart in the case of the real-time visualization and as aggregated results if not.

### Understanding the Code

#### Sentiment Analysis Process
The project uses a simulated streaming approach. Here’s the workflow:
1. The data from the given dataset are processed in chunks. Each chunk simulates a time interval in the streaming.
2. In each chunk, the data is processed by the defined cleaning and preprocessing function.
3. The data is fed to the VADER sentiment analysis, to classify each tweet.
4. At the same time, the data is aggregated by the given window (default is 1 minute).
5. The aggregation is used for real-time plotting, by creating a Plotly chart and updating it with each time interval or for output.
6. A simulated delay is given by `time.sleep` to mimic a real-time data stream.
7. The results are stored in the specified `output_path`.

## Key Features

-   **Real-Time Plotting:** Visualizes sentiment analysis in a dynamic chart using Plotly.
-   **Simulated Streaming:** Processes data in chunks, simulating a real-time stream for analysis.
-   **Modular Design:** Each function is designed to handle a specific part of the pipeline, promoting readability and reusability.
-   **Language Support:** Provides specific handling of both English and French tweets.
