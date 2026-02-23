# SEC EDGAR 10-K NLP Sentiment Analysis Pipeline

## Overview
This repository contains an end-to-end Python pipeline for downloading, cleaning, and analyzing Fortune 500 SEC 10-K filings. It bridges the gap between stock ticker symbols and SEC Central Index Keys (CIKs) to provide direct download URLs for multi-year financial research (2023–2026). It utilizes the strict textual cleaning methodology outlined in the Internet Appendix for *“When is a Liability not a Liability? Textual Analysis, Dictionaries, and 10-Ks”* by Tim Loughran and Bill McDonald to score companies on their Digital Maturity.

## Features
- **Phase 1: Automated Mapping:** Converts stock tickers to 10-digit SEC CIKs using the official SEC JSON map, with hardcoded overrides for recent corporate mergers.
- **Phase 2: Multi-Year Crawling:** Scans SEC Master Indexes and securely downloads raw 10-K HTML filings across 2023–2026, strictly respecting SEC API rate limits.
- **Phase 3: Text Preprocessing:** Uses multiprocessing and BeautifulSoup to strip HTML, Inline XBRL metadata, financial tables (>25% numbers), and encoded images to isolate pure narrative text.
- **Phase 4: Sentiment Scoring:** Applies Regex-compiled, normalized dictionaries to generate a 0-100 Min-Max scaled Digital Maturity score across 6 key dimensions.

### Installation
Clone the repository and install the dependencies. (Note: It is highly recommended to use a virtual environment).
```bash
pip install -r requirements.txt