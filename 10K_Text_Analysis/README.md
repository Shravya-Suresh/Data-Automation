# SEC EDGAR 10-K Target Mapper

## Overview
This folder automates the identification and mapping of Fortune 500 companies to their official SEC 10-K filings. It bridges the gap between stock ticker symbols and SEC Central Index Keys (CIKs) to provide direct download URLs for multi-year financial research (2023–2026). It uses the Internet Appendix for “When is a Liability not a Liability? Textual Analysis, Dictionaries, and 10-Ks” by Tim Loughran and Bill McDonald approach.


## Features
- **Automated Mapping:** Converts stock tickers to 10-digit SEC CIKs using the official SEC JSON map.
- **Multi-Year Crawling:** Scans SEC Master Indexes across 2023, 2024, 2025, and 2026.
- **Data Integrity:** Handles private/public company differentiation and resolves common ticker naming conflicts.
- **Security:** Uses environment variables to protect researcher contact information.

## Setup Instructions

### 1. Installation
Clone the repository and install the dependencies:
```bash
pip install -r requirements.txt