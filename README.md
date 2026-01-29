#ShoonyaOHLC_AutoSync

Automatically fetch Nifty OHLC data (Spot, Futures, Options) from Shoonya API and save to Google Drive daily for backtesting.

Overview:
ShoonyaOHLC_AutoSync is a Python-based automation tool that fetches OHLC (Open, High, Low, Close) data of Nifty’s spot, futures, and options from the Shoonya API daily after market close and saves it to Google Drive. This allows traders and analysts to maintain a historical dataset for backtesting and strategy development without manual intervention.
The workflow is fully automated using a GitHub Actions cron job, which triggers every day at 4 PM.

Features:
✅ Fetch OHLC data for Nifty Spot, Futures, and Options
✅ Saves data to Google Drive in an organized format
✅ Fully automated via GitHub cron job
✅ Ready for backtesting trading strategies

Requirements:
1. Python 3.8+
2. Shoonya API account and credentials
3. Google Drive account & Google Drive API access (service account recommended)
