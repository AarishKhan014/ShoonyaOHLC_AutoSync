# ShoonyaOHLC_AutoSync

**Automatically fetch Nifty OHLC data (Spot, Futures, Options) from Shoonya API and save to Google Drive daily for backtesting.**

---

## Overview

ShoonyaOHLC_AutoSync is a Python-based automation tool that fetches OHLC (Open, High, Low, Close) data of Nifty’s **Spot, Futures, and Options** from the [Shoonya API](https://www.shoonya.com/) **daily after market close** and saves it to Google Drive.  

This allows traders and analysts to maintain a historical dataset for **backtesting and strategy development** without manual intervention.  

The workflow is fully automated using a **GitHub Actions cron job**, which triggers **every day at 4 PM IST**.

---

## Features

- ✅ Fetch OHLC data for Nifty Spot, Futures, and Options  
- ✅ Automatic daily updates post-market close  
- ✅ Saves data to Google Drive in an organized format  
- ✅ Fully automated via GitHub cron job  
- ✅ Ready for backtesting trading strategies  

---

## Requirements

- Python 3.8+  
- Shoonya API account and credentials  
- Google Drive account & Google Drive API access (service account recommended)  

---

## Installation

1. Clone this repository:

```bash
git clone https://github.com/AarishKhan014/ShoonyaOHLC_AutoSync.git
cd ShoonyaOHLC_AutoSync
