# MSX Stock Analyzer

A lightweight local web app for screening and comparing Muscat Stock Exchange stocks using data sourced from the official MSX website.

## What it does

- Screens stocks by value, income, growth, momentum, and quality
- Pulls live MSX market-watch, company, and financial-performance data from official endpoints
- Lets you switch strategy weights for different investing styles
- Filters by sector and search term
- Shows ranked results plus a detail panel with strengths, risks, and chart analytics
- Adds buy-pressure and demand metrics from live bid and ask volume
- Calculates RSI 14 and Bollinger bands from official MSX chart history
- Imports your own CSV snapshot so you can replace the demo data
- Links directly to official MSX pages for Today, Historical, Companies, and Financial Performance

## Run it

From this folder, install the Python packages:

```powershell
python -m pip install -r requirements.txt
```

Then start the Flask app:

```powershell
python app.py
```

Then open `http://127.0.0.1:5000`.

## CSV format

Use this header row:

```csv
ticker,company,sector,price,marketCapM,peRatio,pbRatio,dividendYield,earningsGrowth,priceChange1Y,debtToEquity,roe,currentRatio
```

Example row:

```csv
BKMB,Bank Muscat,Banking,0.292,2330,9.5,1.1,5.8,10.1,11.9,1.5,11.8,1.2
```

## Notes

- Official source website: [https://www.msx.om/](https://www.msx.om/)
- Helpful official pages:
  - Market Watch Today: [https://www.msx.om/market-watch-custom.aspx](https://www.msx.om/market-watch-custom.aspx)
  - Historical: [https://www.msx.om/market-watch-history.aspx](https://www.msx.om/market-watch-history.aspx)
  - Companies: [https://www.msx.om/companies.aspx](https://www.msx.om/companies.aspx)
  - Companies Financial Performance: [https://www.msx.om/Companies-Fin-Pref.aspx](https://www.msx.om/Companies-Fin-Pref.aspx)
- The Flask backend fetches the official MSX endpoints server-side and normalizes them into the app schema.
- Daily chart analytics are computed from `company-chart-data.aspx?s=SYMBOL`.
- If the live fetch fails or the page structure changes, the app falls back to the built-in sample dataset.
- MSX states that market data on the site is delayed by 15 minutes.
- The scoring model is intentionally simple so it is easy to adjust in `app.js`.
