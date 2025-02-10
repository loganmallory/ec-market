# What does this script do?
- Like many other sites, this script gathers EC2 pricing from AWS and displays it in a table
- Unlike other sites, this script:
  - Differentiates between VCPU vs. CPU
  - Breaks the pricing down to Spot vs. On-Demand
  - Grabs additional data like:
    - spot interruption rates
    - clock speed
    - throttling flag
    - hyperthreading flag
    - cores vs vcpu
  - Adds common calculations like price per vcpu hour
  - Uses [Handsontable](https://handsontable.com/) to provide a useful table + filters
  - Runs locally

> [!TIP]
> For example, you can search for EC2s with 4 - 16 cores and sort based on price per hour:
<img width="1728" alt="market-example" src="https://github.com/user-attachments/assets/91a53b24-3cbc-430d-af2e-6e3a6866db58" />


# Usage
> [!NOTE]  
> Running this script involves scraping EC2 market prices (~15sec) and running a local Flask server
```
python3 market.py --browser
```
- The server is configured to auto-update all pricing every 20 minutes (see `REFRESH_FREQ_SECONDS`)
