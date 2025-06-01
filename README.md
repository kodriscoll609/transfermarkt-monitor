# transfermarkt-monitor

This project tracks player transfer values from Transfermarkt by running a full data pipeline from scraping to storage. It includes a custom web scraper, a local SQLite database, and a scheduler that can run the job on a trigger.

Player data is only updated when something actually changes — each record is hashed and compared to keep storage efficient. Logs are written for each run, and the scraper handles errors like timeouts or blocking by using custom headers and retry logic with exponential backoff.

The scraping flow goes step by step: it starts by pulling league names, then clubs, then players. Each stage depends on the one before it.