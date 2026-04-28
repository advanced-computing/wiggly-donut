# A Cross-Market Prediction Basket for the most impactful news stories
Group: wiggly-donut

## 1) What dataset are you going to use? (include link)
We want to build a live dashboard that tracks news-focused prediction markets across multiple platforms (starting with Kalshi and Polymarket). Prediction markets are useful because they aggregate public opinion into an implied probability, often producing surprisingly strong forecasts in practice. We want to create a cross-market “basket” signal instead of treating any one platform as ground truth. By combining probabilities across these sites, we get a more robust read of what expectations are shifting in real time.

We want to do this because of the news relevance. Prediction prices move when new information arrives, so the biggest probability changes over the last week can act like a filter for current affairs on the things that have changed massively. Concretely, we’ll ingest a daily headline snapshot from NewsAPI.org, use Attena’s unified search API to find related Kalshi and Polymarket markets, compute a cross-platform basket, and rank stories by the size of their implied-probability changes.

The News API: https://newsapi.org/

Kalshi API: https://docs.kalshi.com/welcome

Polymarket API: https://docs.polymarket.com/quickstart/overview

Attena API: https://attena-api.fly.dev/docs

## 2) What are your research question(s)? (specific + answerable)
RQ1: Which 10–20 news-related prediction markets show the largest change in implied probability over the last 7 days (or last 24h), across a combined basket of prediction markets?

RQ2: What type of news provokes the most significant movement on prediction markets?

RQ3: What are the most common volatile prediction topics/placements over a week period and how are their evolution correlated with news cycle

Updated RQ: 
RQ1: What share of the top 100 news stories (accessed through the news API) have matched prediction markets, and which topic categories (politics, economics, sports, etc.) are most represented?

RQ2: For stories matched on both platforms, Is there a relationship between a story's news coverage volume and its market-implied probability?
sub-RQ2: Among the top 100 most-covered news stories, which events are prediction markets most confident about — and do higher-covered stories attract higher implied probabilities?
 

## 3) What’s the link to your notebook?
Notebook link:https://github.com/nav-v/adv-comp-project/blob/main/proposal.ipynb


## 4) What’s your target visualization? (include a picture)

![Target dashboard layout](diagram.jpeg)

Target dashboard layout (2-panel, interactive):

Top panel: “Top Movers (Last 7 Days) - shown above.

The top panel will have line charts showing the top 10/20 markets by probability change. Each chart will show the basket’s implied probability over time (e.g., last 7d). The x axis would be time, whilst the y exist would be implied probability.

Bottom panel: “What’s the story?”

When you click a market in the top panel, the page scrolls/jumps to a details section showing:

Market title + platform(s)

Latest probability + 7d change

Relevant headlines pulled from NewsAPI (title, source, timestamp, link), using the market’s keywords.


## 5) What are your known unknowns?
Through our retrospective, we found that our initial API tests were successful: both the Kalshi and Polymarket APIs worked properly, and we successfully implemented the average probability function across the two platforms. However, we have two primary known unknowns we still need to address. First, we need to finalize our matching strategy for connecting news articles to specific prediction markets. Second, there are hosting-related concerns; we are unsure whether we would need to set up cron jobs to keep the newsfeed updated, or if Streamlit is the right platform to allow for constant background computation.

## 6) What challenges do you anticipate?
A major challenge we anticipate revolves around API rate limits. We still need to incorporate and fully understand the News API's rate limits under real-world usage. Furthermore, rather than simply relying on RegEx or a basic keyword matching strategy, we anticipate that achieving true semantic matching between news topics and prediction markets will be a significant technical hurdle.

## 7) updated Methodology

We invert the usual "news → market" matching. Instead, we start from the markets
themselves: a market that has moved is, by definition, attached to a story.

**Stage 1 — Trending markets per platform (politics & geopolitics only)**
Once per day, we hit the Polymarket Gamma API and the Kalshi v2 API directly.
For Polymarket we page through the highest-volume open markets and filter to
politics/geopolitics by tag, series-slug, and keyword match. For Kalshi we list
open events, keep those with `category` ∈ {Politics, World, Elections,
Geopolitics}, and pull markets under those events. On each side we compute a
24-hour change (Polymarket exposes `oneDayPriceChange`; Kalshi exposes both
`last_price_dollars` and `previous_price_dollars`) and keep the top ~50 by
absolute change with a $1k 24h-volume floor.

**Stage 2 — Cross-platform pairing with strict timeframe alignment**
A single Gemini call takes the two top-50 lists and returns pairs that measure
the same outcome. The prompt rejects pairs that disagree on resolution timeframe
("by 2027" vs "by June 2026"), threshold ("S&P > 6000" vs "S&P > 6500"),
direction (yes vs no), or scope ("next president" vs "GOP nominee"). Close
times are passed in to enforce timeframe agreement.

**Stage 3 — Story generation via grounded search**
For each paired (or unpaired top-mover) story, Gemini is called again with
Google Search grounding to find the most relevant news article from the last
seven days. The returned title, description, source, and URL are stored as the
"headline" for that market move.

**Stage 4 — Basket aggregation**
For each story we compute a basket as the mean of the Polymarket and Kalshi
yes-prices when both exist (paired story), or the single-platform yes-price
otherwise. The 1-day basket change is computed against the most recent prior
snapshot of those same markets in BigQuery.

**Stage 5 — Dashboard**
The Streamlit homepage is a 2-panel view. The top panel is a multi-line chart
of the most-trending politics/geopolitics markets over the last seven days. The
bottom panel is the per-story drill-down with metrics and the Polymarket/Kalshi
detail. Data flows entirely through three BigQuery tables —
`daily_headlines`, `daily_market_matches`, `daily_story_baskets`.



## Quickstart Guide

Follow these steps to set up the application. 

**1. Clone and Install**
```bash
git clone https://github.com/nav-v/adv-comp-project.git
cd adv-comp-project
pip install -r requirements.txt
```
**2. Set up Google Cloud Authn**
You need to get a GCP service account credentials so Streamlit can connect to BigQuery. To do this:

a. Visit Google Cloud Consolr, select or create a project, then navigate to **IAM & Admin** > **Service Accounts**.
b. Create a new Service Account and grant it the **BigQuery Data Editor** and **BigQuery Job User** roles so it can read, write, and execute BigQuery jobs.
c. Click on the newly created Service Account, go to the **Keys** tab, click **Add Key**, and choose **JSON**. Download the key file to your machine.

Once you have the JSON key, you need to add it to your Streamlit secrets. Create a `.streamlit` folder and a `secrets.toml` file inside your project directory:
```bash
mkdir .streamlit
touch .streamlit/secrets.toml
```

Open `.streamlit/secrets.toml` and add a `[gcp_service_account]` section. Convert the key-value pairs from your JSON file into TOML format. It should look like this:

```toml
[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "your-private-key-id"
private_key = "-----BEGIN PRIVATE KEY-----\nYOUR\nPRIVATE\nKEY\nHERE\n-----END PRIVATE KEY-----\n"
client_email = "your-service-account-email"
client_id = "your-client-id"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "your-cert-url"
universe_domain = "googleapis.com"
```

**3. (No news API key needed)**
The pipeline now reads directly from the public Polymarket Gamma API and the
Kalshi v2 API — neither requires an API key. News headlines are generated by
Gemini using Google Search grounding via your existing GCP service account.

**4. Populate Your BigQuery Database**
Before running the dashboard, create the latest daily snapshot in BigQuery.

* Open `load_bq.py` and modify `PROJECT_ID` and `DATASET_ID` if your GCP destination is different.
* Run the daily batch script:
```bash
python load_bq.py
```

This stores three daily tables in BigQuery:

* `daily_headlines`
* `daily_market_matches`
* `daily_story_baskets`

**5. Launch the Streamlit Dashboard**
Now that the daily snapshot is loaded into BigQuery, start:
```bash
streamlit run Prediction_Market_Ranking.py
```
A browser tab will open automatically at `http://localhost:8501`.
