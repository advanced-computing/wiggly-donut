# A Cross-Market Prediction Basket for the most impactful news stories
Group: wiggly-donut

## 1) What dataset are you going to use? (include link)
We want to build a live dashboard that tracks news-focused prediction markets across multiple platforms (starting with Kalshi and Polymarket). Prediction markets are useful because they aggregate public opinion into an implied probability, often producing surprisingly strong forecasts in practice. We want to create a cross-market “basket” signal instead of treating any one platform as ground truth. By combining probabilities across these sites, we get a more robust read of what expectations are shifting in real time.

We want to do this because of the news relevance. Prediction prices move when new information arrives, so the biggest probability changes over the last week can act like a filter for current affairs on the things that have changed massively. Concretely, we’ll ingest market data from Kalshi’s public market-data endpoints and Polymarket’s read-only Gamma Markets API, compute “top movers,” and then link each moving market to the most relevant headlines via NewsAPI. 

The News API: https://www.thenewsapi.com/

Kalshi API: https://docs.kalshi.com/welcome

Polymarket API: https://docs.polymarket.com/quickstart/overview

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

**Stage 1 — News Ingestion**
We pull the top 100 (scope decision) headlines from NewsAPI's top-stories endpoint, retaining the title, source, category, and publication timestamp. This snapshot is refreshed on each dashboard load.

**Stage 2 — Market Matching**
For each headline, we query both Kalshi and Polymarket APIs and retrieve open prediction markets. We match markets to headlines using keyword overlap on the market title. Each headline is assigned zero or more matching markets per platform.

**Stage 3 — Probability Aggregation**
For headlines with at least one match on each platform, we compute a cross-market average implied probability: the mean of all matched market "yes" probabilities across Kalshi and Polymarket.

**Stage 4 — Dashboard Presentation**
The Streamlit dashboard displays the 100 stories ranked by their cross-platform average probability. Users can filter by topic category or platform. Clicking a story reveals the matched markets, individual platform probabilities, and relevant headlines.



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

**3. Populate Your BigQuery Database**
Before running the dashboard, fetch the latest probabilities from Polymarket and upload them into your BigQuery dataset.

* Open `load_bq.py` and modify `PROJECT_ID`, `DATASET_ID`, and `TABLE_NAME` at the top of the file to match your GCP project destination.
* Run the script to ingest current Polymarket odds into BigQuery:
```bash
python load_bq.py
```

**4. Launch the Streamlit Dashboard**
Now that the data is loaded into BigQuery, start:
```bash
streamlit run streamlit_app.py
```
A browser tab will open automatically at `http://localhost:8501`.