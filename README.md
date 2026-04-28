# IONOS Traffic & Cost Tool

A web-based dashboard for IONOS Cloud customers to monitor network traffic, understand billing, and forecast next-month costs — all from a browser with no installation required beyond running the app.

---

## Who is this for?

IONOS Cloud account holders who want to:
- Understand what is generating outbound traffic and how much it costs
- Track traffic trends across one or many Virtual Data Centres
- Browse and analyse VPC flow logs stored in IONOS Object Storage
- Project next-month traffic and costs before the bill arrives

---

## What you need

- Your **IONOS API Token** — found in the IONOS DCD under *API Keys*
- Your **Contract ID** — visible in the DCD or billing portal
- For S3 / flow log features: an **S3 Access Key** and **S3 Secret Key** (from the DCD Object Storage section)

You enter these in the browser when you open the app. Nothing is stored on the server.

---

## How to use

### Entering your credentials
Open the app and fill in the **Connection** section at the top:
- **API Token** and **Contract ID** are required for all traffic queries
- **S3 credentials** are only needed if you want to query Object Storage traffic or browse flow logs
- Select your **Market** (EUR / GBP / USD) and adjust the **Outbound Rate per GB** if your contract differs from the default

You can also click **Load .env file** to load a pre-filled configuration file from your computer — it is read entirely in your browser and never sent to the server.

---

### IP Traffic
Query inbound and outbound traffic for a single public IP address:
1. Enter an IP address in the **IP Traffic** tab
2. Select a billing period (month/year)
3. Click **Query Traffic**

Shows: monthly totals, estimated cost, daily bar chart, and a full daily breakdown table.

---

### VDC Traffic
Query aggregate traffic for all IPs in a Virtual Data Centre:
1. Select a VDC from the dropdown (or type a name / choose *All VDCs in Contract*)
2. Select a billing period
3. Click **Query Traffic**

Shows: VDC-level totals, per-IP breakdown with device types (Server, NAT Gateway, ALB, NLB, Kubernetes Node), traffic meter IDs, and S3 Object Storage meters.

---

### Multi-month Range
Query up to 12 consecutive months at once to see trends:
1. Set the **Range** dropdown to more than 1 month
2. Select an **End Period**
3. Click **Query Traffic**

Shows: monthly trend chart, cumulative totals, and estimated cost over the range.

---

### Next Month Traffic & Cost Projection
After any query, the **Next Month Traffic and Cost Projection** card appears:
1. Choose how many months of history to base the projection on
2. Click **Calculate Projection**

Shows: semicircular gauge charts for at-a-glance traffic and cost direction, plus detailed projected inbound, outbound, and cost figures. Cost shows as zero if projected outbound is within the free 2 TB allowance.

---

### S3 Object Storage
Query Object Storage traffic meters at contract level:
1. Go to the **S3 Object Storage** tab
2. Select a billing period
3. Click **Query Traffic**

---

### Flow Logs
Browse VPC flow log records stored in IONOS Object Storage:
1. After a VDC query, scroll down to the **Flow Logs** section
2. Select a date range and click **Load Flow Logs**
3. Optionally visualise traffic relationships in the **Network Graph**

To set up flow logging for the first time, use the **Create Bucket** and **Enable Flow Logs** buttons in the Flow Logs card.

---

### Browse IPs
Click **Browse IPs** at the top to list all public IP addresses on your contract, filtered by device type.

---

## Tips

- The **2 TB free tier** for outbound traffic is applied automatically in all cost estimates — only usage above 2,048 GB is shown as billable
- Inbound traffic on NAT Gateway IPs is return traffic from NAT sessions and is not billable
- If you see a `401` error, your API token has expired — generate a new one in the DCD
- The projection accuracy improves with more historical months selected (6–12 is recommended)

---

## Known limitations

- Cost estimates are indicative only — verify rates and free-tier details against your IONOS contract
- Flow log queries require VPC flow logging to be enabled and logs stored in IONOS Object Storage
- DNS reverse-lookup uses the HackerTarget API (free tier: 100 lookups/day); lookups will silently fail if the limit is reached
- S3 bucket creation for flow logs may take a minute to propagate before the first log upload succeeds
