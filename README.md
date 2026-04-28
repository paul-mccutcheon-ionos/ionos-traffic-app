# IONOS Traffic & Cost Tool

A self-hosted web application for querying, visualising, and projecting IONOS Cloud network traffic and billing data.

## Features

- **IP Traffic** — query inbound/outbound traffic for any public IP, with daily bar chart and breakdown table
- **VDC Traffic** — aggregate traffic across all IPs in a Virtual Data Centre, with per-IP breakdown and device classification (Server, NAT Gateway, ALB, NLB, Kubernetes Node)
- **S3 Object Storage** — query object storage traffic meters at contract level
- **Multi-month Range** — query across up to 12 months with totals and trend charts
- **Cost Estimation** — estimated outbound cost per month with 2 TB free-tier applied; supports EUR / GBP / USD markets
- **Next Month Traffic & Cost Projection** — linear regression over historical months with semicircular gauges for at-a-glance trend indication
- **Flow Logs** — browse VPC flow logs stored in IONOS S3; create and manage flow log buckets and configurations
- **Network Graph** — visualise traffic relationships between resources (servers, ALBs, NLBs, NAT gateways)
- **Browse IPs** — list and filter all public IPs in a contract by device type
- **Traffic Meters** — raw IONOS billing meter IDs breakdown per contract period

## Quick Start

```bash
cp .env.example .env
# Fill in your credentials in .env
npm install
npm start
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Configuration

Copy `.env.example` to `.env` and fill in your values:

| Variable | Description |
|---|---|
| `IONOS_API_TOKEN` | IONOS Cloud API token (from DCD → API Keys) |
| `IONOS_CONTRACT_ID` | Your IONOS contract number |
| `IONOS_S3_ACCESS_KEY` | S3-compatible access key for Object Storage |
| `IONOS_S3_SECRET_KEY` | S3-compatible secret key |
| `AUTH_CONTRACT_ID` | Display name shown in the password prompt (optional) |
| `AUTH_PASSWORD_HASH` | bcrypt hash to password-protect the server-side `.env` prefill (optional) |
| `PORT` | Server port (default: 3000) |

Generate a password hash:
```bash
node -e "require('bcryptjs').hash('yourpassword',10).then(h=>console.log(h))"
```

## Credentials

Credentials entered in the browser are used only for IONOS API calls and are never stored server-side. Alternatively, load a local `.env` file directly in the browser — it is parsed client-side and never transmitted to the server.

## Notes

- Outbound cost estimates apply the IONOS 2 TB/month free-tier before calculating billable usage
- Inbound traffic on NAT Gateway IPs is return traffic for NAT sessions and is not billable
- Flow log queries require S3 credentials and an existing flow log bucket
- The projection uses linear regression; accuracy improves with more historical months selected
