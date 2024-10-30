# Organization Metrics Collector

This repository automates the collection, storage, and persistence of GitHub traffic metrics for repositories within the `CCP-NC` organization. By using GitHub Actions, this project regularly gathers key statistics like views, clones, referrers, and popular paths, allowing long-term tracking beyond GitHub's default 14-day limit.

## Features
- **Automated Data Collection**: A GitHub Actions workflow runs twice daily to collect traffic data for all active repositories.
- **Long-Term Data Storage**: Metrics are stored as JSON files, with a summary stored in CSV format to enable easy analysis.
- **Extensibility**: The project can be easily expanded to include additional metrics or modify the collection frequency.

## Prerequisites
GitHub Personal Access Token (PAT) with repo, read:org, and public_repo permissions.
Python 3.8+ for local testing.

## Getting Started
1. Clone the Repository
```bash
git clone https://github.com/CCP-NC/metrics.git
cd metrics
```

2. Set Up Environment Variables
Create a .env file in the root directory and add the following:

```plaintext
GH_TOKEN=your_github_token_here
ORG_NAME=CCP-NC
```

3. Install Dependencies
Install Python dependencies:

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

4. Run Locally for Testing
Execute the main script locally to verify it collects metrics successfully. The script takes in the repository name as an argument:

```bash
python .github/scripts/collect_traffic.py <repo-name>
```

5. Deploy Workflow to GitHub
After verifying locally, commit and push changes to deploy the GitHub Actions workflow. The workflow will start collecting traffic metrics automatically.

## Usage

**Traffic Data**: Raw daily JSON data is saved in `traffic-stats/` for each metric (e.g., views, clones) and each repo.

**Summary**: A daily summary is maintained in `traffic-stats/summary.csv`.

## Contributing

Contributions are welcome! To contribute:

1. Fork this repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -m "Add feature"`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.
