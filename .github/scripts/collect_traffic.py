import argparse
from logging.handlers import RotatingFileHandler
import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
from typing import Dict, Any, Optional
import concurrent.futures

class GitHubTrafficCollector:
    def __init__(self, repo: str):
        self.token = os.environ['GH_TOKEN']
        self.repo = repo
        self.org = "CCP-NC"
        self.base_url = f"https://api.github.com/repos/{self.org}/{self.repo}"
        self.stats_dir = Path("traffic-stats")
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up logging with rotation
        logfilename = self.stats_dir / "traffic_collector.log"
        handler = RotatingFileHandler(
            logfilename, maxBytes=1000000, backupCount=5
        )
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[handler]
        )
        self.logger = logging.getLogger(__name__)

        
        # Reusable session for better performance
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        })
        
        # Cache for API responses
        self._cache: Dict[str, Any] = {}

    def _make_request(self, endpoint: str, retries: int = 3, delay: float = 2) -> Optional[Dict]:
        """Make a request to the GitHub API with exponential backoff and rate limiting."""
        cache_key = f"{self.repo}:{endpoint}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        for attempt in range(retries):
            try:
                response = self.session.get(f"{self.base_url}/{endpoint}")
                
                # Handle rate limiting
                if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
                    remaining = int(response.headers['X-RateLimit-Remaining'])
                    if remaining == 0:
                        reset_time = int(response.headers['X-RateLimit-Reset'])
                        sleep_time = reset_time - time.time() + 1
                        if sleep_time > 0:
                            logging.warning(f"Rate limit reached. Sleeping for {sleep_time} seconds")
                            time.sleep(sleep_time)
                            continue

                if response.status_code == 200:
                    data = response.json()
                    self._cache[cache_key] = data
                    return data
                elif response.status_code == 404:
                    logging.warning(f"Resource not found: {endpoint}")
                    return None
                elif attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
                else:
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for {endpoint}: {str(e)}")
                if attempt == retries - 1:
                    raise

        return None

    def _process_referrers(self, referrers_data: Optional[list]) -> Dict[str, Any]:
        """Process referrers data with optimized calculations."""
        if not referrers_data:
            return {
                'top_referrer': 'none',
                'top_referrer_count': 0,
                'top_referrer_uniques': 0,
                'total_referrer_count': 0,
                'total_referrer_uniques': 0,
                'distinct_referrers': 0
            }

        # Use pandas for efficient calculations
        df = pd.DataFrame(referrers_data)
        
        if df.empty:
            return self._process_referrers(None)
            
        top_referrer = df.nlargest(1, ['count', 'uniques']).iloc[0]
        
        return {
            'top_referrer': top_referrer.get('referrer', 'none'),
            'top_referrer_count': int(top_referrer.get('count', 0)),
            'top_referrer_uniques': int(top_referrer.get('uniques', 0)),
            'total_referrer_count': int(df['count'].sum()),
            'total_referrer_uniques': int(df['uniques'].sum()),
            'distinct_referrers': len(df)
        }

    def _process_paths(self, paths_data: Optional[list]) -> Dict[str, Any]:
        """Process paths data with optimized calculations."""
        if not paths_data:
            return {
                'top_path': 'none',
                'top_path_count': 0,
                'top_path_uniques': 0,
                'total_path_count': 0,
                'total_path_uniques': 0,
                'distinct_paths': 0,
                'readme_views': 0,
                'readme_uniques': 0
            }

        # Use pandas for efficient calculations
        df = pd.DataFrame(paths_data)
        
        if df.empty:
            return self._process_paths(None)
            
        df['is_readme'] = df['path'].str.lower() == '/readme.md'
        top_path = df.nlargest(1, ['count', 'uniques']).iloc[0]
        readme_stats = df[df['is_readme']].agg({
            'count': 'sum',
            'uniques': 'sum'
        }).fillna(0)

        return {
            'top_path': top_path.get('path', 'none'),
            'top_path_count': int(top_path.get('count', 0)),
            'top_path_uniques': int(top_path.get('uniques', 0)),
            'total_path_count': int(df['count'].sum()),
            'total_path_uniques': int(df['uniques'].sum()),
            'distinct_paths': len(df),
            'readme_views': int(readme_stats.get('count', 0)),
            'readme_uniques': int(readme_stats.get('uniques', 0))
        }

    def _save_raw_data(self, metric_name: str, data: Any, timestamp: str) -> None:
        """Save raw data with compression."""
        filename = self.stats_dir / f"{self.repo}-{metric_name}-{timestamp}.json"
        df = pd.DataFrame({'data': [data]})
        df.to_json(filename, orient='records')

    def _update_summary(self, daily_data: Dict[str, Any]) -> None:
        """Update summary with efficient pandas operations."""
        summary_file = self.stats_dir / "summary.csv"
        
        try:
            if summary_file.exists():
                df = pd.read_csv(summary_file)
                mask = (df["timestamp"] == daily_data["timestamp"]) & (df["repository"] == daily_data["repository"])
                if mask.any():
                    # Update the existing row
                    for key, value in daily_data.items():
                        df.loc[mask, key] = value
                else:
                    df = pd.concat([df, pd.DataFrame([daily_data])], ignore_index=True)
            else:
                df = pd.DataFrame([daily_data])
            
            # Sort and optimize before saving
            df = df.sort_values(['timestamp', 'repository']).reset_index(drop=True)
            df.to_csv(summary_file, index=False)
        except Exception as e:
            logging.error(f"Error updating summary: {str(e)}")
            raise

    def collect_metrics(self) -> None:
        """Collect metrics with parallel processing where possible."""
        metrics = {
            "views": "traffic/views",
            "clones": "traffic/clones",
            "referrers": "traffic/popular/referrers",
            "paths": "traffic/popular/paths"
        }
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d")
        daily_data = {"timestamp": timestamp, "repository": self.repo}

        # Collect metrics in parallel where possible
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_metric = {
                executor.submit(self._make_request, endpoint): metric_name
                for metric_name, endpoint in metrics.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_metric):
                metric_name = future_to_metric[future]
                try:
                    data = future.result()
                    self._save_raw_data(metric_name, data, timestamp)
                    
                    if metric_name in ["views", "clones"]:
                        daily_data[f"{metric_name}_count"] = data.get("count", 0)
                        daily_data[f"{metric_name}_uniques"] = data.get("uniques", 0)
                    elif metric_name == "referrers":
                        daily_data.update(self._process_referrers(data))
                    elif metric_name == "paths":
                        daily_data.update(self._process_paths(data))
                except Exception as e:
                    logging.error(f"Error collecting {metric_name}: {str(e)}")
                    if metric_name in ["views", "clones"]:
                        daily_data[f"{metric_name}_count"] = 0
                        daily_data[f"{metric_name}_uniques"] = 0
                    elif metric_name == "referrers":
                        daily_data.update(self._process_referrers(None))
                    elif metric_name == "paths":
                        daily_data.update(self._process_paths(None))

        self._update_summary(daily_data)

def main():
    parser = argparse.ArgumentParser(description='Collect GitHub traffic metrics.')
    parser.add_argument('repo', help='The name of the repository to collect traffic data for.')
    args = parser.parse_args()

    collector = GitHubTrafficCollector(args.repo)
    collector.collect_metrics()

if __name__ == "__main__":
    main()