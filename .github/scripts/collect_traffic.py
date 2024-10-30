import argparse
import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time

class GitHubTrafficCollector:
    def __init__(self, repo):
        self.token = os.environ['GH_TOKEN']
        self.repo = repo
        self.org = "CCP-NC"
        self.base_url = f"https://api.github.com/repos/{self.org}/{self.repo}"
        self.stats_dir = Path("traffic-stats")
        self.stats_dir.mkdir(parents=True, exist_ok=True)

        
        logging.basicConfig(
            filename=self.stats_dir / "collector.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def _make_request(self, endpoint, retries=3, delay=2):
        headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }
        for attempt in range(retries):
            response = requests.get(f"{self.base_url}/{endpoint}", headers=headers)
            if response.status_code == 200:
                return response.json()
            elif attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                response.raise_for_status()

    def collect_metrics(self):
        metrics = {
            "views": "traffic/views",
            "clones": "traffic/clones",
            "referrers": "traffic/popular/referrers",
            "paths": "traffic/popular/paths"
        }
        
        timestamp = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Collect all metrics
        daily_data = {
            "timestamp": timestamp,
            "repository": self.repo
        }

        for metric_name, endpoint in metrics.items():
            try:
                data = self._make_request(endpoint)
                self._save_raw_data(metric_name, data, timestamp)
                
                if metric_name in ["views", "clones"]:
                    daily_data[f"{metric_name}_count"] = data.get("count", 0)
                    daily_data[f"{metric_name}_uniques"] = data.get("uniques", 0)
                elif metric_name == "referrers":
                    referrer_metrics = self._process_referrers(data)
                    daily_data.update(referrer_metrics)
                elif metric_name == "paths":
                    path_metrics = self._process_paths(data)
                    daily_data.update(path_metrics)
            except Exception as e:
                logging.error(f"Error collecting {metric_name} for {self.repo}: {e}")
                if metric_name in ["views", "clones"]:
                    daily_data[f"{metric_name}_count"] = 0
                    daily_data[f"{metric_name}_uniques"] = 0
                elif metric_name == "referrers":
                    daily_data.update(self._process_referrers([]))
                elif metric_name == "paths":
                    daily_data.update(self._process_paths([]))

        self._update_summary(daily_data)

    def _save_raw_data(self, metric_name, data, timestamp):
        filename = self.stats_dir / f"{self.repo}-{metric_name}-{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump({
                "collected_at": datetime.utcnow().isoformat(),
                "data": data
            }, f, indent=2)


    def _update_summary(self, daily_data):
        summary_file = self.stats_dir / "summary.csv"
        df = pd.read_csv(summary_file) if summary_file.exists() else pd.DataFrame()

        # Check if the latest entry for this repo and date already exists
        if not df.empty and (df["timestamp"] == daily_data["timestamp"]).any() and (df["repository"] == daily_data["repository"]).any():
            logging.info(f"No new data for {self.repo} on {daily_data['timestamp']}")
            return  # No new data

        new_row = pd.DataFrame([daily_data])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(summary_file, index=False)


    def _process_referrers(self, referrers_data):
        """Process referrers data to extract summary metrics."""
        if not referrers_data:
            return {
                'top_referrer': 'none',
                'top_referrer_count': 0,
                'top_referrer_uniques': 0,
                'total_referrer_count': 0,
                'total_referrer_uniques': 0,
                'distinct_referrers': 0
            }

        # Sort referrers by count
        sorted_referrers = sorted(referrers_data, key=lambda x: (x.get('count', 0), x.get('uniques', 0)), reverse=True)
        
        top_referrer = sorted_referrers[0] if sorted_referrers else {'referrer': 'none', 'count': 0, 'uniques': 0}
        
        return {
            'top_referrer': top_referrer.get('referrer', 'none'),
            'top_referrer_count': top_referrer.get('count', 0),
            'top_referrer_uniques': top_referrer.get('uniques', 0),
            'total_referrer_count': sum(r.get('count', 0) for r in referrers_data),
            'total_referrer_uniques': sum(r.get('uniques', 0) for r in referrers_data),
            'distinct_referrers': len(referrers_data)
        }
    


    def _process_paths(self, paths_data):
        """Process paths data to extract summary metrics."""
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

        # Sort paths by count
        sorted_paths = sorted(paths_data, key=lambda x: (x.get('count', 0), x.get('uniques', 0)), reverse=True)
        top_path = sorted_paths[0] if sorted_paths else {'path': 'none', 'count': 0, 'uniques': 0}
        
        # Calculate README metrics (considering both /README.md and /readme.md)
        readme_paths = [p for p in paths_data if p.get('path', '').lower() == '/readme.md']
        readme_stats = readme_paths[0] if readme_paths else {'count': 0, 'uniques': 0}
        
        return {
            'top_path': top_path.get('path', 'none'),
            'top_path_count': top_path.get('count', 0),
            'top_path_uniques': top_path.get('uniques', 0),
            'total_path_count': sum(p.get('count', 0) for p in paths_data),
            'total_path_uniques': sum(p.get('uniques', 0) for p in paths_data),
            'distinct_paths': len(paths_data),
            'readme_views': readme_stats.get('count', 0),
            'readme_uniques': readme_stats.get('uniques', 0)
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect GitHub traffic metrics.')
    parser.add_argument('repo', help='The name of the repository to collect traffic data for.')
    args = parser.parse_args()

    collector = GitHubTrafficCollector(args.repo)
    collector.collect_metrics()