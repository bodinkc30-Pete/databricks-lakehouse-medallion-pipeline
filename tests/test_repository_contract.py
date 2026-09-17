from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_required_portfolio_files_exist(self):
        required = [
            "notebooks/01_spark_foundation.py",
            "sql/01_incremental_merge.sql",
            "sql/02_cdc_watermark.sql",
            "sql/03_monitoring_audit.sql",
            "docs/architecture/lakehouse_architecture.md",
            "docs/screenshots/data-quality/data_quality_summary.png",
            "docs/screenshots/reliability/reliability_test_executive_summary.png",
            "docs/screenshots/performance/performance_improvement_summary.png",
            "docs/screenshots/performance/04_photon_broadcast_hash_join.png",
            "docs/screenshots/ci/github_actions_success.png",
        ]
        missing = [path for path in required if not (ROOT / path).exists()]
        self.assertEqual(missing, [], f"Missing portfolio evidence: {missing}")

    def test_sql_is_real_and_detectable(self):
        attrs = (ROOT / ".gitattributes").read_text(encoding="utf-8-sig")
        self.assertIn("*.sql linguist-detectable=true", attrs)
        sql_text = "\n".join(
            p.read_text(encoding="utf-8-sig") for p in sorted((ROOT / "sql").glob("*.sql"))
        ).upper()
        for token in ("MERGE INTO", "CREATE TABLE", "SELECT", "UPDATE"):
            self.assertIn(token, sql_text)

    def test_readme_evidence_links_resolve(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        links = re.findall(r"\[[^\]]+\]\(([^)]+)\)", readme)
        local_links = [x.split("#", 1)[0] for x in links if not x.startswith(("http://", "https://"))]
        missing = sorted({x for x in local_links if x and not (ROOT / x).exists()})
        self.assertEqual(missing, [], f"Broken README links: {missing}")

    def test_readme_platform_identity_is_visible_near_top(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        top = "\n".join(readme.splitlines()[:90])
        for token in (
            "Databricks Lakehouse",
            "Apache Spark",
            "PySpark",
            "Spark SQL",
            "Delta Lake",
            "Unity Catalog",
            "Databricks / Spark Execution Evidence",
        ):
            self.assertIn(token, top)

    def test_no_obvious_secrets_in_text_sources(self):
        patterns = [
            re.compile(r"BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY"),
            re.compile(r"AKIA[0-9A-Z]{16}"),
            re.compile(r"sk-[A-Za-z0-9]{20,}"),
        ]
        offenders = []
        for ext in ("*.py", "*.sql", "*.md", "*.yml", "*.yaml"):
            for path in ROOT.rglob(ext):
                if ".git" in path.parts:
                    continue
                text = path.read_text(encoding="utf-8-sig", errors="ignore")
                if any(pattern.search(text) for pattern in patterns):
                    offenders.append(str(path.relative_to(ROOT)))
        self.assertEqual(offenders, [], f"Potential secrets found: {offenders}")


if __name__ == "__main__":
    unittest.main()
