"""Fixture-level checks; intentionally independent of pyspark and Databricks."""
import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FILE = ROOT / "data" / "sample_longlamp_style.jsonl"

class FixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.requests = [json.loads(line) for line in FILE.read_text().splitlines() if line.strip()]
        cls.papers = [p for request in cls.requests for p in request["profile"]]

    def test_eight_requests(self):
        self.assertEqual(len(self.requests), 8)
        self.assertEqual(len({r['request_id'] for r in self.requests}), 8)

    def test_twenty_eight_profiles(self):
        self.assertEqual(len(self.papers), 28)
        self.assertTrue(all(set(['id', 'title', 'abstract', 'year', 'topic']) <= p.keys() for p in self.papers))

    def test_expected_clean_count(self):
        valid = [p for p in self.papers
                 if p['id'] and p['title'].strip()
                 and p['year'].isdigit() and 2000 <= int(p['year']) <= 2030]
        self.assertEqual(len({p['id'] for p in valid}), 24)
        self.assertEqual(len(valid) - len({p['id'] for p in valid}), 2)

    def test_notebook_present_and_embedded_fixture_matches(self):
        notebook = (ROOT / 'notebooks' / '01_medallion_pipeline.py').read_text()
        self.assertIn('CREATE VOLUME IF NOT EXISTS', notebook)
        self.assertIn('dropDuplicates(["paper_id"])', notebook)
        self.assertIn('try_cast(paper.year AS INT)', notebook)
        self.assertIn('SAMPLE_REQUESTS = ', notebook)
        import ast
        module = ast.parse(notebook)
        embedded = next(ast.literal_eval(node.value) for node in module.body
                        if isinstance(node, ast.Assign) and
                        any(isinstance(target, ast.Name) and target.id == 'SAMPLE_REQUESTS' for target in node.targets))
        self.assertEqual(embedded, self.requests)

    def test_student_challenge_is_editable(self):
        challenge = (ROOT / 'notebooks' / '02_student_challenge.py').read_text()
        self.assertIn('by_topic_df = None', challenge)
        self.assertIn('TODO 1', challenge)

if __name__ == '__main__':
    unittest.main()
