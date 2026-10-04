import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
BASE = Path('/run/media/arnav/ESD-USB/assignment3/data/output')
tags = {'spark': 'million-measured-20261005-032053', 'ray': 'million-measured-20261005-032749'}
spark = pd.read_parquet(BASE / f"spark-{tags['spark']}")
ray = pd.read_parquet(BASE / f"ray-{tags['ray']}")
columns = list(spark.columns)
assert set(columns) == set(ray.columns)
pd.testing.assert_frame_equal(
    spark.sort_values(columns).reset_index(drop=True),
    ray[columns].sort_values(columns).reset_index(drop=True), check_exact=True,
)
report = {'result': 'PASS', 'rows': len(spark), 'columns': len(columns),
          'exact_values_dtypes_and_row_multiplicity': True, 'runs': tags}
(ROOT / 'evidence/measured-million-parity.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
