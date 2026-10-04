import hashlib
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from disk_guard import GIB, USB, check_space

HERE = Path(__file__).resolve().parent
MANIFEST = HERE / 'evidence' / 'dataset-manifest.json'


def download(item, folder, device, previous):
    check_space(device)
    path = folder / item['file']
    digest = hashlib.sha256()
    if path.exists():
        if path.stat().st_size != item['bytes']:
            raise RuntimeError(f'Existing file has the wrong size: {path}')
        with path.open('rb') as source:
            while chunk := source.read(1024 * 1024):
                check_space(device)
                digest.update(chunk)
        if previous and digest.hexdigest() != previous['sha256']:
            raise RuntimeError(f'Existing file checksum changed: {path}')
        print(f'Verified existing {path.name}', flush=True)
    else:
        partial = path.with_suffix(path.suffix + '.part')
        print(f'Downloading {path.name} ({item["bytes"] / 1e6:.1f} MB)', flush=True)
        count = 0
        with urllib.request.urlopen(item['url'], timeout=30) as response:
            if response.status != 200:
                raise RuntimeError(f'Unexpected HTTP status: {response.status}')
            with partial.open('wb') as target:
                while chunk := response.read(1024 * 1024):
                    check_space(device)
                    count += len(chunk)
                    if count > item['bytes']:
                        raise RuntimeError(f'Remote size changed: {path.name}')
                    target.write(chunk)
                    digest.update(chunk)
                target.flush()
                os.fsync(target.fileno())
        if count != item['bytes']:
            raise RuntimeError(f'Incomplete download: {path.name}, {count}/{item["bytes"]} bytes')
        check_space(device)
        partial.replace(path)
    return {
        **item, 'path': str(path), 'sha256': digest.hexdigest(),
        'verified_at_utc': datetime.now(timezone.utc).isoformat(),
    }


def main():
    check_space(None)
    device = USB.stat().st_dev
    plan = json.loads((HERE / 'dataset_plan.json').read_text())
    entries = [(item, USB / 'assignment3/data/raw') for item in plan['files']]
    entries.append((plan['lookup'], USB / 'assignment3/data/lookup'))
    remaining = sum(item['bytes'] for item, folder in entries if not (folder / item['file']).exists())
    _, usb_free = check_space(device)
    if usb_free * GIB < remaining + 4 * GIB:
        raise RuntimeError('Not enough USB space for the downloads plus the 4 GiB reserve.')
    saved = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {'source_page': plan['source_page'], 'files': []}
    previous = {item['file']: item for item in saved['files']}
    for index, (item, folder) in enumerate(entries, 1):
        check_space(device)
        folder.mkdir(parents=True, exist_ok=True)
        record = download(item, folder, device, previous.get(item['file']))
        previous[item['file']] = record
        saved['files'] = list(previous.values())
        temporary = MANIFEST.with_suffix('.json.part')
        temporary.write_text(json.dumps(saved, indent=2) + '\n')
        temporary.replace(MANIFEST)
        print(f'[{index}/{len(entries)}] Saved and verified {item["file"]}', flush=True)
    print(f'DONE: {len(plan["files"])} trip files ({plan["total_bytes"] / 1e9:.3f} GB) and zone lookup. Manifest saved.', flush=True)


if __name__ == '__main__':
    main()
