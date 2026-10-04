import os
import shutil
import subprocess
import time
from pathlib import Path

USB = Path('/run/media/arnav/ESD-USB')
GIB = 1024 ** 3
CONTAINERS = {
    'a3-spark-master', 'a3-spark-worker-a', 'a3-spark-worker-b',
    'a3-spark-check', 'a3-spark-driver',
    'a3-ray-head', 'a3-ray-worker-a', 'a3-ray-worker-b', 'a3-ray-driver',
}


def check_space(device):
    if not USB.is_mount():
        raise RuntimeError('USB is not mounted at the expected path.')
    if device is not None and USB.stat().st_dev != device:
        raise RuntimeError('USB mount changed.')
    root_free = shutil.disk_usage('/').free / GIB
    usb_free = shutil.disk_usage(USB).free / GIB
    if root_free < 3 or usb_free < 4:
        raise RuntimeError(f'Low free space: internal={root_free:.2f} GiB, USB={usb_free:.2f} GiB.')
    if not os.access(USB, os.W_OK):
        raise RuntimeError('USB is not writable.')
    return root_free, usb_free


def stop_assignment():
    result = subprocess.run(
        ['docker', 'ps', '--format', '{{.Names}}'],
        capture_output=True, text=True, check=True, timeout=10,
    )
    names = sorted(CONTAINERS.intersection(result.stdout.splitlines()))
    if names:
        subprocess.run(['docker', 'stop', '--time', '5', *names], check=True, timeout=20)
        print('Stopped assignment containers: ' + ', '.join(names), flush=True)
    else:
        print('No assignment containers are running.', flush=True)


def main():
    device = None
    count = 0
    try:
        while True:
            root_free, usb_free = check_space(device)
            if device is None:
                device = USB.stat().st_dev
                print('Guard active: checks every 2 seconds; stops assignment containers below 3 GiB internal or 4 GiB USB free.', flush=True)
            if count % 15 == 0:
                print(f'{time.strftime("%H:%M:%S")} Free: internal={root_free:.2f} GiB, USB={usb_free:.2f} GiB', flush=True)
            count += 1
            time.sleep(2)
    except (OSError, RuntimeError) as error:
        print(f'STOP: {error}', flush=True)
        try:
            stop_assignment()
        except (OSError, subprocess.SubprocessError) as stop_error:
            print(f'Automatic stop failed: {stop_error}. Stop assignment jobs manually now.', flush=True)
        raise SystemExit(1)
    except KeyboardInterrupt:
        print('\nGuard stopped. Disk protection is no longer active.', flush=True)


if __name__ == '__main__':
    main()
