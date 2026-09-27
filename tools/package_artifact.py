#!/usr/bin/env python3
"""Build the artifact inventory and deterministic ZIP, outside the source tree."""
from pathlib import Path
import argparse
import hashlib
import zipfile


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=root.parent / (root.name + '.zip'))
    args = parser.parse_args()
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error('ZIP output must be outside the artifact directory')
    skipped = {'.git', '__pycache__', '.venv', '.terraform', 'reproduced', 'dist'}
    files = []
    for path in sorted(root.rglob('*')):
        rel = path.relative_to(root)
        if any(part in skipped for part in rel.parts):
            continue
        if path.is_symlink():
            raise SystemExit(f'Refusing symbolic link: {rel}')
        if path.is_file() and rel.as_posix() != 'SHA256SUMS':
            files.append(path)
    inventory = root / 'SHA256SUMS'
    inventory.write_text(''.join(
        f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root).as_posix()}\n'
        for path in files), encoding='utf-8')
    files.append(inventory)
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(files):
            info = zipfile.ZipInfo(root.name + '/' + path.relative_to(root).as_posix(),
                                   date_time=(2026, 9, 27, 0, 0, 0))
            info.create_system = 3
            mode = 0o100755 if path.suffix == '.sh' else 0o100644
            info.external_attr = mode << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes(), compresslevel=9)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + '.sha256').write_text(
        f'{digest}  {output.name}\n', encoding='utf-8')
    print(f'{output}: {len(files)} files, {output.stat().st_size} bytes')
    print(f'SHA256: {digest}')


if __name__ == '__main__':
    main()
