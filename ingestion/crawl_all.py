import argparse
import fnmatch
import subprocess
import sys
import time
from pathlib import Path

# Các folder bị bỏ qua khi quét
EXCLUDE_DIRS = {"__pycache__", ".git", ".venv", "venv", "env", "node_modules", ".idea", ".vscode"}


def find_scripts(root: Path, pattern: str, self_path: Path):
    scripts = []
    for path in root.rglob("*.py"):
        if path.resolve() == self_path:
            continue
        if any(part in EXCLUDE_DIRS for part in path.relative_to(root).parts):
            continue
        if not fnmatch.fnmatch(path.name, pattern):
            continue
        scripts.append(path)
    return sorted(scripts)  # sắp xếp theo đường dẫn để thứ tự ổn định


def run_script(path: Path, timeout):
    start = time.time()
    try:
        # cwd = folder chứa script, để các đường dẫn tương đối trong script vẫn đúng
        result = subprocess.run(
            [sys.executable, path.name],
            cwd=path.parent,
            timeout=timeout,
        )
        status = "OK" if result.returncode == 0 else f"FAIL (exit {result.returncode})"
    except subprocess.TimeoutExpired:
        status = f"TIMEOUT (>{timeout}s)"
    return status, time.time() - start


def main():
    parser = argparse.ArgumentParser(description="Chạy hàng loạt script Python trong các folder con")
    parser.add_argument("--root", default=".", help="Thư mục gốc để quét (mặc định: thư mục hiện tại)")
    parser.add_argument("--pattern", default="*.py", help="Mẫu tên file, vd: main.py hoặc 'job_*.py'")
    parser.add_argument("--timeout", type=int, default=None, help="Giới hạn thời gian mỗi script (giây)")
    parser.add_argument("--stop-on-error", action="store_true", help="Dừng khi có script lỗi")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ liệt kê script sẽ chạy")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    scripts = find_scripts(root, args.pattern, Path(__file__).resolve())

    if not scripts:
        print("Không tìm thấy script nào.")
        return 0

    print(f"Tìm thấy {len(scripts)} script trong {root}\n")
    if args.dry_run:
        for s in scripts:
            print(" -", s.relative_to(root))
        return 0

    results = []
    for i, script in enumerate(scripts, 1):
        rel = script.relative_to(root)
        print(f"[{i}/{len(scripts)}] >>> {rel}")
        status, secs = run_script(script, args.timeout)
        results.append((rel, status, secs))
        print(f"    -> {status} ({secs:.1f}s)\n")
        if status != "OK" and args.stop_on_error:
            print("Dừng vì --stop-on-error.")
            break

    print("=" * 60)
    print("TỔNG KẾT")
    for rel, status, secs in results:
        print(f"{status:<22} {secs:>7.1f}s  {rel}")
    failed = [r for r in results if r[1] != "OK"]
    print(f"\nThành công: {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())