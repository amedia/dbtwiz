#!/usr/bin/env python3
"""Test runner script for dbtwiz."""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ SUCCESS")
        if result.stdout:
            print("Output:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("❌ FAILED")
        print(f"Exit code: {e.returncode}")
        if e.stdout:
            print("Stdout:")
            print(e.stdout)
        if e.stderr:
            print("Stderr:")
            print(e.stderr)
        return False


def main():
    """Main test runner function."""
    print("🚀 dbtwiz Test Runner")
    print("=" * 60)

    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Error: pyproject.toml not found. Please run from the project root.")
        sys.exit(1)

    # Install test dependencies
    print("\n📦 Installing test dependencies...")
    if not run_command(
        ["poetry", "install", "--with", "test"], "Install test dependencies"
    ):
        print("❌ Failed to install test dependencies")
        sys.exit(1)

    # Run tests with different configurations
    test_configs = [
        (["poetry", "run", "pytest", "--version"], "Check pytest version"),
        (["poetry", "run", "pytest", "--collect-only"], "Collect test discovery"),
        (
            ["poetry", "run", "pytest", "-v", "--tb=short"],
            "Run all tests with verbose output",
        ),
        (
            ["poetry", "run", "pytest", "--cov=dbtwiz", "--cov-report=term-missing"],
            "Run tests with coverage",
        ),
        (
            ["poetry", "run", "pytest", "--cov=dbtwiz", "--cov-report=html"],
            "Generate HTML coverage report",
        ),
        (
            ["poetry", "run", "pytest", "-m", "not slow", "--tb=short"],
            "Run fast tests only",
        ),
    ]

    success_count = 0
    total_count = len(test_configs)

    for cmd, description in test_configs:
        if run_command(cmd, description):
            success_count += 1

    # Summary
    print(f"\n{'=' * 60}")
    print("📊 TEST SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total test configurations: {total_count}")
    print(f"Successful: {success_count}")
    print(f"Failed: {total_count - success_count}")

    if success_count == total_count:
        print("\n🎉 All tests passed successfully!")
        sys.exit(0)
    else:
        print(f"\n⚠️  {total_count - success_count} test configuration(s) failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
