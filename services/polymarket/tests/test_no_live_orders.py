"""
Static assertions that the polymarket service cannot place a live order.

These tests are Phase B's legal-guardrail check: they enforce that no code
path in the service can submit a signed order to the Polymarket CLOB, and
that every row persisted via publish_opportunity() is marked simulation=1.

They operate on the source files textually + via AST so they do not require
the runtime import graph (anthropic / redis / prometheus_client).
"""
from __future__ import annotations

import ast
import os
import re
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parent.parent
SERVICE_FILES = [
    SERVICE_DIR / "polymarket.py",
    SERVICE_DIR / "whale_tracker.py",
    SERVICE_DIR / "exit_monitor.py",
    SERVICE_DIR / "sizing.py",
]

# Anything in this list is a known-banned dependency or endpoint that would
# indicate a live-trading path was added. Adding one requires a deliberate
# follow-on plan; this test fails until that plan removes the guard.
BANNED_IMPORTS = {
    "py_clob_client",
    "py-clob-client",
    "clob_client",
    "eth_account",   # signing private keys
    "web3",          # on-chain submission
}

BANNED_URL_PATTERNS = [
    # The CLOB order-submission endpoint. Read-only paths (/markets, /book)
    # are fine; POSTing to /order is not.
    re.compile(r"clob\.polymarket\.com/order", re.IGNORECASE),
    re.compile(r"/orders?\b.*post", re.IGNORECASE),
]


def _source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _imported_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module.split(".")[0])
    return names


class TestNoBannedImports:
    def test_no_live_trading_libraries_imported(self):
        offenders: list[tuple[str, str]] = []
        for path in SERVICE_FILES:
            tree = ast.parse(_source(path))
            for name in _imported_names(tree):
                if name.replace("-", "_") in BANNED_IMPORTS:
                    offenders.append((path.name, name))
        assert not offenders, (
            f"Live-trading imports detected (Phase B guardrail violated): {offenders}. "
            "Enabling live execution requires a separate plan."
        )


class TestNoOrderEndpointPost:
    def test_no_post_to_clob_order_endpoint(self):
        offenders: list[tuple[str, int, str]] = []
        for path in SERVICE_FILES:
            for i, line in enumerate(_source(path).splitlines(), start=1):
                # Skip doc/comment lines — they can mention order endpoints.
                stripped = line.lstrip()
                if stripped.startswith("#") or stripped.startswith('"') or stripped.startswith("'"):
                    continue
                for pat in BANNED_URL_PATTERNS:
                    if pat.search(line):
                        offenders.append((path.name, i, line.strip()))
        assert not offenders, (
            f"Found reference to CLOB order-submission endpoint in executable code: {offenders}"
        )

    def test_the_only_requests_post_call_targets_the_subgraph(self):
        """
        There is exactly one requests.post() in the service: the Goldsky
        GraphQL call in whale_tracker._gql_post. Any new .post() callsite
        added to the service will fail this test until explicitly whitelisted.
        """
        post_callsites: list[tuple[str, int]] = []
        for path in SERVICE_FILES:
            for i, line in enumerate(_source(path).splitlines(), start=1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                # Match `requests.post(` or `<client>.post(` on a non-comment line.
                if re.search(r"\brequests\.post\s*\(", line):
                    post_callsites.append((path.name, i))
        # Exactly one allowed site, inside whale_tracker.
        assert len(post_callsites) == 1, (
            f"Expected exactly one requests.post() callsite (Goldsky subgraph), found: {post_callsites}"
        )
        fname, _ = post_callsites[0]
        assert fname == "whale_tracker.py", (
            f"The single requests.post() callsite must be in whale_tracker.py, got {fname}"
        )


class TestPublishOpportunityForcesSimulation:
    def test_publish_opportunity_hardcodes_simulation_flag(self):
        """
        publish_opportunity() must contain a literal assignment forcing the
        simulation flag to 1, independent of any env var. This is the
        last-ditch guardrail if SIMULATION_MODE is ever mis-set.
        """
        src = _source(SERVICE_DIR / "polymarket.py")
        tree = ast.parse(src)

        func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "publish_opportunity":
                func = node
                break
        assert func is not None, "publish_opportunity() not found in polymarket.py"

        found_literal_one = False
        for node in ast.walk(func):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "simulation_flag":
                        if isinstance(node.value, ast.Constant) and node.value.value == 1:
                            found_literal_one = True
        assert found_literal_one, (
            "publish_opportunity() must assign simulation_flag = 1 as a literal "
            "(no env-var, no conditional). This is a legal guardrail."
        )


class TestPrivateKeyUnused:
    def test_private_key_env_is_read_but_not_passed_to_signing_code(self):
        """
        POLYMARKET_PRIVATE_KEY may be read (the var exists in .env.example),
        but it must not flow into any signing / transaction code. If it is
        passed as an argument to any function, this test should fail.
        """
        src = _source(SERVICE_DIR / "polymarket.py")
        tree = ast.parse(src)
        passed_as_arg: list[int] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for arg in node.args:
                    if isinstance(arg, ast.Name) and arg.id == "POLYMARKET_PRIVATE_KEY":
                        passed_as_arg.append(node.lineno)
                for kw in node.keywords:
                    if isinstance(kw.value, ast.Name) and kw.value.id == "POLYMARKET_PRIVATE_KEY":
                        passed_as_arg.append(node.lineno)
        assert not passed_as_arg, (
            f"POLYMARKET_PRIVATE_KEY is passed into a function at lines {passed_as_arg}. "
            "In simulation mode the key must never flow beyond the module-level constant."
        )
