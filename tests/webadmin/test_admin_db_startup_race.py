"""Gunicorn workers open a brand-new database at the same moment."""

import multiprocessing

from admin import db


def _open(path, barrier, out):
    barrier.wait()
    try:
        conn = db.connect(path)
        db.init_schema(conn)
        out.put("ok")
    except Exception as exc:  # reported to the parent
        out.put(repr(exc))


def test_parallel_first_start_on_a_fresh_database(tmp_path):
    for attempt in range(20):
        path = str(tmp_path / f"fresh{attempt}.db")
        ctx = multiprocessing.get_context("fork")
        barrier, out = ctx.Barrier(6), ctx.Queue()
        procs = [ctx.Process(target=_open, args=(path, barrier, out)) for _ in range(6)]
        for p in procs:
            p.start()
        results = [out.get(timeout=60) for _ in procs]
        for p in procs:
            p.join()
        assert results == ["ok"] * 6, results
