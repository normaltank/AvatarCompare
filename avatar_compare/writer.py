"""One durable SQLite writer, grouping bounded concurrent submissions per commit."""
from __future__ import annotations

from dataclasses import dataclass, field
import queue
import sqlite3
import threading
import time
from concurrent.futures import Future


@dataclass
class Submission:
    statements: list
    script: str | None = None
    transaction: object = None
    event: threading.Event = field(default_factory=threading.Event)
    rowcounts: object = field(default_factory=list)
    error: Exception | None = None
    future: Future = field(default_factory=Future)


class SharedWriter:
    def __init__(self, path, capacity=256, batch_size=64, batch_wait=0.002):
        self.path, self.batch_size, self.batch_wait = path, batch_size, batch_wait
        self.queue = queue.Queue(maxsize=capacity)
        self.ready = threading.Event()
        self.start_error = None
        self.closed = False
        self.commits = self.submissions = self.peak_batch = 0
        self.thread = threading.Thread(target=self.run, daemon=True, name='avatar-sqlite-writer')
        self.thread.start()
        self.ready.wait()
        if self.start_error:
            raise self.start_error

    def run(self):
        db = None
        try:
            db = sqlite3.connect(self.path, timeout=30)
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
        except Exception as error:
            if db is not None:
                db.close()
            self.start_error = error
            self.ready.set()
            return
        self.ready.set()
        try:
            carry = None
            while True:
                first = carry if carry is not None else self.queue.get()
                carry = None
                if first is None:
                    break
                batch = [first]
                if first.script is None:
                    deadline = time.monotonic()+self.batch_wait
                    while len(batch) < self.batch_size and time.monotonic() < deadline:
                        try:
                            item = self.queue.get(timeout=max(0, deadline-time.monotonic()))
                        except queue.Empty:
                            break
                        if item is None or item.script is not None:
                            carry = item
                            if item is None:
                                # close() is called only once all submitters have joined.
                                self.queue.put(None)
                            break
                        batch.append(item)
                try:
                    if first.script is not None:
                        db.executescript(first.script)
                    else:
                        with db:
                            for item in batch:
                                if item.transaction is not None:
                                    # Read/write callbacks observe earlier scopes in
                                    # this transaction and never commit themselves.
                                    item.rowcounts=item.transaction(db)
                                else:item.rowcounts = [db.execute(sql, parameters).rowcount for sql, parameters in item.statements]
                    self.commits += 1
                    self.submissions += len(batch)
                    self.peak_batch = max(self.peak_batch, len(batch))
                except Exception as error:
                    for item in batch:
                        item.error = error
                    try:
                        db.rollback()
                    except sqlite3.Error:
                        # The original failed commit is still reported to every
                        # caller; rollback failure must never turn into success.
                        pass
                finally:
                    # Acknowledge only after commit: a caller never mistakes a
                    # queued write for a durable completed result.
                    for item in batch:
                        item.event.set()
                        if item.error:
                            item.future.set_exception(item.error)
                        else:
                            item.future.set_result(item.rowcounts)
        finally:
            db.close()

    def submit(self, statements=(), *, script=None, transaction=None):
        return self.submit_future(statements, script=script, transaction=transaction).result()

    def submit_future(self, statements=(), *, script=None, transaction=None):
        if self.closed:
            raise RuntimeError('写入队列已经关闭')
        if transaction is not None and (script is not None or statements):
            raise ValueError('事务回调不能与SQL批次或脚本混用')
        submission = Submission(list(statements), script=script, transaction=transaction)
        submission.future.set_running_or_notify_cancel()
        self.queue.put(submission)
        return submission.future

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.queue.put(None)
        self.thread.join()


class BufferedConnection:
    """Read on the caller's connection; send complete write scopes to one owner."""
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.pending = None

    def __enter__(self):
        if self.pending is not None:
            raise RuntimeError('嵌套写入事务不受支持')
        self.pending = []
        return self

    def __exit__(self, kind, value, traceback):
        statements, self.pending = self.pending, None
        if kind is None and statements:
            self.writer.submit(statements)
        return False

    def execute(self, sql, parameters=()):
        if sql.lstrip().upper().startswith(('SELECT', 'PRAGMA', 'EXPLAIN')):
            if self.pending:
                raise RuntimeError('写入事务尚未提交，不能从独立读连接读取')
            return self.reader.execute(sql, parameters)
        if self.pending is None:
            return self.write_batch([(sql, parameters)])
        self.pending.append((sql, parameters))
        return None

    def executescript(self, script):
        self.writer.submit(script=script)

    def commit(self):
        if self.pending:
            raise RuntimeError('请通过写入事务上下文完成提交')

    def write_batch(self, statements):
        if self.pending is not None:
            raise RuntimeError('写入批次不能嵌套')
        return self.writer.submit(statements)
