"""Local unit-test/reference policy. NOT a broker, CLI, or secure deployment."""
from contextlib import contextmanager
import hashlib
import hmac
import re
import secrets
import sqlite3
import time
import uuid

_SCHEMA = """
CREATE TABLE IF NOT EXISTS admin (
    id INTEGER PRIMARY KEY CHECK (id = 1), salt BLOB NOT NULL, verifier BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY, active INTEGER NOT NULL CHECK (active IN (0,1))
);
CREATE TABLE IF NOT EXISTS invitations (
    digest BLOB PRIMARY KEY, expires INTEGER NOT NULL, used_at INTEGER
);
CREATE TABLE IF NOT EXISTS bearers (
    digest BLOB PRIMARY KEY, agent_id TEXT NOT NULL REFERENCES agents(id),
    expires INTEGER NOT NULL, revoked_at INTEGER
);
CREATE TABLE IF NOT EXISTS task_bindings (
    id TEXT PRIMARY KEY, owner TEXT NOT NULL REFERENCES agents(id)
);
"""
_BEARER_TTL = 7 * 24 * 60 * 60
_MAX_INVITE_TTL = 15 * 60


class Rejected(Exception):
    """A generic authentication or authorization failure."""


def _password_key(password, salt):
    if not isinstance(password, str):
        raise Rejected()
    encoded = password.encode("utf-8")
    if not encoded or len(encoded) > 1024:
        raise Rejected()
    return hashlib.scrypt(encoded, salt=salt, n=2**15, r=8, p=1, maxmem=128*1024**2)


def _digest(kind, token):
    n = 22 if kind == "en" else 43
    if not isinstance(token, str) or len(token) != n + 3:
        raise Rejected()
    if re.fullmatch(kind + r"_[A-Za-z0-9_-]{" + str(n) + "}", token) is None:
        raise Rejected()
    return hashlib.sha256(b"luigi-auth-reference/v1/" + token.encode("ascii")).digest()


def _secret(kind):
    token = kind + "_" + secrets.token_urlsafe(16 if kind == "en" else 32)
    return token, _digest(kind, token)


class AuthCore:
    def __init__(self, db_path, *, clock=time.time):
        self._path = db_path
        self._clock = clock
        with self._db() as db:
            db.executescript(_SCHEMA)

    @contextmanager
    def _db(self, *, tx=False):
        db = sqlite3.connect(self._path, isolation_level=None, timeout=5)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            if tx:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if tx:
                db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def _now(self):
        return int(self._clock())

    def bootstrap_admin(self, password):
        """Offline trusted setup only. Never a transport endpoint."""
        if not isinstance(password, str) or len(password) < 12:
            raise Rejected()
        salt = secrets.token_bytes(16)
        verifier = _password_key(password, salt)
        with self._db(tx=True) as db:
            try:
                db.execute("INSERT INTO admin VALUES (1, ?, ?)", (salt, verifier))
            except sqlite3.IntegrityError:
                raise Rejected() from None

    def _require_admin(self, db, password):
        row = db.execute("SELECT salt,verifier FROM admin WHERE id=1").fetchone()
        if row is None or not hmac.compare_digest(_password_key(password, row[0]), row[1]):
            raise Rejected()

    def issue_invite(self, admin_password, *, ttl=300):
        if type(ttl) is not int or not 1 <= ttl <= _MAX_INVITE_TTL:
            raise ValueError("invalid invitation lifetime")
        token, digest = _secret("en")
        with self._db(tx=True) as db:
            self._require_admin(db, admin_password)
            db.execute("INSERT INTO invitations VALUES (?, ?, NULL)", (digest, self._now() + ttl))
        return token

    def _issue_bearer(self, db, agent, now):
        token, digest = _secret("ag")
        db.execute("INSERT INTO bearers VALUES (?,?,?,NULL)", (digest, agent, now + _BEARER_TTL))
        return token

    def redeem_invite(self, invitation):
        digest = _digest("en", invitation)
        with self._db(tx=True) as db:
            now = self._now()
            updated = db.execute(
                "UPDATE invitations SET used_at=? WHERE digest=? AND used_at IS NULL AND expires>?",
                (now, digest, now),
            ).rowcount
            if updated != 1:
                raise Rejected()
            agent = uuid.uuid4().hex
            db.execute("INSERT INTO agents VALUES (?,1)", (agent,))
            token = self._issue_bearer(db, agent, now)
        return agent, token

    def _active_agent(self, db, digest, now):
        row = db.execute(
            """SELECT a.id FROM agents a JOIN bearers b ON a.id=b.agent_id
               WHERE b.digest=? AND b.expires>? AND b.revoked_at IS NULL AND a.active=1""",
            (digest, now),
        ).fetchone()
        if row is None:
            raise Rejected()
        return row[0]

    def rotate_bearer(self, bearer):
        digest = _digest("ag", bearer)
        with self._db(tx=True) as db:
            now = self._now()
            agent = self._active_agent(db, digest, now)
            db.execute("UPDATE bearers SET revoked_at=? WHERE digest=?", (now, digest))
            replacement = self._issue_bearer(db, agent, now)
        return replacement

    def revoke_agent(self, admin_password, agent_id):
        with self._db(tx=True) as db:
            self._require_admin(db, admin_password)
            if db.execute("UPDATE agents SET active=0 WHERE id=?", (agent_id,)).rowcount != 1:
                raise Rejected()
            db.execute(
                "UPDATE bearers SET revoked_at=? WHERE agent_id=? AND revoked_at IS NULL",
                (self._now(), agent_id),
            )

    def bind_new_task(self, bearer):
        """Records ownership only; does not submit, schedule, or run anything."""
        digest = _digest("ag", bearer)
        with self._db(tx=True) as db:
            owner = self._active_agent(db, digest, self._now())
            task_id = uuid.uuid4().hex
            db.execute("INSERT INTO task_bindings VALUES (?,?)", (task_id, owner))
        return task_id

    def require_task_owner(self, bearer, task_id):
        """Example own-task read check. Real mutations must include check in their transaction."""
        digest = _digest("ag", bearer)
        with self._db(tx=True) as db:
            owner = self._active_agent(db, digest, self._now())
            if not db.execute("SELECT 1 FROM task_bindings WHERE id=? AND owner=?", (task_id, owner)).fetchone():
                raise Rejected()
