import csv
import shutil
import tempfile
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

from fastapi import FastAPI, Depends, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = "postgresql://homepass_user:1vfkjjK8nNlFLNE@127.0.0.1:5432/homepass"
PICTURES_SOURCE = Path("pictures")
PICTURES_DEST_ROOT = Path("/srv/homepass")
CSV_SOURCE = Path("homes.csv")
PICTURES_ROOT = PICTURES_DEST_ROOT / "pictures"
SERVER_CSV = PICTURES_DEST_ROOT / "homes.csv"
PICTURES_URL_BASE = "http://jumbo.galagen.net:2205/pictures"


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow calls from the browser-based frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ApartmentOut(BaseModel):
    id: int
    name: str
    address: str
    area: int | None = None
    cost: float | None = None
    tokens: int | None = None
    type: str | None = None
    images: list[str] = Field(default_factory=list)

    class Config:
        orm_mode = True


class UserOut(BaseModel):
    id: int
    wallet: str
    name: str
    surname: str

    class Config:
        orm_mode = True


def _resolve_image_url(value: str | None) -> str | None:
    if not value:
        return None

    candidate = Path(value)
    if candidate.is_absolute():
        try:
            candidate = candidate.relative_to(PICTURES_ROOT)
        except ValueError:
            pass

    parts = [part for part in candidate.as_posix().split("/") if part]
    if parts and parts[0].lower() == "pictures":
        parts = parts[1:]

    if not parts:
        return None

    normalized = "/".join(parts)
    return f"{PICTURES_URL_BASE}/{normalized}"


def _get_image_paths(db, apartment_id: int) -> list[str]:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT file_path
            FROM pictures
            WHERE apartment_id = %s
            ORDER BY is_primary DESC, id;
            """,
            (apartment_id,),
        )
        return [row["file_path"] for row in cur.fetchall()]


def _attach_images(db, rows: list[dict]) -> None:
    for row in rows:
        raw_paths = _get_image_paths(db, row["id"])
        row["images"] = [p for p in (_resolve_image_url(r) for r in raw_paths) if p]


def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def ensure_user_exists(db, user_id: int) -> bool:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM users WHERE id = %s;", (user_id,))
        return cur.fetchone() is not None


def copy_pictures_folder(source: Path, dest_root: Path) -> Path:
    if not source.exists():
        raise FileNotFoundError(f"Local pictures directory not found: {source}")

    dest_root.mkdir(parents=True, exist_ok=True)
    dest_path = dest_root / "pictures"
    shutil.copytree(source, dest_path, dirs_exist_ok=True)
    return dest_path


def ensure_pictures_available() -> None:
    if PICTURES_ROOT.exists():
        return
    try:
        copy_pictures_folder(PICTURES_SOURCE, PICTURES_DEST_ROOT)
    except FileNotFoundError:
        pass


def insert_apartments_from_csv(cur, csv_path: Path, user_id: int) -> int:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    inserted = 0
    with csv_path.open("r", encoding="utf-8", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            name = (row.get("title") or row.get("name") or row.get("url") or "").strip()
            address = (row.get("address") or row.get("adress") or "").strip()
            if not name or not address:
                continue

            area_raw = (row.get("area_m2") or row.get("area") or "").replace(",", "").strip()
            try:
                area_val = int(float(area_raw))
            except (ValueError, TypeError):
                continue

            price_raw = (row.get("price_usd") or row.get("cost") or "").replace(",", "").strip()
            try:
                cost_val = Decimal(price_raw)
            except (InvalidOperation, TypeError):
                continue

            apt_type = (row.get("type") or "Flat").strip()

            cur.execute(
                """
                SELECT 1 FROM apartaments
                WHERE name = %s AND adress = %s AND user_id = %s;
                """,
                (name, address, user_id),
            )
            if cur.fetchone():
                continue

            cur.execute(
                """
                INSERT INTO apartaments (user_id, name, adress, area, cost, tokens, type)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (user_id, name, address, area_val, cost_val, 0, apt_type),
            )
            inserted += 1
    return inserted


@app.get("/houses", response_model=list[ApartmentOut])
def get_houses(n: int = 10, db=Depends(get_db)):
    ensure_pictures_available()
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                adress AS address,
                area,
                cost,
                tokens,
                type::text AS type
            FROM apartaments
            ORDER BY id
            LIMIT %s;
            """,
            (n,),
        )
        rows = cur.fetchall()

    _attach_images(db, rows)
    return rows


@app.get("/user_houses", response_model=list[ApartmentOut])
def get_user_houses(user_id: int, n: int = 10, db=Depends(get_db)):
    ensure_pictures_available()
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                name,
                adress AS address,
                area,
                cost,
                tokens,
                type::text AS type
            FROM apartaments
            WHERE user_id = %s
            ORDER BY id
            LIMIT %s;
            """,
            (user_id, n),
        )
        rows = cur.fetchall()
    _attach_images(db, rows)
    return rows


@app.get("/user/{user_id}", response_model=UserOut)
def get_user(user_id: int, db=Depends(get_db)):
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                wallet,
                name,
                surname
            FROM users
            WHERE id = %s;
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return row




@app.post("/import_csv")
def import_csv(
    user_id: int = 1,
    csv_path: str | None = None,
    db=Depends(get_db),
):
    if not ensure_user_exists(db, user_id):
        raise HTTPException(status_code=404, detail="User not found")

    path = Path(csv_path) if csv_path else SERVER_CSV
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{path} not found")

    with db.cursor() as cur:
        inserted = insert_apartments_from_csv(cur, path, user_id)
    db.commit()

    return {
        "source_csv": str(path),
        "inserted_apartments": inserted,
    }
