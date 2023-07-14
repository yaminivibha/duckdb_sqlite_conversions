import click
import duckdb
import os


@click.command()
@click.argument("duckdbname")
@click.argument("sqlitedbname")
def duckdb2sqlite(duckdbname, sqlitedbname):
  """
  Move all tables from duckdb into sqlitedb

  duckdb: file path to input duckdb file
  sqlitedb: file path to output sqlite file
  """
  db = duckdb.connect(duckdbname)

  os.system("mkdir -p ./csvs/")
  for (tname,) in db.sql("SELECT table_name FROM information_schema.tables").fetchall():
    exprs = []
    for (attr, data_type) in db.sql(f"select column_name, data_type from information_schema.columns where table_name = '{tname}'").fetchall():
      if "struct" in data_type.lower():
        exprs.append(f""" "{attr}"::json as "{attr}" """)
      else:
        exprs.append(f'"{attr}"')

    q = f"""COPY (SELECT {", ".join(exprs)} FROM {tname}) TO './csvs/{tname}.csv' (format csv, header)"""
    print(q)
    db.sql(q)

  cmds = [
    ".mode csv",
    ".headers on"
  ]
  for fname in os.listdir("./csvs/"):
    if not fname.endswith(".csv"): continue
    tname = fname.split(".")[0]
    cmds.extend([
      f"drop table if exists {tname};",
      f".import ./csvs/{fname} {tname}"
    ])

  with open("loadintosqlite.txt","w") as f:
    f.write("\n".join(cmds))

  os.system(f"sqlite3 {sqlitedbname} '.read loadintosqlite.txt'")


if __name__ == "__main__":
  duckdb2sqlite()