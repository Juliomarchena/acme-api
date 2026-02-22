import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text

app = FastAPI(title="ACME API")

DATABASE_URL = os.environ["MYSQL_URL"]

engine = create_engine(DATABASE_URL)

@app.get("/")
def home():
    return {"status": "API funcionando"}

@app.get("/producto-mas-solicitado")
def producto_mas_solicitado():

    query = """
    SELECT 
        p.Descripcion AS producto,
        SUM(pe.Cant) AS total_unidades,
        SUM(pe.Importe) AS total_importe
    FROM pedidos pe
    JOIN productos p
      ON pe.Fab = p.Id_Fab
     AND pe.Producto = p.Id_Producto
    GROUP BY p.Descripcion
    ORDER BY total_unidades DESC
    LIMIT 1;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()

    return result if result else {}
