import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text

app = FastAPI(title="ACME API")

# Railway entrega mysql:// pero SQLAlchemy necesita mysql+pymysql://
DATABASE_URL = os.environ["MYSQL_URL"]

# Corrección automática del prefijo
if DATABASE_URL.startswith("mysql://"):
    DATABASE_URL = DATABASE_URL.replace("mysql://", "mysql+pymysql://", 1)

engine = create_engine(DATABASE_URL)

@app.get("/")
def home():
    return {"status": "API ACME funcionando correctamente"}

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
    return dict(result) if result else {}
