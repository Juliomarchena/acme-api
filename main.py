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


# ─────────────────────────────────────────
#  HEALTH CHECK
# ─────────────────────────────────────────
@app.get("/")
def home():
    return {"status": "API ACME funcionando correctamente"}


# ─────────────────────────────────────────
#  PRODUCTOS
# ─────────────────────────────────────────

@app.get("/producto-mas-solicitado")
def producto_mas_solicitado():
    """Producto con mayor cantidad de unidades vendidas"""
    query = """
    SELECT
        p.Descripcion   AS producto,
        SUM(pe.Cant)    AS total_unidades,
        SUM(pe.Importe) AS total_importe
    FROM pedidos pe
    JOIN productos p
      ON pe.Fab = p.Id_Fab AND pe.Producto = p.Id_Producto
    GROUP BY p.Descripcion
    ORDER BY total_unidades DESC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/producto-menos-solicitado")
def producto_menos_solicitado():
    """Producto con menor cantidad de unidades vendidas"""
    query = """
    SELECT
        p.Descripcion   AS producto,
        SUM(pe.Cant)    AS total_unidades,
        SUM(pe.Importe) AS total_importe
    FROM pedidos pe
    JOIN productos p
      ON pe.Fab = p.Id_Fab AND pe.Producto = p.Id_producto
    GROUP BY p.Descripcion
    ORDER BY total_unidades ASC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/producto-mayor-margen")
def producto_mayor_margen():
    """Producto con mayor margen de ganancia (requiere columna Costo en productos)"""
    query = """
    SELECT
        Descripcion                                        AS producto,
        Precio                                             AS precio_venta,
        Costo                                              AS costo,
        ROUND((Precio - Costo), 2)                         AS ganancia_unitaria,
        ROUND((Precio - Costo) / Precio * 100, 1)          AS margen_pct
    FROM productos
    ORDER BY margen_pct DESC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/producto-menor-margen")
def producto_menor_margen():
    """Producto con menor margen de ganancia (requiere columna Costo en productos)"""
    query = """
    SELECT
        Descripcion                                        AS producto,
        Precio                                             AS precio_venta,
        Costo                                              AS costo,
        ROUND((Precio - Costo), 2)                         AS ganancia_unitaria,
        ROUND((Precio - Costo) / Precio * 100, 1)          AS margen_pct
    FROM productos
    ORDER BY margen_pct ASC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/producto-menos-stock")
def producto_menos_stock():
    """Producto con menos existencias en inventario"""
    query = """
    SELECT
        Descripcion  AS producto,
        Existencias  AS stock,
        Precio       AS precio_venta
    FROM productos
    ORDER BY Existencias ASC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/top-productos")
def top_productos():
    """Top 5 productos más vendidos por unidades"""
    query = """
    SELECT
        p.Descripcion   AS producto,
        SUM(pe.Cant)    AS total_unidades,
        SUM(pe.Importe) AS total_importe
    FROM pedidos pe
    JOIN productos p
      ON pe.Fab = p.Id_Fab AND pe.Producto = p.Id_Producto
    GROUP BY p.Descripcion
    ORDER BY total_unidades DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query)).mappings().all()
    return [dict(r) for r in results]


# ─────────────────────────────────────────
#  VENDEDORES
# ─────────────────────────────────────────

@app.get("/vendedor-top")
def vendedor_top():
    """Vendedor con mayor importe total de ventas"""
    query = """
    SELECT
        r.Nombre        AS vendedor,
        r.Cuota         AS cuota_asignada,
        SUM(pe.Importe) AS total_ventas,
        ROUND(SUM(pe.Importe) / r.Cuota * 100, 1) AS cumplimiento_pct
    FROM pedidos pe
    JOIN repventas r ON pe.Rep = r.Num_Empl
    GROUP BY r.Nombre, r.Cuota
    ORDER BY total_ventas DESC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/vendedores-sobre-cuota")
def vendedores_sobre_cuota():
    """Vendedores que superaron su cuota asignada"""
    query = """
    SELECT
        r.Nombre        AS vendedor,
        r.Cuota         AS cuota_asignada,
        SUM(pe.Importe) AS total_ventas,
        ROUND(SUM(pe.Importe) / r.Cuota * 100, 1) AS cumplimiento_pct
    FROM pedidos pe
    JOIN repventas r ON pe.Rep = r.Num_Empl
    GROUP BY r.Nombre, r.Cuota
    HAVING total_ventas >= r.Cuota
    ORDER BY cumplimiento_pct DESC;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query)).mappings().all()
    return [dict(r) for r in results]


# ─────────────────────────────────────────
#  CLIENTES
# ─────────────────────────────────────────

@app.get("/cliente-top")
def cliente_top():
    """Cliente que más compra en importe total"""
    query = """
    SELECT
        c.Empresa        AS cliente,
        COUNT(pe.Num_Pedido) AS num_pedidos,
        SUM(pe.Importe)  AS total_compras
    FROM pedidos pe
    JOIN clientes c ON pe.Clie = c.Num_Clie
    GROUP BY c.Empresa
    ORDER BY total_compras DESC
    LIMIT 1;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


@app.get("/top-clientes")
def top_clientes():
    """Top 5 clientes por importe total de compras"""
    query = """
    SELECT
        c.Empresa        AS cliente,
        COUNT(pe.Num_Pedido) AS num_pedidos,
        SUM(pe.Importe)  AS total_compras
    FROM pedidos pe
    JOIN clientes c ON pe.Clie = c.Num_Clie
    GROUP BY c.Empresa
    ORDER BY total_compras DESC
    LIMIT 5;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query)).mappings().all()
    return [dict(r) for r in results]


# ─────────────────────────────────────────
#  OFICINAS
# ─────────────────────────────────────────

@app.get("/ventas-por-oficina")
def ventas_por_oficina():
    """Ventas totales agrupadas por oficina (ciudad)"""
    query = """
    SELECT
        o.Ciudad        AS oficina,
        o.Region        AS region,
        COUNT(pe.Num_Pedido) AS num_pedidos,
        SUM(pe.Importe) AS total_ventas
    FROM pedidos pe
    JOIN repventas r ON pe.Rep = r.Num_Empl
    JOIN oficinas o ON r.Oficina_Rep = o.Oficina
    GROUP BY o.Ciudad, o.Region
    ORDER BY total_ventas DESC;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query)).mappings().all()
    return [dict(r) for r in results]


# ─────────────────────────────────────────
#  RESUMEN GENERAL
# ─────────────────────────────────────────

@app.get("/resumen")
def resumen():
    """KPIs generales de la empresa ACME"""
    query = """
    SELECT
        COUNT(DISTINCT pe.Num_Pedido)  AS total_pedidos,
        COUNT(DISTINCT pe.Clie)        AS total_clientes_activos,
        COUNT(DISTINCT pe.Rep)         AS total_vendedores_activos,
        SUM(pe.Importe)                AS ventas_totales,
        ROUND(AVG(pe.Importe), 2)      AS ticket_promedio
    FROM pedidos pe;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).mappings().first()
    return dict(result) if result else {}


# ─────────────────────────────────────────
#  DATOS COMPLETOS PARA EL AGENTE
#  (Este endpoint alimenta al agente de
#   Copilot Studio con toda la información
#   necesaria para responder cualquier
#   pregunta en lenguaje natural)
# ─────────────────────────────────────────

@app.get("/datos-completos")
def datos_completos():
    """
    Devuelve TODOS los datos enriquecidos de ACME en una sola llamada.
    El agente de Copilot Studio usa este endpoint para responder
    cualquier pregunta en lenguaje natural sin necesidad de SQL.
    """
    with engine.connect() as conn:

        # ── Pedidos enriquecidos con todas las dimensiones ──
        pedidos = conn.execute(text("""
            SELECT
                pe.Num_Pedido                                   AS num_pedido,
                DATE_FORMAT(pe.Fecha_Pedido, '%Y-%m-%d')        AS fecha_pedido,
                YEAR(pe.Fecha_Pedido)                           AS anio,
                MONTH(pe.Fecha_Pedido)                          AS mes,
                MONTHNAME(pe.Fecha_Pedido)                      AS nombre_mes,
                c.Empresa                                       AS cliente,
                r.Nombre                                        AS vendedor,
                r.Cuota                                         AS cuota_vendedor,
                o.Ciudad                                        AS oficina,
                o.Region                                        AS region,
                p.Descripcion                                   AS producto,
                p.Id_Fab                                        AS fabricante,
                pe.Cant                                         AS cantidad,
                pe.Importe                                      AS importe,
                p.Precio                                        AS precio_unitario,
                p.Costo                                         AS costo_unitario,
                ROUND((p.Precio - p.Costo) / p.Precio * 100,1) AS margen_pct,
                ROUND(pe.Cant * p.Costo, 2)                     AS costo_total,
                ROUND(pe.Importe - (pe.Cant * p.Costo), 2)      AS ganancia
            FROM pedidos pe
            JOIN clientes  c  ON pe.Clie    = c.Num_Clie
            JOIN repventas r  ON pe.Rep     = r.Num_Empl
            JOIN oficinas  o  ON r.Oficina_Rep = o.Oficina
            JOIN productos p  ON pe.Fab     = p.Id_Fab
                             AND pe.Producto = p.Id_Producto
            ORDER BY pe.Fecha_Pedido DESC
        """)).mappings().all()

        # ── Inventario actual ──
        inventario = conn.execute(text("""
            SELECT
                Descripcion  AS producto,
                Id_Fab       AS fabricante,
                Precio       AS precio,
                Costo        AS costo,
                Existencias  AS stock,
                ROUND((Precio - Costo) / Precio * 100, 1) AS margen_pct
            FROM productos
            ORDER BY Existencias ASC
        """)).mappings().all()

        # ── Desempeño de vendedores ──
        vendedores = conn.execute(text("""
            SELECT
                r.Nombre                                        AS vendedor,
                r.Cuota                                         AS cuota,
                o.Ciudad                                        AS oficina,
                SUM(pe.Importe)                                 AS total_ventas,
                COUNT(pe.Num_Pedido)                            AS num_pedidos,
                ROUND(SUM(pe.Importe) / r.Cuota * 100, 1)      AS cumplimiento_pct
            FROM repventas r
            LEFT JOIN pedidos  pe ON r.Num_Empl    = pe.Rep
            LEFT JOIN oficinas o  ON r.Oficina_Rep = o.Oficina
            GROUP BY r.Nombre, r.Cuota, o.Ciudad
            ORDER BY total_ventas DESC
        """)).mappings().all()

    return {
        "pedidos":     [dict(r) for r in pedidos],
        "inventario":  [dict(r) for r in inventario],
        "vendedores":  [dict(r) for r in vendedores]
    }
