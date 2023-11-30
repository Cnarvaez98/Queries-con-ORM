from tortoise.models import Model
from tortoise import fields, Tortoise
from tortoise.expressions import Function
from pypika import CustomFunction
from datetime import datetime

# Definición de modelos
class Cliente(Model):
    id = fields.IntField(pk=True)
    nombre = fields.CharField(max_length=255)

class Producto(Model):
    id = fields.IntField(pk=True)
    nombre = fields.CharField(max_length=255)

class Venta(Model):
    id = fields.IntField(pk=True)
    cliente = fields.ForeignKeyField("models.Cliente", related_name="ventas")
    producto = fields.ForeignKeyField("models.Producto", related_name="ventas")
    monto = fields.DecimalField(max_digits=8, decimal_places=2)
    fecha_hora = fields.DatetimeField()

# Inicializar Tortoise
async def init():
    await Tortoise.init(
        db_url='sqlite://:memory:',
        modules={'models': ['nombre_de_tu_script']}  # Asegúrate de cambiar 'nombre_de_tu_script'
    )
    await Tortoise.generate_schemas()

# Consultas
async def consulta_1():
    resultados = await Cliente.annotate(
        n_compras=Function('COUNT', Venta.id)
    ).all().values_list('nombre', 'n_compras')
    return resultados

async def consulta_2():
    resultados = await Producto.annotate(
        monto_total=Function('SUM', Venta.monto)
    ).all().order_by('-monto_total').values_list('nombre', 'monto_total')
    return resultados

async def consulta_3():
    resultados = await Venta.annotate(
        fecha_trunc=TruncDate('fecha_hora')
    ).group_by('fecha_trunc').annotate(
        promedio_monto=Function('AVG', Venta.monto)
    ).order_by('fecha_trunc').values_list('fecha_trunc', 'promedio_monto')
    return resultados

# Implementación de la función custom para truncar datetime a solo date
class TruncDate(Function):
    database_func = CustomFunction("DATE", ["name"])

# Función principal
async def main():
    await init()
    resultados_1 = await consulta_1()
    resultados_2 = await consulta_2()
    resultados_3 = await consulta_3()

    # Imprimir resultados
    print("Número total de compras realizadas por cada cliente:")
    for resultado in resultados_1:
        print(resultado)

    print("\nMonto total de ventas por producto (ordenado de mayor a menor):")
    for resultado in resultados_2:
        print(resultado)

    print("\nPromedio de montos de ventas por día ordenado por fechas:")
    for resultado in resultados_3:
        print(resultado)

# Ejecutar la función principal
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

