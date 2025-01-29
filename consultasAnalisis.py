import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Conectar a la base de datos
db_path = 'climate_data_warehouse2.db'
conn = sqlite3.connect(db_path)

# Consultas SQL
def execute_query(query, connection):
    return pd.read_sql_query(query, connection)

# Consulta 1: Temperatura global promedio por año
query1 = """
SELECT y.year, AVG(c.global_avg_temp) AS avg_temperature
FROM climate_indicators_fact c
JOIN year_dim y ON c.year_id = y.id
GROUP BY y.year
ORDER BY y.year;
"""
temperature_data = execute_query(query1, conn)

# Graficar resultados de la Consulta 1
plt.figure(figsize=(10, 6))
plt.plot(temperature_data['year'], temperature_data['avg_temperature'], marker='o', label='Global Avg Temp (°C)')
plt.title('Promedio de Temperatura Global por Año')
plt.xlabel('Año')
plt.ylabel('Temperatura (°C)')
plt.grid()
plt.legend()
plt.show()

# Consulta 2 mejorada: Pérdidas económicas totales por tipo de evento extremo
query2 = """
SELECT 
    e.event_type AS "Tipo de Evento Extremo", 
    SUM(f.economic_loss) AS "Pérdidas Económicas Totales (Billones)"
FROM 
    extreme_events_fact f
JOIN 
    extreme_event_type_dim e ON f.event_type_id = e.id
GROUP BY 
    e.event_type
ORDER BY 
    "Pérdidas Económicas Totales (Billones)" DESC;
"""
events_loss_data = execute_query(query2, conn)

# Graficar resultados de la Consulta 2 mejorada
plt.figure(figsize=(12, 6))
plt.barh(events_loss_data['Tipo de Evento Extremo'], events_loss_data['Pérdidas Económicas Totales (Billones)'], color='skyblue')
plt.title('Pérdidas Económicas Totales por Tipo de Evento Extremo')
plt.xlabel('Pérdidas Económicas Totales (Billones)')
plt.ylabel('Tipo de Evento Extremo')
plt.grid(axis='x', linestyle='--', alpha=0.7)
plt.gca().invert_yaxis()  # Invertir el eje para que el mayor esté arriba
plt.show()

# Consulta 3 mejorada: Aumento promedio del nivel del mar por ecosistema
query3 = """
SELECT 
    eco.ecosystem AS "Ecosistema",
    AVG(f.sea_level_rise) AS "Aumento Promedio del Nivel del Mar (mm)"
FROM 
    climate_indicators_fact f
JOIN 
    ecosystem_dim eco ON f.year_id = eco.id
GROUP BY 
    eco.ecosystem
ORDER BY 
    "Aumento Promedio del Nivel del Mar (mm)" DESC;
"""
sea_level_data = execute_query(query3, conn)

# Graficar resultados de la Consulta 3 mejorada
plt.figure(figsize=(10, 6))
plt.bar(sea_level_data['Ecosistema'], sea_level_data['Aumento Promedio del Nivel del Mar (mm)'], color='coral')
plt.title('Aumento Promedio del Nivel del Mar por Ecosistema')
plt.xlabel('Ecosistema')
plt.ylabel('Aumento Promedio (mm)')
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# Consulta 4: Pérdidas económicas acumuladas por región
query4 = """
SELECT 
    r.region AS "Región", 
    SUM(f.economic_loss) AS "Pérdidas Económicas Totales (Billones)"
FROM 
    extreme_events_fact f
JOIN 
    region_dim r ON f.region_id = r.id
GROUP BY 
    r.region
ORDER BY 
    "Pérdidas Económicas Totales (Billones)" DESC;
"""
region_loss_data = execute_query(query4, conn)

# Graficar resultados de la Consulta 4
plt.figure(figsize=(10, 6))
plt.pie(region_loss_data['Pérdidas Económicas Totales (Billones)'], labels=region_loss_data['Región'], autopct='%1.1f%%', startangle=140, colors=plt.cm.Paired.colors)
plt.title('Distribución de Pérdidas Económicas por Región')
plt.axis('equal')
plt.show()

# Consulta 5: Relación entre CO2 y aumento del nivel del mar
# Consulta 5 mejorada: Relación entre CO2 y aumento del nivel del mar a lo largo del tiempo
query5_improved = """
SELECT 
    y.year AS "Año", 
    AVG(f.co2_concentration) AS "Concentración Promedio de CO2 (ppm)", 
    AVG(f.sea_level_rise) AS "Aumento Promedio del Nivel del Mar (mm)"
FROM 
    climate_indicators_fact f
JOIN 
    year_dim y ON f.year_id = y.id
GROUP BY 
    y.year
ORDER BY 
    y.year;
"""
co2_sea_trend_data = execute_query(query5_improved, conn)

# Graficar resultados de la Consulta 5 mejorada como dispersión
plt.figure(figsize=(10, 6))
plt.scatter(
    co2_sea_trend_data['Concentración Promedio de CO2 (ppm)'],
    co2_sea_trend_data['Aumento Promedio del Nivel del Mar (mm)'],
    color='teal', alpha=0.7, edgecolor='black'
)
plt.title('Relación entre CO2 y Aumento del Nivel del Mar')
plt.xlabel('Concentración Promedio de CO2 (ppm)')
plt.ylabel('Aumento Promedio del Nivel del Mar (mm)')
plt.grid(alpha=0.5, linestyle='--')
plt.show()

# Ejecutar consulta SQL y almacenar resultados en un DataFrame
query6 = """
SELECT 
    y.year AS "Año", 
    e.event_type AS "Tipo de Evento Extremo", 
    COUNT(f.id) AS "Frecuencia de Eventos"
FROM 
    extreme_events_fact f
JOIN 
    extreme_event_type_dim e ON f.event_type_id = e.id
JOIN 
    year_dim y ON f.year_id = y.id
WHERE f.source_id = 1
GROUP BY 
    y.year, e.event_type
ORDER BY 
    y.year, "Frecuencia de Eventos" DESC;
"""
event_frequency_data = execute_query(query6, conn)

# Asegurar que los datos están ordenados
event_frequency_data = event_frequency_data.sort_values(by=["Año", "Tipo de Evento Extremo"])

plt.figure(figsize=(12, 6))

# Graficar cada tipo de evento por separado
for event in event_frequency_data["Tipo de Evento Extremo"].unique():
    data = event_frequency_data[event_frequency_data["Tipo de Evento Extremo"] == event]
    plt.plot(data["Año"], data["Frecuencia de Eventos"], marker='o', linestyle='-', label=event)

plt.title('Frecuencia de Eventos Extremos por Tipo y Año')
plt.xlabel('Año')
plt.ylabel('Frecuencia de Eventos')
plt.legend(title="Tipo de Evento Extremo", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid()
plt.tight_layout()
plt.show()

