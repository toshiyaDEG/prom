from pymongo import MongoClient

# Conectarse a la base de datos de MongoDB
client = MongoClient('mongodb+srv://anemone:3mbF4repnPWfbLof@meu.vvjxgc6.mongodb.net/')
db = client['testMeu']
collection = db['feedback_web']

#Definir la operación de agregación
pipeline = [

    #Añadir campos mes y año + convertirlos a string
    {'$addFields':{
                'month':{'$toString':{'$month':'$created_at'}},
                'year':{'$toString':{'$year':'$created_at'}}
                }
    },

    #Añadir campos mes y año + concatenarlos
    {'$addFields':{
        'date': {'$concat': ['$month','/','$year']},
        'thisYear': {'$eq':['$year','2024']}
        }
    },

    # Agrupando fecha concatenada, recuperando tipo de panel y obteniendo  promedio
    {'$group': {
        '_id': {'date': '$date',
                'panel':{
                    '$ifNull':
                        ['$panel', 'No existe']
                    }
                },
        'promedio_calificaciones': {'$avg': '$score'}
        }
     },

    {'$sort': {'_id': 1}} 
]

    
# Ejecutar la agregación y guardarla en rsult
result = collection.aggregate(pipeline)

# Guardar resultado en JSON
#with open('prom_calif.json', 'w') as f:
for doc in result:
    print(doc)
    #f.write(str(doc) + '\n')
    promedio_calificaciones = doc.get('promedio_calificaciones')
    date = doc['_id']['date']
    panel = doc['_id']['panel']

if panel == 'true':
    origen = 'panel de clientes'
elif panel == 'false':
   origen = 'neubox.com'
else:
    origen = 'No existe'

print(f"El promedio de calificaciones en {date} desde el {origen} es: {promedio_calificaciones}")