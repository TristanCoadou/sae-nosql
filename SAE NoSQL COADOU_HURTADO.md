# **Analyse des Données avec SQLite et MongoDB**

## **Importation des bibliothèques**

```python
import sqlite3
import pandas as pd
import pymongo
```

## **Connexion à SQLite et lecture des données**
```python
# Connexion à la base SQLite
db_path = "ClassicModel.sqlite"  # Chemin vers votre base SQLite
conn = sqlite3.connect(db_path)

# Fonction pour exécuter une requête et la transformer en DataFrame
def fetch_table(query):
    return pd.read_sql_query(query, conn)

# Lecture des tables
Product = fetch_table("SELECT * FROM Products;")
customers = fetch_table("SELECT * FROM Customers;")
offices = fetch_table("SELECT * FROM Offices;")
payments = fetch_table("SELECT * FROM Payments;")
orders = fetch_table("SELECT * FROM Orders;")
employees = fetch_table("SELECT * FROM Employees;")
orderdetails = fetch_table("SELECT * FROM OrderDetails;")
```
## **Association des données**
```python
# Associer les détails des commandes
orders = orders.assign(
    OrderDetails=[
        list(orderdetails.query("orderNumber == @id").drop(columns=["orderNumber"]).to_dict(orient="records")) 
        if id in list(orderdetails.orderNumber) else None
        for id in orders.orderNumber
    ]
)

# Ajouter les commandes et paiements dans le document des clients
customers = customers.assign(
    orders=[
        list(orders.query("customerNumber == @id").to_dict(orient="records"))
        if id in list(orders.customerNumber) else None
        for id in customers.customerNumber
    ],
    Payments=[
        list(payments.query("customerNumber == @id").drop(columns=["customerNumber"]).to_dict(orient="records"))
        if id in list(payments.customerNumber) else None
        for id in customers.customerNumber
    ]
)

# Associer employés aux bureaux
offices_employees = offices.assign(
    Employees=[
        list(employees.query("officeCode == @code").to_dict(orient="records"))
        if code in list(employees.officeCode) else None
        for code in offices.officeCode
    ]
)

# Répertorier les produits (sans modification pour l'instant)
products_doc = Product
```
## **Conversion en JSON documents**

```python
customers_json = customers.to_dict(orient="records")
offices_employees_json = offices_employees.to_dict(orient="records")
products_json = products_doc.to_dict(orient="records")

# Fermer la connexion SQLite
conn.close()
```

## **Connexion à MongoDB et insertion des données**

```python
# Connexion MongoDB
client = pymongo.MongoClient(
     "mongodb+srv://Mango_user:8ce8toFJ5qcWDq6T@cluster-but-sd.swl74.mongodb.net/?retryWrites=true&w=majority&appName=cluster-but-sd"
)

# Accès à la base MongoDB
db = client.SAEF

# Insérer les données enrichies dans MongoDB
db.customers.insert_many(customers_json)
db.offices.insert_many(offices_employees_json)
db.products.insert_many(products_json)

# Vérification des données insérées
print("Clients insérés :", db.customers.count_documents({}))
print("Bureaux insérés :", db.offices.count_documents({}))
print("Produits insérés :", db.products.count_documents({}))

```
# **Analyse des Données avec MongoDB**

## QUESTION 1

```python
# Requête MongoDB
result = [
    { "$match": { "orders": None } },  # Condition : `orders` est null
    { "$project": {  # Projection pour inclure uniquement `customerNumber` et `customerName`
        "_id": 0,
        "customerNumber": 1,
        "customerName": 1
    }}
]

# Exécution de l'agrégation
result = list(customers_collection.aggregate(result))

# afficher les résultats
print(pd.DataFrame(result))
```

## QUESTION 2
```python
result = [
    # Extraire les employés de chaque bureau
    { "$unwind": "$Employees" },  # Parcourt chaque employé dans le tableau `Employees`

    # Associer les employés à leurs clients via `salesRepEmployeeNumber`
    {
        "$lookup": {
            "from": "customers",
            "localField": "Employees.employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "customers"
        }
    },

    {
        "$addFields": {
            "numberOfCustomers": { "$size": "$customers" },  # Nombre de clients associés
            "numberOfOrders": {
                "$sum": {
                    "$map": {
                        "input": "$customers",
                        "as": "customer",
                        "in": { "$size": { "$ifNull": ["$$customer.orders", []] } }
                    }
                }
            },
            "totalOrderAmount": {
                "$sum": {
                    "$map": {
                        "input": "$customers",
                        "as": "customer",
                        "in": {
                            "$reduce": {
                                "input": { "$ifNull": ["$$customer.Payments", []] },
                                "initialValue": 0,
                                "in": { "$add": ["$$value", { "$ifNull": ["$$this.amount", 0] }] }
                            }
                        }
                    }
                }
            }
        }
    },

    # Grouper par employé 
    {
        "$group": {
            "_id": {
                "employeeNumber": "$Employees.employeeNumber",
                "firstName": "$Employees.firstName",
                "lastName": "$Employees.lastName"
            },
            "numberOfCustomers": { "$max": "$numberOfCustomers" },
            "numberOfOrders": { "$max": "$numberOfOrders" },
            "totalOrderAmount": { "$max": "$totalOrderAmount" }
        }
    },

    # Projeter les informations finales
    {
        "$project": {
            "_id": 0,
            "employeeNumber": "$_id.employeeNumber",
            "firstName": "$_id.firstName",
            "lastName": "$_id.lastName",
            "numberOfCustomers": 1,
            "numberOfOrders": 1,
            "totalOrderAmount": 1
        }
    },

    # Trier par numéro d'employé
    { "$sort": { "employeeNumber": 1 } }
]

# Exécution de l'agrégation
result = list(employees_collection.aggregate(result))

# afficher les résultats
print(pd.DataFrame(result))

```

## QUESTION 3
```python
result = [
    # Extraire les employés de chaque bureau
    { "$unwind": "$Employees" },

    # Associer les employés aux clients
    {
        "$lookup": {
            "from": "customers",
            "localField": "Employees.employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "customers"
        }
    },

    # Filtrer uniquement les clients valides
    {
        "$addFields": {
            "customers": {
                "$filter": {
                    "input": "$customers",
                    "as": "customer",
                    "cond": { "$ne": ["$$customer", None] }  # Exclut les valeurs nulles
                }
            }
        }
    },


    {
        "$addFields": {
            "numberOfCustomers": { "$size": "$customers" },
            "numberOfOrders": {
                "$sum": {
                    "$map": {
                        "input": "$customers",
                        "as": "customer",
                        "in": { "$size": { "$ifNull": ["$$customer.orders", []] } }
                    }
                }
            },
            "totalOrderAmount": {
                "$sum": {
                    "$map": {
                        "input": "$customers",
                        "as": "customer",
                        "in": {
                            "$reduce": {
                                "input": { "$ifNull": ["$$customer.Payments", []] },
                                "initialValue": 0,
                                "in": {
                                    "$add": [
                                        "$$value",
                                        {
                                            "$cond": {
                                                "if": { "$isNumber": "$$this.amount" },
                                                "then": "$$this.amount",
                                                "else": 0
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            # Calcul des clients provenant d'autres pays
            "clientsFromOtherCountries": {
                "$size": {
                    "$filter": {
                        "input": "$customers",
                        "as": "customer",
                        "cond": { "$ne": ["$$customer.country", "$country"] }
                    }
                }
            }
        }
    },

    # Grouper au niveau du bureau
    {
        "$group": {
            "_id": {
                "officeCode": "$officeCode",
                "country": "$country",
                "city": "$city"
            },
            "numberOfCustomers": { "$sum": "$numberOfCustomers" },
            "numberOfOrders": { "$sum": "$numberOfOrders" },
            "totalOrderAmount": { "$sum": "$totalOrderAmount" },
            "clientsFromOtherCountries": { "$sum": "$clientsFromOtherCountries" }
        }
    },

    # Projeter les résultats finaux
    {
        "$project": {
            "_id": 0,
            "officeCode": "$_id.officeCode",
            "city": "$_id.city",
            "country": "$_id.country",
            "numberOfCustomers": 1,
            "numberOfOrders": 1,
            "totalOrderAmount": 1,
            "clientsFromOtherCountries": 1
        }
    },

        # Trier par numéro d'employé
    { "$sort": { "officeCode": 1 } }
]

# Exécution de l'agrégation
result = list(employees_collection.aggregate(result))

# afficher les résultats
print(pd.DataFrame(result))
```

## QUESTION 4
```python
result = [
    # Dérouler les commandes des clients
    { "$unwind": "$orders" },
    # Dérouler les détails des commandes
    { "$unwind": "$orders.OrderDetails" },

    # Grouper par `productCode`
    {
        "$group": {
            "_id": "$orders.OrderDetails.productCode",  # Grouper par produit
            "numberOfOrders": { "$sum": 1 },  # Nombre total de commandes
            "totalQuantityOrdered": { "$sum": "$orders.OrderDetails.quantityOrdered" },  # Quantité totale commandée
            "uniqueCustomers": { "$addToSet": "$customerNumber" }  # Ensemble unique des clients
        }
    },

    # Ajouter le nombre de clients distincts
    {
        "$addFields": {
            "numberOfDistinctCustomers": { "$size": "$uniqueCustomers" }
        }
    },

    # Associer le nom du produit depuis la collection `products`
    {
        "$lookup": {
            "from": "products",  # Collection des produits
            "localField": "_id",  # `productCode` (groupe précédent)
            "foreignField": "productCode",  # Champ correspondant dans `products`
            "as": "productInfo"  # Résultat du lookup
        }
    },

    # Dérouler les informations produits (car `lookup` produit un tableau)
    { "$unwind": "$productInfo" },

    # Ajouter `productName` dans les résultats
    {
        "$addFields": {
            "productName": "$productInfo.productName"
        }
    },

    # Projeter les résultats finaux
    {
        "$project": {
            "_id": 0,
            "productCode": "$_id",
            "productName": 1,
            "numberOfOrders": 1,
            "totalQuantityOrdered": 1,
            "numberOfDistinctCustomers": 1
        }
    },

    # Trier par `productCode`
    { "$sort": { "productCode": 1 } }
]

# Exécuter le resultat
result = list(customers_collection.aggregate(result))

# afficher les résultats 
print(pd.DataFrame(result))
```
## QUESTION 5
```python
result = [
    # Étape 1 : Ajouter un champ indiquant si un client n'a pas de commandes
    {
        "$addFields": {
            "hasOrders": {"$gt": [{"$size": {"$ifNull": ["$orders", []]}}, 0]}
        }
    },

    # Étape 2 : Déplier les commandes des clients
    {
        "$unwind": {
            "path": "$orders",
            "preserveNullAndEmptyArrays": True  # Inclure les clients sans commandes
        }
    },

    # Étape 3 : Déplier les détails des commandes (`OrderDetails`)
    {
        "$unwind": {
            "path": "$orders.OrderDetails",
            "preserveNullAndEmptyArrays": True  # Inclure les commandes sans détails
        }
    },

    # Étape 4 : Grouper les données pour chaque client 
    {
        "$group": {
            "_id": "$customerNumber",
            "country": {"$first": "$country"},
            "numberOfOrders": {"$addToSet": "$orders.orderNumber"},  # Compter les commandes uniques
            "totalOrderAmount": {
                "$sum": {
                    "$cond": [
                        {
                            "$and": [
                                {"$ifNull": ["$orders.OrderDetails.quantityOrdered", False]},
                                {"$ifNull": ["$orders.OrderDetails.priceEach", False]}
                            ]
                        },
                        {"$multiply": ["$orders.OrderDetails.quantityOrdered", "$orders.OrderDetails.priceEach"]},
                        0
                    ]
                }
            },
            "totalPaidAmount": {
                "$sum": {
                    "$reduce": {
                        "input": {"$ifNull": ["$Payments", []]},
                        "initialValue": 0,
                        "in": {"$add": ["$$value", {"$ifNull": ["$$this.amount", 0]}]}
                    }
                }
            }
        }
    },

    # Étape 5 : Calculer la taille des commandes uniques
    {
        "$addFields": {
            "numberOfOrders": {"$size": {"$ifNull": ["$numberOfOrders", []]}}
        }
    },

    # Étape 6 : Grouper par pays pour calculer les totaux
    {
        "$group": {
            "_id": {"$ifNull": ["$country", "Unknown"]},  # Gérer les pays manquants
            "numberOfOrders": {"$sum": "$numberOfOrders"},
            "totalOrderAmount": {"$sum": "$totalOrderAmount"},
            "totalPaidAmount": {"$sum": "$totalPaidAmount"}
        }
    },

    # Étape 7 : Projeter les résultats finaux
    {
        "$project": {
            "_id": 0,
            "country": "$_id",
            "numberOfOrders": 1,
            "totalOrderAmount": 1,
            "totalPaidAmount": 1
        }
    },

    # Étape 8 : Trier par pays
    {"$sort": {"country": 1}}
]



# Exécuter le resultat
result = list(customers_collection.aggregate(result))

# afficher les résultats 
print(pd.DataFrame(result))

```

## QUESTION 6
```python
result = [
    # Étape 1 : Déplier les commandes des clients
    {
        "$unwind": {
            "path": "$orders",
            "preserveNullAndEmptyArrays": True
        }
    },

    # Étape 2 : Déplier les détails des commandes (`OrderDetails`)
    {
        "$unwind": {
            "path": "$orders.OrderDetails",
            "preserveNullAndEmptyArrays": True
        }
    },

    # Étape 3 : Joindre avec les produits
    {
        "$lookup": {
            "from": "products",
            "localField": "orders.OrderDetails.productCode",
            "foreignField": "productCode",
            "as": "productInfo"
        }
    },

    # Étape 4 : Ajouter `productLine`
    {
        "$addFields": {
            "productLine": {
                "$ifNull": [{ "$arrayElemAt": ["$productInfo.productLine", 0] }, "None"]
            }
        }
    },

    # Étape 5 : Grouper par pays, ligne de produit et commande unique
    {
        "$group": {
            "_id": {
                "country": "$country",
                "productLine": "$productLine",
                "orderNumber": "$orders.orderNumber"
            }
        }
    },

    # Étape 6 : Re-grouper pour obtenir le nombre d'ordres
    {
        "$group": {
            "_id": {
                "country": "$_id.country",
                "productLine": "$_id.productLine"
            },
            "numberOfOrders": { "$sum": 1 }
        }
    },

    # Étape 7 : Projeter les champs finaux
    {
        "$project": {
            "_id": 0,
            "country": {
                "$ifNull": ["$_id.country", "None"]
            },
            "productLine": {
                "$ifNull": ["$_id.productLine", "None"]
            },
            "numberOfOrders": 1
        }
    },

    # Étape 8 : Trier par pays et ligne de produit
    {
        "$sort": {
            "country": 1,
            "productLine": 1
        }
    }
]


# Exécution du résultat
result = list(customers_collection.aggregate(result))

# Afficher les résultats 
print(pd.DataFrame(result))

```
## QUESTION 7
```python
result = [
    # Étape 1 : Déplier les commandes des clients
    {
        "$unwind": {
            "path": "$orders",
            "preserveNullAndEmptyArrays": True  # Inclut les clients sans commandes
        }
    },

    # Étape 2 : Déplier les détails des commandes (`OrderDetails`)
    {
        "$unwind": {
            "path": "$orders.OrderDetails",
            "preserveNullAndEmptyArrays": True  # Inclut les commandes sans détails
        }
    },

    # Étape 3 : Joindre avec les produits pour récupérer `productLine`
    {
        "$lookup": {
            "from": "products",
            "localField": "orders.OrderDetails.productCode",
            "foreignField": "productCode",
            "as": "productInfo"
        }
    },

    # Étape 4 : Ajouter `productLine`
    {
        "$addFields": {
            "productLine": {
                "$ifNull": [{ "$arrayElemAt": ["$productInfo.productLine", 0] }, "None"]
            }
        }
    },

    # Étape 5 : Calculer le montant total payé via les paiements
    {
        "$addFields": {
            "totalPaidAmount": {
                "$sum": {
                    "$map": {
                        "input": { "$ifNull": ["$Payments", []] },  # Liste des paiements
                        "as": "payment",
                        "in": { "$ifNull": ["$$payment.amount", 0] }  # Ajouter les montants des paiements
                    }
                }
            }
        }
    },

    # Étape 6 : Grouper par `country` et `productLine`
    {
        "$group": {
            "_id": {
                "country": "$country",
                "productLine": "$productLine"
            },
            "totalPaidAmount": { "$sum": "$totalPaidAmount" }
        }
    },

    # Étape 7 : Projeter les champs finaux
    {
        "$project": {
            "_id": 0,
            "country": {
                "$ifNull": ["$_id.country", "None"]
            },
            "productLine": {
                "$ifNull": ["$_id.productLine", "None"]
            },
            "totalPaidAmount": 1
        }
    },

    # Étape 8 : Trier par `country` et `productLine`
    {
        "$sort": {
            "country": 1,
            "productLine": 1
        }
    }
]


# Exécution du résultat
result = list(customers_collection.aggregate(result))

# Afficher les résultats 
print(pd.DataFrame(result))

```
## QUESTION 8
```python
result = [
    # Étape 1 : Déplier les commandes des clients
    {
        "$unwind": {
            "path": "$orders",
            "preserveNullAndEmptyArrays": True
        }
    },

    # Étape 2 : Déplier les détails des commandes (`OrderDetails`)
    {
        "$unwind": {
            "path": "$orders.OrderDetails",
            "preserveNullAndEmptyArrays": True
        }
    },

    # Étape 3 : Associer les produits pour obtenir `buyPrice`
    {
        "$lookup": {
            "from": "products",
            "localField": "orders.OrderDetails.productCode",
            "foreignField": "productCode",
            "as": "productInfo"
        }
    },

    # Étape 4 : Déplier le produit associé
    {
        "$unwind": {
            "path": "$productInfo",
            "preserveNullAndEmptyArrays": True
        }
    },

    # Étape 5 : Ajouter un champ pour calculer la marge, en gérant les cas de valeurs manquantes
    {
        "$addFields": {
            "margin": {
                "$cond": {
                    "if": {
                        "$and": [
                            { "$ne": ["$orders.OrderDetails.priceEach", None] },
                            { "$ne": ["$productInfo.buyPrice", None] }
                        ]
                    },
                    "then": {
                        "$subtract": ["$orders.OrderDetails.priceEach", "$productInfo.buyPrice"]
                    },
                    "else": None
                }
            }
        }
    },

    # Étape 6 : Grouper par produit pour calculer la marge moyenne
    {
        "$group": {
            "_id": {
                "productCode": "$orders.OrderDetails.productCode",
                "productName": "$productInfo.productName"
            },
            "averageMargin": { "$avg": "$margin" }
        }
    },

    # Étape 7 : Projeter uniquement les champs demandés
    {
        "$project": {
            "_id": 0,
            "productCode": "$_id.productCode",
            "productName": "$_id.productName",
            "averageMargin": 1
        }
    },

    # Étape 8 : Trier par marge moyenne décroissante
    { "$sort": { "averageMargin": -1 } },

    # Étape 9 : Limiter aux 10 premiers produits
    { "$limit": 10 }
]

# Exécution du résultat
result = list(customers_collection.aggregate(result))

# Charger les résultats
print(pd.DataFrame(result))
```

## QUESTION 9 
```python
result = [
    # Étape 1 : Déplier les commandes des clients
    {
        "$unwind": {
            "path": "$orders",
            "preserveNullAndEmptyArrays": False  # Inclut uniquement les clients avec commandes
        }
    },

    # Étape 2 : Déplier les détails des commandes (`OrderDetails`)
    {
        "$unwind": {
            "path": "$orders.OrderDetails",
            "preserveNullAndEmptyArrays": False  # Inclut uniquement les commandes avec détails
        }
    },

    # Étape 3 : Associer les produits pour obtenir `buyPrice`
    {
        "$lookup": {
            "from": "products",
            "localField": "orders.OrderDetails.productCode",
            "foreignField": "productCode",
            "as": "productInfo"
        }
    },

    # Étape 4 : Déplier le produit associé
    {
        "$unwind": {
            "path": "$productInfo",
            "preserveNullAndEmptyArrays": False  # Garde uniquement les détails avec produits associés
        }
    },

    # Étape 5 : Filtrer les ventes à perte (prix de vente < prix d'achat)
    {
        "$match": {
            "$expr": {
                "$lt": ["$orders.OrderDetails.priceEach", "$productInfo.buyPrice"]
            }
        }
    },

    # Étape 6 : Projeter les champs nécessaires
    {
        "$project": {
            "_id": 0,
            "productCode": "$orders.OrderDetails.productCode",
            "productName": "$productInfo.productName",
            "customerName": "$customerName",
            "customerNumber": "$customerNumber",
            "priceEach": "$orders.OrderDetails.priceEach",
            "buyPrice": "$productInfo.buyPrice"
        }
    },

    # Étape 7 : Trier par client et produit
    {
        "$sort": {
            "customerName": 1,
            "productName": 1
        }
    }
]

# Exécution du pipeline
result = list(customers_collection.aggregate(result))

#Afficher les résultats
print(pd.DataFrame(result))
```
## Question Bonus

```python
result = [
    # Étape 1 : Calculer le montant total payé (Payments)
    {
        "$addFields": {
            "totalPaidAmount": {
                "$sum": {
                    "$map": {
                        "input": { "$ifNull": ["$Payments", []] },
                        "as": "payment",
                        "in": { "$ifNull": ["$$payment.amount", 0] }
                    }
                }
            }
        }
    },

    # Étape 2 : Calculer le montant total des achats (orders.OrderDetails)
    {
        "$addFields": {
            "totalOrderAmount": {
                "$sum": {
                    "$map": {
                        "input": { "$ifNull": ["$orders", []] },
                        "as": "order",
                        "in": {
                            "$sum": {
                                "$map": {
                                    "input": { "$ifNull": ["$$order.OrderDetails", []] },
                                    "as": "detail",
                                    "in": {
                                        "$multiply": [
                                            { "$ifNull": ["$$detail.quantityOrdered", 0] },
                                            { "$ifNull": ["$$detail.priceEach", 0] }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    },

    # Étape 3 : Grouper pour chaque client pour éviter les doublons
    {
        "$group": {
            "_id": "$customerNumber",
            "customerName": { "$first": "$customerName" },
            "totalPaidAmount": { "$first": "$totalPaidAmount" },
            "totalOrderAmount": { "$first": "$totalOrderAmount" }
        }
    },

    # Étape 4 : Filtrer les clients où le montant payé est STRICTEMENT supérieur au montant des achats
    {
        "$match": {
            "$expr": { "$gt": ["$totalPaidAmount", "$totalOrderAmount"] }
        }
    },

    # Étape 5 : Projeter uniquement les champs nécessaires
    {
        "$project": {
            "_id": 0,
            "customerNumber": "$_id",
            "customerName": 1,
            "totalPaidAmount": 1,
            "totalOrderAmount": 1
        }
    }
]

# Exécution du résultat
result = list(customers_collection.aggregate(result))

# Affichage des résultats
print(pd.DataFrame(result))

```