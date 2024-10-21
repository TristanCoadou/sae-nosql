# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn = sqlite3.connect('Z:/NoSQL/ClassicModel (2).sqlite')

Q1 = pandas.DataFrame(pandas.read_sql_query('Select C.customerName from Customers C LEFT OUTER JOIN Orders O Using(customerNumber) Where O.orderNumber is null ',conn))
print(Q1)


Q2 = pandas.DataFrame(pandas.read_sql_query('Select E.lastname, count(DISTINCT(C.customernumber)) as clients, count(DISTINCT(O.OrderNumber)) as commandes, Sum(D.priceEach * D.QuantityOrdered) as prix  from Employees E right Join Customers C right JOIN Orders O right JOIN OrderDetails D on E.EmployeeNumber = C.SalesRepEmployeeNumber and C.customerNumber = O.customerNumber and  O.ordernumber=D.ordernumber GROUP BY 1',conn))
print(Q2)

Q3 = pandas.DataFrame(pandas.read_sql_query('Select B.city, count(DISTINCT(C.customernumber)) as clients, count(DISTINCT(O.OrderNumber)) as commandes, Sum(D.priceEach * D.QuantityOrdered) as prix, COUNT(DISTINCT(C.country)) as pays from Offices B Right join Employees E right Join Customers C right JOIN Orders O right JOIN OrderDetails D on B.officecode=E.officecode and E.EmployeeNumber = C.SalesRepEmployeeNumber and C.customerNumber = O.customerNumber and  O.ordernumber=D.ordernumber GROUP BY 1',conn))
print(Q3)

Q4 = pandas.DataFrame(pandas.read_sql_query('Select P.ProductName, count(DISTINCT(D.ordernumber)) as commande, Sum(D.priceEach * D.QuantityOrdered) as prix,  Sum(D.quantityOrdered) as quantite , count(DISTINCT(O.customernumber)) as clients from Products P right join Orders O right JOIN OrderDetails D on P.productcode=D.productcode and D.ordernumber=O.ordernumber GROUP BY 1',conn))
print(Q4)

Q5 = pandas.DataFrame(pandas.read_sql_query('Select C.country, count(DISTINCT(O.OrderNumber)) as commandes, Sum(D.priceEach * D.QuantityOrdered) as prix, Sum(P.amount) as prix_payé from Payments P right join Customers C right left join Orders O right JOIN OrderDetails D on P.customerNumber = C.customerNumber and C.customerNumber = O.customerNumber and  O.ordernumber=D.ordernumber GROUP BY 1, 2',conn))
print(Q5)

Q6_7 = pandas.DataFrame(pandas.read_sql_query('Select P.Productline, C.country, count(DISTINCT(D.ordernumber)) as commande, Sum(D.priceEach * D.QuantityOrdered) as prix from Products P right JOIN OrderDetails D right join Orders O right join Customers C  on P.productcode=D.productcode and D.ordernumber=O.ordernumber and C.customerNumber = O.customerNumber GROUP BY 1,2',conn))
print(Q6_7)

Q8 =  pandas.DataFrame(pandas.read_sql_query('Select P.Productname, (D.priceeach - P.buyprice) as marge  from Products P right JOIN OrderDetails D on P.productcode=D.productcode  GROUP BY 1 Order by 2 desc limit 10',conn))
print(Q8)

Q9 =  pandas.DataFrame(pandas.read_sql_query('Select P.Productname, C.customernumber as IDclient,C.customername as clientNom, (D.priceeach - P.buyprice) as marge  from Products P right JOIN OrderDetails D right join Orders O right Join customers C on P.productcode=D.productcode and D.ordernumber = O.ordernumber and O.customernumber = C.customernumber where marge < 0  GROUP BY 1,2,3 Order by 2 ',conn))
print(Q9)

Q10 = pandas.DataFrame(pandas.read_sql_query('Select C.customername, sum(P.amount) as payé, Sum(D.priceEach * D.QuantityOrdered) as prix from Payments P right Join Customers C left JOIN Orders O right JOIN OrderDetails D on P.Customernumber=C.Customernumber and C.customerNumber = O.customerNumber and  O.ordernumber=D.ordernumber GROUP BY 1 HAVING SUM(P.amount) < SUM(D.priceEach * D.QuantityOrdered)',conn))
print(Q10)

Q10 = pandas.DataFrame(pandas.read_sql_query(
    '''Select C.customername,
        sum(P.amount) as payé, 
        Sum(D.priceEach * D.QuantityOrdered) as prix 
    from Payments P right Join Customers C left JOIN Orders O right JOIN OrderDetails D 
        on P.Customernumber=C.Customernumber and C.customerNumber = O.customerNumber and  O.ordernumber=D.ordernumber 
    GROUP BY 1 
    HAVING SUM(P.amount) <= SUM(D.priceEach * D.QuantityOrdered)
    '''
    ,conn))
print(Q10)

# Récupération du contenu de Customers avec une requête SQL
pandas.read_sql_query('SELECT * FROM Customers;', conn)



# Fermeture de la connexion : IMPORTANT à faire dans un cadre professionnel
conn.close()



