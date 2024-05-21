--Project 2 :Chen Ben-Ami 304939903

--Question 1
SELECT
	p.ProductID, p.Name, p.Color, p.ListPrice, p.Size
FROM
	Production.Product as p
LEFT JOIN
	Sales.SalesOrderDetail as o
	ON p.ProductID = o.ProductID
WHERE
	o.ProductID IS NULL
ORDER BY
	p.ProductID;

--Question 2
SELECT 
    c.CustomerID,
	ISNULL(p.LastName, 'Unknown') as LastName,
    ISNULL(p.FirstName, 'Unknown') as FirstName
    FROM 
    Sales.Customer as c
LEFT JOIN 
    Person.Person as p
	ON c.CustomerID = p.BusinessEntityID
LEFT JOIN 
    Sales.SalesOrderHeader as s
	ON c.CustomerID = s.CustomerID
WHERE 
    s.CustomerID IS NULL
ORDER BY 
    c.CustomerID ASC;

--Question 3
WITH CustomerOrders as
(
    SELECT c.CustomerID, p.FirstName, p.LastName, COUNT(SOH.SalesOrderID) as CountOfOrders
    FROM 
        Sales.Customer as c
    LEFT JOIN 
        Person.Person as p
		ON c.PersonID = p.BusinessEntityID
    LEFT JOIN 
        Sales.SalesOrderHeader as SOH
		ON c.CustomerID = SOH.CustomerID
    GROUP BY 
        c.CustomerID, p.FirstName, p.LastName
)
SELECT TOP 10
    CustomerID, FirstName, LastName, CountOfOrders
FROM 
    CustomerOrders
ORDER BY 
    CountOfOrders DESC;

--Question 4
SELECT p.FirstName, p.LastName, e.JobTitle, e.HireDate,
    (SELECT COUNT(*) 
     FROM HumanResources.Employee as e2 
     WHERE e2.JobTitle = e.JobTitle) as CountOfTitle
FROM 
    HumanResources.Employee as e
INNER JOIN 
    Person.Person as p
	ON e.BusinessEntityID = P.BusinessEntityID;

--Question 5
WITH OrderHistory as (
 SELECT SOH.SalesOrderID, c.CustomerID, p.LastName, p.FirstName, SOH.OrderDate as LastOrder, 
		DENSE_RANK() OVER(PARTITION BY C.CustomerID ORDER BY SOH.OrderDate DESC) as RN,
		LAG(SOH.OrderDate) OVER (PARTITION BY C.CustomerID ORDER BY SOH.OrderDate ASC) as PreviousOrder
 FROM Sales.SalesOrderHeader as SOH JOIN Sales.Customer as c ON SOH.CustomerID = c.CustomerID
									JOIN Person.Person as p ON c.PersonID = p.BusinessEntityID
)

 SELECT SalesOrderID, CustomerID, LastName, FirstName, LastOrder, PreviousOrder
 FROM OrderHistory
 WHERE RN = 1;

--Question 6
WITH RankedOrders as (
    SELECT 
        soh.SalesOrderID, YEAR(soh.OrderDate) as Year, p.LastName, p.FirstName,
        SUM(sod.UnitPrice * (1 - sod.UnitPriceDiscount) * sod.OrderQty) AS Total,
        ROW_NUMBER() OVER(PARTITION BY YEAR(soh.OrderDate) ORDER BY SUM(sod.UnitPrice * (1 - sod.UnitPriceDiscount) * sod.OrderQty) DESC) AS OrderRank
    FROM 
        Sales.SalesOrderHeader as soh
    JOIN 
        Sales.SalesOrderDetail as sod 
		ON soh.SalesOrderID = sod.SalesOrderID
    JOIN 
        Sales.Customer as c
		ON soh.CustomerID = c.CustomerID
    JOIN
        Person.Person as p
		ON c.PersonID = p.BusinessEntityID
    GROUP BY 
        soh.SalesOrderID, YEAR(soh.OrderDate), p.LastName, p.FirstName
)
SELECT ro.Year, ro.SalesOrderID, ro.LastName, ro.FirstName,  ro.Total
FROM 
    RankedOrders AS ro
WHERE 
    ro.OrderRank = 1;

--Question 7
	SELECT
	Month,
    ISNULL([2011], 0) as [2011],
    ISNULL([2012], 0) as [2012],
    ISNULL([2013], 0) as [2013],
    ISNULL([2014], 0) as [2014]
FROM 
    (
        SELECT 
            YEAR(OrderDate) as Year,
            MONTH(OrderDate) as Month,
            COUNT(*) as OrdersCount
        FROM 
            Sales.SalesOrderHeader
        GROUP BY 
            YEAR(OrderDate), MONTH(OrderDate)
    ) as SourceTable
PIVOT
(
    SUM(OrdersCount)
    FOR Year IN ([2011], [2012], [2013], [2014])
) as PivotTable
ORDER BY 
    Month;

--Question 8

	WITH TBL
AS
(
	SELECT YEAR(OrderDate) as Year,
    MONTH(OrderDate) as Month,
    SUM(sod.UnitPrice) as Sum_Price
	 FROM 
        Sales.SalesOrderHeader soh
    JOIN 
        Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    GROUP BY 
	YEAR(OrderDate), MONTH(OrderDate)
) ,
TBL2
AS
(

    SELECT *,
    SUM(Sum_price)OVER(PARTITION BY Year ORDER BY Month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumSum,
	ROW_NUMBER()OVER(PARTITION BY Year ORDER BY Month) AS RN
	FROM TBL
),
TBL3
AS
(
	SELECT Year, CAST(Month AS VARCHAR) AS Month, Sum_Price, CumSum, RN
	FROM TBL2
	UNION 
	SELECT YEAR(OrderDate) Year, 'Grand_Total', NULL, SUM(sod.UnitPrice) AS SUM_Price, 13
	 FROM 
        Sales.SalesOrderHeader as soh
    JOIN 
        Sales.SalesOrderDetail as sod
	ON soh.SalesOrderID = sod.SalesOrderID
	GROUP BY YEAR(OrderDate)
	UNION
	SELECT 3000, 'Grand_Total', NULL, SUM(sod.UnitPrice) AS Sum_Price, 100
	FROM  Sales.SalesOrderHeader as soh JOIN Sales.SalesOrderDetail as sod
	ON soh.SalesOrderID = sod.SalesOrderID
)
SELECT Year, Month, Sum_Price, CumSum
FROM TBL3
ORDER BY Year, RN;

--Question 9
WITH RankedEmployees AS (
    SELECT
        d.Name AS DepartmentName,
        edh.BusinessEntityID AS "Employee'sId",
        CONCAT(p.FirstName, ' ', p.LastName) AS "Employee'sFullName",
        edh.StartDate AS HireDate,
        RANK() OVER (PARTITION BY d.Name ORDER BY edh.StartDate ASC) AS Seniority,
        LEAD(edh.BusinessEntityID) OVER (PARTITION BY d.Name ORDER BY edh.StartDate ASC) AS PreviousEmployeeID,
        LEAD(CONCAT(p.FirstName, ' ', p.LastName)) OVER (PARTITION BY d.Name ORDER BY edh.StartDate DESC) AS PreviousEmpName,
        LEAD(edh.StartDate) OVER (PARTITION BY d.Name ORDER BY edh.StartDate DESC) AS PreviousEmpHDate,
        ABS(DATEDIFF(DAY, edh.StartDate, LEAD(edh.StartDate) OVER (PARTITION BY d.Name ORDER BY edh.StartDate DESC))) AS DiffDays
    FROM
        HumanResources.EmployeeDepartmentHistory AS edh 
    INNER JOIN
        HumanResources.Department AS d ON edh.DepartmentID = d.DepartmentID
    INNER JOIN
        Person.Person AS p ON edh.BusinessEntityID = p.BusinessEntityID
    WHERE
        edh.StartDate IS NOT NULL
)
SELECT
    DepartmentName,
    "Employee'sId",
    "Employee'sFullName",
    HireDate,
	Seniority,
    PreviousEmpName,
    PreviousEmpHDate,
    DiffDays
FROM
    RankedEmployees
ORDER BY
    DepartmentName;

--Question 10
SELECT 
    e.HireDate, 
    edh.DepartmentID,
    STRING_AGG(CONCAT(p.BusinessEntityID, ' ', p.LastName, ' ', p.firstName), ', ') AS TeamEmployees
FROM 
    HumanResources.EmployeeDepartmentHistory edh
JOIN 
    Person.Person p ON edh.BusinessEntityID = p.BusinessEntityID
JOIN
    HumanResources.Employee e ON edh.BusinessEntityID = e.BusinessEntityID
LEFT JOIN 
    (
        SELECT 
            DepartmentID,
            StartDate
        FROM 
            HumanResources.EmployeeDepartmentHistory
		
        GROUP BY 
            DepartmentID, StartDate
    ) dup ON edh.StartDate = dup.StartDate
WHERE edh.EndDate IS NULL
GROUP BY 
    e.HireDate, edh.DepartmentID
ORDER BY 
    e.HireDate DESC;