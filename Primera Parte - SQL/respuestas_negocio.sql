/****************************************/
/************PUNTO 1*********************/
/****************************************/

SELECT C.ID,C.DISPLAY_NAME,SUM (TOTAL_COST) AS SUMTOTAL
FROM MELI.ORDERS O
JOIN MELI.CUSTOMERS C ON C.ID=O.CUSTOMER_ID
WHERE C.SELLER=1  AND C.BIRTH_DATE=TO_DATE(CURRENT_DATE) AND O.DATE_ORDER  BETWEEN ('01/01/2020') AND ('31/01/2020') AND O.STATUS!='CANC'
GROUP BY C.ID,C.DISPLAY_NAME
HAVING  SUM (TOTAL_COST) >1500

/****************************************/
/************PUNTO 2*********************/
/****************************************/

SELECT * FROM (
SELECT DATEDISPLAY,DISPLAY_NAME,NUMSALES,QUANSALES, SUMSALES, ROW_NUMBER() OVER (PARTITION BY DATEDISPLAY  ORDER BY DATEDISPLAY,SUMSALES DESC) AS OR_SEL
FROM(
SELECT TO_CHAR(DATE_ORDER,'MM-YYYY') AS DATEDISPLAY,C.DISPLAY_NAME,COUNT(1) AS NUMSALES, SUM(QUANTITY) QUANSALES,SUM(TOTAL_COST) AS SUMSALES
FROM MELI.ORDERS O
JOIN MELI.CUSTOMERS C ON C.ID=O.CUSTOMER_ID
JOIN MELI.ITEMS I ON I.ID=O.ITEM_ID
WHERE C.SELLER=1  AND O.DATE_ORDER  BETWEEN ('01/01/2020') AND ('31/12/2020') AND I.CATEGORY_CODE='CELL' AND O.STATUS!='CANC'
GROUP BY TO_CHAR(DATE_ORDER,'MM-YYYY'),C.DISPLAY_NAME
ORDER BY DATEDISPLAY,SUMSALES DESC))
WHERE OR_SEL<=5


/****************************************/
/************PUNTO 3*********************/
/****************************************/


CREATE OR REPLACE PROCEDURE InsertarItemConsolidation AS

BEGIN

DELETE FROM MELI.ITEM_CONSOLIDATION WHERE ITEM_DATE=TO_DATE(CURRENT_DATE);

INSERT INTO MELI.ITEM_CONSOLIDATION (ITEM_PRICE, ITEM_STATUS,ITEM_DATE)
SELECT PRICE, STATUS,CURRENT_DATE FROM MELI.ITEMS;


END;
