CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT SUM(A) FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT AVG(A) FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT MIN(A) FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT MAX(A) FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT COUNT(*) FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/r.dat' USING CSV;

SELECT COUNT(*) FROM R, S;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT COUNT(*) FROM R, S WHERE R.B = S.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A) FROM R, S WHERE R.B = S.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A), AVG(C) FROM R, S WHERE R.B = S.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT SUM(A*C) FROM R, S WHERE R.B = S.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T (C int, D int) FROM FILE 'test/t.dat' USING CSV;

SELECT SUM(A*D) FROM R, S, T WHERE (R.B = S.B) AND (S.C = T.C);SELECT 1;
SELECT 1.2;
SELECT 1.e4;
SELECT 1.2e4;
SELECT 1.e-4;
SELECT 1.2e-4;
SELECT -1;
SELECT -1.2 AS bob;
SELECT -1.e4;
SELECT -1.2e4;
SELECT -1.e-4;
SELECT -1.2e-4 AS joe;
SELECT 1+1;
SELECT (1+1)*2;
SELECT 1*2;
SELECT TRUE;
SELECT true;
SELECT FALSE;
SELECT NOT TRUE;
SELECT TRUE AND FALSE;
SELECT TRUE OR FALSE;
SELECT 'foobar';'
SELECT 'foobar\\';
SELECT 'foobar\'';
SELECT 2.0*2;
CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, SUM(B) FROM R GROUP BY A;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, AVG(B) FROM R GROUP BY A;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, MIN(B) FROM R GROUP BY A;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, MAX(B) FROM R GROUP BY A;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, COUNT(*) FROM R GROUP BY A;
SELECT A, B, COUNT(*) FROM R GROUP BY A,B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, COUNT(*) FROM R, S GROUP BY A;
SELECT A, R.B, COUNT(*) FROM R, S GROUP BY A, R.B;
SELECT A, R.B, C, COUNT(*) FROM R, S GROUP BY A, R.B, C;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT A, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A;
SELECT A, R.B, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B;
SELECT A, R.B, C, COUNT(*) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;


SELECT A, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A;
SELECT A, R.B, SUM(C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B;
SELECT R.B, SUM(A * C) FROM R, S WHERE R.B = S.B GROUP BY A, R.B, C;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT R.B, SUM(A), AVG(C) FROM R, S WHERE R.B = S.B GROUP BY R.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT A, SUM(A+B), AVG(C+B) FROM R, S WHERE R.B = S.B GROUP BY A;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT R.B, SUM(A*C) FROM R, S WHERE R.B = S.B GROUP BY R.B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T (C int, D int) FROM FILE 'test/t.dat' USING CSV;

SELECT A, SUM(D) FROM R, S, T WHERE (R.B = S.B) AND (S.C = T.C) GROUP BY A;1,3
4,2
2,1
5,3
3,4
2,3
5,5
4,5
2,3
4,24,3
3,2
4,1
5,2
2,3
4,5
2,4
3,1
1,5
4,5CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A FROM R;CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R, S;
CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R JOIN S ON R.B = S.B;
CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R, S;
SELECT R.A, R.B, S.B, S.C FROM R, S;

SELECT R.A, S.C FROM R JOIN S;
SELECT R.A, R.B, S.B, S.C FROM R JOIN S;

SELECT R.A, S.C FROM R JOIN S ON R.B = S.B;
SELECT R.A, R.B, S.B, S.C FROM R JOIN S ON R.B = S.B;

SELECT R.A, S.C FROM R, S WHERE R.B = S.B;
SELECT R.A, R.B, S.B, S.C FROM R, S WHERE R.B = S.B;CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R, S WHERE R.B = S.B;CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;

SELECT R.A, S.C FROM R, S WHERE (R.B = S.B) AND ((R.A > S.C) or (R.A < S.C));CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T ( C int, D int ) FROM FILE 'test/t.dat' USING CSV;

SELECT R.A, T.D FROM R, S, T WHERE (R.B = S.B) AND (S.C = T.C);CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T ( C int, D int ) FROM FILE 'test/t.dat' USING CSV;

SELECT R.A, T.D FROM R, S, T WHERE (R.B = S.B) AND (S.C < T.C);CREATE TABLE R ( A int, B int ) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S ( B int, C int ) FROM FILE 'test/s.dat' USING CSV;
CREATE TABLE T ( C int, D int ) FROM FILE 'test/t.dat' USING CSV;

SELECT R.A, T.D FROM R, S, T WHERE (R.B = S.B) AND (T.C < S.C);5,4
4,3
5,3
4,4
4,4
1,3
3,1
2,2
1,4
3,2CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

SELECT    c.custkey, c.name, 
          c.acctbal, n.name, c.address, c.phone, c.comment,
          sum(l.extendedprice * (1 - l.discount)) as revenue 
FROM      CUSTOMER c, ORDERS o, LINEITEM l, NATION n
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate >= 19931001)
      AND (o.orderdate < 19940101)
      AND (l.returnflag = 'R')
      AND (c.nationkey = n.nationkey)
GROUP BY  c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment
ORDER BY revenue DESC
LIMIT 20;
CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

SELECT    c.custkey, c.name, 
          c.acctbal, n.name, c.address, c.phone, c.comment,
          sum(l.extendedprice * (1 - l.discount)) as revenue 
FROM      CUSTOMER c, ORDERS o, LINEITEM l, NATION n
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate >= 19931001)
      AND (o.orderdate < 19940101)
      AND (l.returnflag = 'R')
      AND (c.nationkey = n.nationkey)
GROUP BY  c.custkey, c.name, c.acctbal, c.phone, n.name, c.address, c.comment;
CREATE TABLE PART (
  partkey int,
  name string,
  mfgr string,
  brand string,
  type string,
  size int,
  container string,
  retailprice float,
  comment string
) FROM FILE 'part.tbl' USING TPCH('part');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT    SUM(l.extendedprice * (1 - l.discount) ) AS revenue
FROM      LINEITEM l, PART p 
WHERE  (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#12')
        AND (    (p.container = 'SM CASE')
              OR (p.container = 'SM BOX')
              OR (p.container = 'SM PACK')
              OR (p.container = 'SM PKG')
            )
        AND (l.quantity >= 1)
        AND (l.quantity <= (1 + 10))
        AND (p.size >= 1)
        AND (p.size <= 5)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      ) OR (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#23')
        AND (    (p.container = 'MED BAG')
              OR (p.container = 'MED BOX')
              OR (p.container = 'MED PKG')
              OR (p.container = 'MED PACK')
            )
        AND (l.quantity >= 10)
        AND (l.quantity <= (10 + 10))
        AND (p.size >= 1)
        AND (p.size <= 10)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      ) OR (
            (p.partkey = l.partkey)
        AND (p.brand = 'Brand#34')
        AND (    (p.container = 'LG CASE')
              OR (p.container = 'LG BOX')
              OR (p.container = 'LG PACK')
              OR (p.container = 'LG PKG')
            )
        AND (l.quantity >= 20)
        AND (l.quantity <= (20 + 10))
        AND (p.size >= 1)
        AND (p.size <= 15)
        AND (    (l.shipmode = 'AIR')
              OR (l.shipmode = 'AIR REG')
            )
        AND (l.shipinstruct = 'DELIVER IN PERSON')
      );
CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT   l.returnflag, l.linestatus,
         SUM(l.quantity) AS sum_qty,
         SUM(l.extendedprice) AS sum_base_price, 
         SUM(l.extendedprice*(1-l.discount)) AS sum_disc_price, 
         SUM(l.extendedprice*(1-l.discount)*(1+l.tax)) AS sum_charge, 
         AVG(l.quantity) AS avg_qty,
         AVG(l.extendedprice) AS avg_price,
         AVG(l.discount) AS avg_disc,
         COUNT(*) AS count_order
FROM     LINEITEM l
WHERE    (l.shipdate <= 19981101)
GROUP BY l.returnflag, l.linestatus
ORDER BY l.returnflag, l.linestatus;
CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT   l.returnflag, l.linestatus,
         SUM(l.quantity) AS sum_qty,
         SUM(l.extendedprice) AS sum_base_price, 
         SUM(l.extendedprice*(1-l.discount)) AS sum_disc_price, 
         SUM(l.extendedprice*(1-l.discount)*(1+l.tax)) AS sum_charge, 
         AVG(l.quantity) AS avg_qty,
         AVG(l.extendedprice) AS avg_price,
         AVG(l.discount) AS avg_disc,
         COUNT(*) AS count_order
FROM     LINEITEM l
WHERE    (l.shipdate <= 19981101)
GROUP BY l.returnflag, l.linestatus;

CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT    l.orderkey,
          o.orderdate,
          o.shippriority,
          SUM(l.extendedprice*(1-l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l
WHERE     (c.mktsegment = 'BUILDING' )
      AND (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate < 19950315)
      AND (l.shipdate > 19950315)
GROUP BY  l.orderkey, o.orderdate, o.shippriority
ORDER BY revenue DESC,
         o.orderdate
LIMIT 10;


CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT    l.orderkey,
          o.orderdate,
          o.shippriority,
          SUM(l.extendedprice*(1-l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l
WHERE     (c.mktsegment = 'BUILDING' )
      AND (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (o.orderdate < 19950315)
      AND (l.shipdate > 19950315)
GROUP BY  l.orderkey, o.orderdate, o.shippriority;

CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

CREATE TABLE SUPPLIER (
  suppkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  comment string
) FROM FILE 'supplier.tbl' USING TPCH('supplier');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

CREATE TABLE REGION (
  regionkey int,
  name string,
  comment string
) FROM FILE 'region.tbl' USING TPCH('region');

SELECT    n.name,
          SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l, SUPPLIER s, NATION n, REGION r
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (l.suppkey = s.suppkey)
      AND (c.nationkey = s.nationkey)
      AND (s.nationkey = n.nationkey)
      AND (n.regionkey = r.regionkey)
      AND (r.name = 'ASIA')
      AND (o.orderdate >= 19940101)
      AND (o.orderdate < 19950101)
GROUP BY  n.name
ORDER BY revenue DESC;

CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

CREATE TABLE SUPPLIER (
  suppkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  comment string
) FROM FILE 'supplier.tbl' USING TPCH('supplier');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

CREATE TABLE REGION (
  regionkey int,
  name string,
  comment string
) FROM FILE 'region.tbl' USING TPCH('region');

SELECT    n.name,
          SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM      CUSTOMER c, ORDERS o, LINEITEM l, SUPPLIER s, NATION n, REGION r
WHERE     (c.custkey = o.custkey)
      AND (l.orderkey = o.orderkey)
      AND (l.suppkey = s.suppkey)
      AND (c.nationkey = s.nationkey)
      AND (s.nationkey = n.nationkey)
      AND (n.regionkey = r.regionkey)
      AND (r.name = 'ASIA')
      AND (o.orderdate >= 19940101)
      AND (o.orderdate < 19950101)
GROUP BY  n.name;
CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

SELECT   SUM(l.extendedprice*l.discount) AS revenue
FROM     LINEITEM l
WHERE    (l.shipdate >= 19940101)
     AND (l.shipdate < 19950101)
     AND (l.discount > 0.05)
     AND (l.discount < 0.07)
     AND (l.quantity < 24);CREATE TABLE PART (
  partkey int,
  name string,
  mfgr string,
  brand string,
  type string,
  size int,
  container string,
  retailprice float,
  comment string
) FROM FILE 'part.tbl' USING TPCH('part');

CREATE TABLE SUPPLIER (
  suppkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  comment string
) FROM FILE 'supplier.tbl' USING TPCH('supplier');

CREATE TABLE PARTSUPP (
  partkey int,
  suppkey int,
  availqty int,
  supplycost float,
  comment string
) FROM FILE 'partsupp.tbl' USING TPCH('partsupp');

CREATE TABLE CUSTOMER (
  custkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal float,
  mktsegment string,
  comment string
) FROM FILE 'customer.tbl' USING TPCH('customer');

CREATE TABLE ORDERS (
  orderkey int,
  custkey int,
  orderstatus string,
  totalprice float,
  orderdate date,
  orderpriority string,
  clerk string,
  shippriority int,
  comment string
) FROM FILE 'orders.tbl' USING TPCH('orders');

CREATE TABLE LINEITEM (
  orderkey int,
  partkey int,
  suppkey int,
  linenumber int,
  quantity float,
  extendedprice float,
  discount float,
  tax float,
  returnflag string,
  linestatus string,
  shipdate date,
  commitdate date,
  receiptdate date,
  shipinstruct string,
  shipmode string,
  comment string
) FROM FILE 'lineitem.tbl' USING TPCH('lineitem');

CREATE TABLE NATION (
  nationkey int,
  name string,
  regionkey int,
  comment string
) FROM FILE 'nation.tbl' USING TPCH('nation');

CREATE TABLE REGION (
  regionkey int,
  name string,
  comment string
) FROM FILE 'region.tbl' USING TPCH('region');
SELECT 1 AS A
  UNION
SELECT 2 AS A;SELECT 1 AS A, 3 AS B
  UNION
SELECT 2 AS A, 4 AS B;SELECT 1 AS A, 3 AS B
  UNION
SELECT 2 AS A, 4 AS B
  UNION
SELECT 5 AS A, 6 AS B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (A int, B int) FROM FILE 'test/s.dat' USING CSV;

SELECT A, B FROM R
  UNION
SELECT A, B FROM S;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, B FROM R
  UNION
SELECT 1 AS A, 2 AS B;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, B FROM R
  UNION
SELECT A, B FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;

SELECT A, B FROM R
  UNION
SELECT A, B FROM R
  UNION
SELECT A, B FROM R;CREATE TABLE R (A int, B int) FROM FILE 'test/r.dat' USING CSV;
CREATE TABLE S (B int, C int) FROM FILE 'test/s.dat' USING CSV;

SELECT B FROM R
  UNION
SELECT B FROM S;