--
-- SELECT
--

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
ANALYZE onek2;

SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;


SELECT two, stringu1, ten, string4
   INTO TABLE tmp
   FROM onek;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
SELECT p.name, p.age FROM person* p;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
SELECT p.name, p.age FROM person* p ORDER BY age using >, name;

--
-- Test some cases involving whole-row Var referencing a subquery
--
select foo from (select 1) as foo;
select foo from (select null) as foo;
select foo from (select 'xyzzy',1,null) as foo;

--
-- Test VALUES lists
--
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- a more complex case
-- looks like we're coding lisp :-)
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i;

-- try VALUES in a subquery
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
VALUES (1,2), (3,4+4), (7,77.7);

VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl;

--
-- Test ORDER BY options
--

CREATE TEMP TABLE foo (f1 int);

INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- check if indexscans do the right things
CREATE INDEX fooi ON foo (f1);
SET enable_sort = false;

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

DROP INDEX fooi;
CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

SELECT * FROM foo ORDER BY f1;
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
SELECT * FROM foo ORDER BY f1 DESC;
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
create function sillysrf(int) returns setof int as
  'values (1),(10),(2),($1)' language sql immutable;

select sillysrf(42);
select sillysrf(-1) order by 1;

drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
select * from (values (2),(null),(1)) v(k) where k = k order by k;
select * from (values (2),(null),(1)) v(k) where k = k;

-- Test warnings for missing AS for aliases
SET missing_as_warning TO false;
select 1 foo, 2 bar;
select 1 as foo, 2 as bar;
select count(*) foo;

select 1 from onek o1 limit 1;
select 1 from onek o1, onek o2 limit 1;
select 1 from onek as o1, onek as o2 limit 1;
select un1 from onek ok (un1) where un1 = 88888888;
select un1 from onek as ok (un1) where un1 = 88888888;
update onek ok set unique1 = -88888888 where unique1 = 88888888;
update onek as ok set unique1 = -88888888 where unique1 = 88888888;

create function func_for_missing_as() returns record language sql as 'select 1::int';
select * from func_for_missing_as() f (id int);
select * from func_for_missing_as() as f (id int);

SET missing_as_warning TO true;
select 1 foo, 2 bar;
select 1 as foo, 2 as bar;
select count(*) foo;

do $$
declare
    i1 int;
    i2 int;
begin
    select unique1 unique2 FROM onek into i1, i2;
    select unique1, unique2 FROM onek into i1, i2;
end;
$$;

select 1 from onek o1 limit 1;
select 1 from onek o1, onek o2 limit 1;
select 1 from onek as o1, onek as o2 limit 1;
select un1 from onek ok (un1) where un1 = 88888888;
select un1 from onek as ok (un1) where un1 = 88888888;
update onek ok set unique1 = -88888888 where unique1 = 88888888;
update onek as ok set unique1 = -88888888 where unique1 = 88888888;

select * from func_for_missing_as() f (id int);
select * from func_for_missing_as() as f (id int);

drop function func_for_missing_as();
SET missing_as_warning TO false;
