0.4.3
-----
* Upgrade clojure.java.jdbc to 0.6.1 (venantius)
* Upgrade c3p0 to 0.9.5.2 (gfZeng)
* Add support for excluding c3p0 (k13gomez)
* Add support for foreign key name option for parent table in relation (k13gomez)
* Make order of rows for insert queries not matter (tie-rack)
* Add support for ILIKE predicate (arttuka)
* Add support for predicate keywords (zjhmale)
* Fix reflection warnings (divs1210)
* Fix arglist metadata for `join` (venantius)
* Fix syntax error in README (zjhsdtc)
* Update entity-fields documentation (jdpopkin)


0.4.2
-----
* Fix regression in 0.4.1 causing global config changes not to take affect if defdb is called first
    * This fix reverts "Honor db specific options when using `with-db`" which will be reimplemented in future release
* Bump clojure/java.jdbc version to 0.3.7 (danielsoro)

0.4.1
-----
* Fix warnings when using Clojure 1.7 (glittershark)
* Bump clojure/java.jdbc version to 0.3.6
* Add possibility to set initial pool size for connection pool (AKurilin)
* Add support for driver properties when using connection pool (federkasten)
* Return number of deleted rows
* Honor db specific options when using `with-db` (ls4f)
* Fix lazy namespace resolution of relations (juhovh)
* Fix arity of `order` (lokori)
* Stop printing exceptions to \*out\*

0.4.0
-----
* Stop using deprecated implementation of [clojure.java.jdbc](https://github.com/clojure/java.jdbc) (scttnlsn)
* Return number of updated rows
* Add support for read-only transactions and transactions with a specific isolation level (ls4f)
* Add support for joining entities using a specific join type (starks67)

0.3.3
-----
* Fix regression in 0.3.2 causing NPE when fetching relations and default connection hasn't been set
* Fix exception with multiple raws in where clause
* Use db from main query when fetching children with no db specified

0.3.2
-----
* Support EXISTS keyword (pocket7878)
* Add helper for creating FirebirdSQL connection (ls4f)
* Fix concurrency issue on with-db (David Raphael)
* Use bind parameters for boolean values
* Fix error message for missing db connection when lazily fetching subentities

0.3.1
-----
* SQL generation is now deterministic across Clojure versions
* Fix for using wrong delimiters in JOIN query for has-one and belongs-to relations
* Fix join columns used for fetching belongs-to relation when subentity is not using default primary key and has transform fn defined
* Fix for losing db connection or using wrong connection when lazily fetching subentities (David Raphael)

0.3.0
-----
No changes since the previous release candidate.

0.3.0-RC8
---------
* Ensure fields are not overriden when using transform fn on subentity of one-to-one relation

0.3.0-RC7
---------
* Upgrade java.jdbc to 0.3, use deprecated namespace (MerelyAPseudonym)
* Add support for HP Vertica (Chris Benninger)
* Add transform fn support to one-to-one relations (Immo Heikkinen)

0.3.0-RC6
---------
* Options for IdleConnectionTestPeriod and TestConnectionOnCheckin (federkasten)
* defonce config options to prevent issues with MySQL delimiters (cosmi)
* README cleanup (writa)
* Can now run a test query on checkout for connection verification (joekarl)
* Introduced alias-delimiter option to choose word for alias (ktsujister and Ringman)
* Fixed issue with set-fields not applying db delimiters (mjtodd)
* Made korma.sql.engine/bind-params public for incubator (sgrove)

0.3.0-RC5
---------

* support nested `korma.db/transaction` calls (josephwilk)
* integrated with TravisCI

0.3.0-RC4
---------

* MS Access db config helper function (Vadim Geshel)
* bugfix in `do-query` w/ options (David Kettering)

0.3.0-RC3
---------

* Add db-spec creator for ODBC connections (David Kettering)
* Add stdev aggregate (David Kettering)
* Parenthesize multiple JOIN expressions (David Kettering)
* Use optional AS keyword in alias clauses (David Kettering)
* Use <> instead of != in relational comparisons (David Kettering)

0.3.0-RC2
---------

* Connection pool always returns a connection pool, use :make-pool? option to disable pool creation

0.3.0-RC1
---------

* Allow opting out of the connection pool creation
* Allow sending other kinds of sb specs straight through to java.jdbc

0.3.0-beta15
---------------------

*  transactional wrapping for multiple databases (Timo Westkämper)
*  fixing macro expansion bug introduced in recent beta (Moritz Heidkamp, Joshua Eckroth)
*  Can use mysql/count so that (count :*) works correctly on MySQL (Tsutomu YANO)
*  Added `union`, `union-all`, and `intersect` queries

0.3.0-beta14
------------

*  Support for `many-to-many` relationships (Dennis Roberts)
*  Fixed table-name delimiting, and at the same time, support Postgres' schema and 
   queries covering multiple databases.  
   See: https://github.com/korma/Korma/pull/105 (Tsutomu YANO)

0.3.0-beta13
------------

*   Set Min/Max Connection Pool Size from db spec (Nick Zalabak)
*   Merge defaults instead of overwriting them (Jim Crossley)
*   DB specs can reference existing datasources or JNDI references (Jim Crossley)
*   Added `between` predicate (Charles Duffy)
*   Corrected default port for MS SQL Server (Alexander Zolotko)
*   Added basic `having` support (Mike Aldred)
*   Added `korma.db/h2` to create a database specification for a h2 database (Juha Syrjälä)
*   Insert statements with empty values turn into SQL of "DO 0", a NOOP
*   Empty where clauses are ignored, instead of creating illegal SQL

*started on Dec 27, 2012*

