*   Added basic `having` support (Mike Aldred)
*   Added `korma.db/h2` to create a database specification for a h2 database (Juha Syrjälä)
*   Insert statements with empty values turn into SQL of "DO 0", a NOOP
*   Empty where clauses are ignored, instead of creating illegal SQL

*started on Dec 27, 2012*

