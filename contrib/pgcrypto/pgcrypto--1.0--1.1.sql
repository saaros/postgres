/* contrib/pgcrypto/pgcrypto--1.0--1.1.sql */

\echo Use "ALTER EXTENSION pgcrypto UPDATE" to load this file. \quit

CREATE FUNCTION gen_random_uuid()
RETURNS uuid
AS 'MODULE_PATHNAME', 'pg_random_uuid'
LANGUAGE C VOLATILE;
