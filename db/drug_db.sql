CREATE SCHEMA IF NOT EXISTS "public";

CREATE TABLE public.nodes (
"node_id" serial PRIMARY KEY,
"node_type" varchar(50) NOT NULL,
"value" varchar(5000) NOT NULL,
"source" varchar(50) NOT NULL,
"parameters" json
);

CREATE TABLE public.edges (
"edge_id" serial PRIMARY KEY,
"from_type" varchar(50) NOT NULL,
"from_value" varchar(5000) NOT NULL,
"to_type" varchar(50) NOT NULL,
"to_value" varchar(5000) NOT NULL,
"label" varchar(50) NOT NULL,
"source" varchar(50) NOT NULL,
"parameters" json
);