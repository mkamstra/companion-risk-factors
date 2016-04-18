--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: topology; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO postgres;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


--
-- Name: postgis_topology; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;


--
-- Name: EXTENSION postgis_topology; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: measurementsite; Type: TABLE; Schema: public; Owner: snt; Tablespace: 
--

CREATE TABLE measurementsite (
    id integer NOT NULL,
    ndwid text NOT NULL,
    name text NOT NULL,
    ndwtype integer NOT NULL,
    location geometry(Point,4326) NOT NULL,
    carriageway1 text,
    lengthaffected1 integer,
    coordinates geometry(LineString,4326)
);


ALTER TABLE measurementsite OWNER TO snt;

--
-- Name: measurementsite_id_seq; Type: SEQUENCE; Schema: public; Owner: snt
--

CREATE SEQUENCE measurementsite_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE measurementsite_id_seq OWNER TO snt;

--
-- Name: measurementsite_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: snt
--

ALTER SEQUENCE measurementsite_id_seq OWNED BY measurementsite.id;


--
-- Name: measurementsite_weatherstation_link; Type: TABLE; Schema: public; Owner: snt; Tablespace: 
--

CREATE TABLE measurementsite_weatherstation_link (
    msid integer NOT NULL,
    wsid integer NOT NULL
);


ALTER TABLE measurementsite_weatherstation_link OWNER TO snt;

--
-- Name: measurementsitetype; Type: TABLE; Schema: public; Owner: snt; Tablespace: 
--

CREATE TABLE measurementsitetype (
    id integer NOT NULL,
    ndwtype text
);


ALTER TABLE measurementsitetype OWNER TO snt;

--
-- Name: measurementsitetype_id_seq; Type: SEQUENCE; Schema: public; Owner: snt
--

CREATE SEQUENCE measurementsitetype_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE measurementsitetype_id_seq OWNER TO snt;

--
-- Name: measurementsitetype_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: snt
--

ALTER SEQUENCE measurementsitetype_id_seq OWNED BY measurementsitetype.id;


--
-- Name: weatherstation; Type: TABLE; Schema: public; Owner: snt; Tablespace: 
--

CREATE TABLE weatherstation (
    id integer NOT NULL,
    knmiid integer,
    name character varying(64),
    location geometry(Point,4326),
    altitude real
);


ALTER TABLE weatherstation OWNER TO snt;

--
-- Name: weatherstation_id_seq; Type: SEQUENCE; Schema: public; Owner: snt
--

CREATE SEQUENCE weatherstation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE weatherstation_id_seq OWNER TO snt;

--
-- Name: weatherstation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: snt
--

ALTER SEQUENCE weatherstation_id_seq OWNED BY weatherstation.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: snt
--

ALTER TABLE ONLY measurementsite ALTER COLUMN id SET DEFAULT nextval('measurementsite_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: snt
--

ALTER TABLE ONLY measurementsitetype ALTER COLUMN id SET DEFAULT nextval('measurementsitetype_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: snt
--

ALTER TABLE ONLY weatherstation ALTER COLUMN id SET DEFAULT nextval('weatherstation_id_seq'::regclass);


--
-- Name: measurementsite_id_key; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY measurementsite
    ADD CONSTRAINT measurementsite_id_key UNIQUE (id);


--
-- Name: measurementsite_pkey; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY measurementsite
    ADD CONSTRAINT measurementsite_pkey PRIMARY KEY (ndwid);


--
-- Name: measurementsite_weatherstation_link_pkey; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY measurementsite_weatherstation_link
    ADD CONSTRAINT measurementsite_weatherstation_link_pkey PRIMARY KEY (msid, wsid);


--
-- Name: measurementsitetype_ndwtype_key; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY measurementsitetype
    ADD CONSTRAINT measurementsitetype_ndwtype_key UNIQUE (ndwtype);


--
-- Name: measurementsitetype_pkey; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY measurementsitetype
    ADD CONSTRAINT measurementsitetype_pkey PRIMARY KEY (id);


--
-- Name: weatherstation_pkey; Type: CONSTRAINT; Schema: public; Owner: snt; Tablespace: 
--

ALTER TABLE ONLY weatherstation
    ADD CONSTRAINT weatherstation_pkey PRIMARY KEY (id);


--
-- Name: measurementsite_ndwtype_fkey; Type: FK CONSTRAINT; Schema: public; Owner: snt
--

ALTER TABLE ONLY measurementsite
    ADD CONSTRAINT measurementsite_ndwtype_fkey FOREIGN KEY (ndwtype) REFERENCES measurementsitetype(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: measurementsite_weatherstation_link_msid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: snt
--

ALTER TABLE ONLY measurementsite_weatherstation_link
    ADD CONSTRAINT measurementsite_weatherstation_link_msid_fkey FOREIGN KEY (msid) REFERENCES measurementsite(id) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: measurementsite_weatherstation_link_wsid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: snt
--

ALTER TABLE ONLY measurementsite_weatherstation_link
    ADD CONSTRAINT measurementsite_weatherstation_link_wsid_fkey FOREIGN KEY (wsid) REFERENCES weatherstation(id) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

