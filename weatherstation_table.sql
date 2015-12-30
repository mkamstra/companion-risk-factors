create table weatherstation (id serial PRIMARY KEY, knmiid int, name varchar(64), location GEOGRAPHY(POINT, 4326), altitude real);

alter table weatherstation owner to snt;

insert into weatherstation values(3, 220, 'testje3', ST_GeographyFromText('SRID=4326;POINT(5.1 52.0)'), 50);

select  *,st_distance(location, ST_GeographyFromText('SRID=4326;POINT(5.1 51.0)')) from weatherstation where st_distance(location, ST_GeographyFromText('SRID=4326;POINT(5.1 51.0)')) > 10;

sudo -u postgres psql companion


create table weatherstation2 (id serial PRIMARY KEY, knmiid int, name varchar(64), location GEOMETRY(POINT, 4326), altitude real);

alter table weatherstation2 owner to snt;

insert into weatherstation2 values(3, 220, 'testje3', ST_GeomFromText('POINT(5.1 52.0)', 4326), 50);

select knmiid,name,st_astext(location),st_distance_sphere(location, ST_GeomFromText('POINT(52.165 4.419)')) as dist from weatherstation order by dist;