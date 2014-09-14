create table movies (
  id int primary key,
  title text
);

create table licenses (
  id int primary key,
  movie_id int references movies (id),
  title text, 
  start_date timestamp, 
  end_date timestamp
);

create sequence id_seq;
