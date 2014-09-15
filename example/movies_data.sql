set application_name to 'request_1,gary';

insert into movies(id, title) 
values (nextval(current_schema || '.id_seq'), 'Star Wars');

insert into licenses (id, movie_id, title, start_date, end_date)
values(nextval(current_schema || '.id_seq'), 1, 'Disney', '01-01-2000'::timestamp, '03-01-2000'::timestamp);

select * 
from movies m, licenses l
where m.id = l.movie_id;

set application_name to 'request_2,gary';

update movies 
set title = 'Star Wars - Phantom Menace' 
where title = 'Star Wars';

set application_name to 'request_3,greg';
delete from licenses where id = 2;
delete from movies where id = 2;
