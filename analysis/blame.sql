-- Blame query
select 
  *
from 
(
  select 
    audit_date,
    audit_user,
    audit_action,
    id,
    title,
    (case when ne(title, lead(title) over y) then audit_user else null end) title$u, 
    dense_rank() over y title$r,
    title$c
  from (
    select 
      id,
      title,
      ne(title, lead(title) over w) title$c,
      audit_date,
      audit_user,
      audit_action
    from movies$a
    where audit_action in ('I', 'U')
    WINDOW w AS ( PARTITION by id ORDER BY audit_date DESC)
  ) a
  WINDOW y AS ( PARTITION by id ORDER BY audit_date DESC, title$c DESC)
) b
WHERE title$r = 1
;

