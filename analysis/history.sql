

-- Range test
select
  id, title
from
  movies_vw
where 
  license_effective @> now()

-- Range view
  CREATE view movie_ranges AS 
  SELECT
    tsrange(
      s.audit_date, 
      coalesce(lead(s.audit_date) 
                 over(
                   partition by s.i_id 
                   order by s.audit_date), 
               'infinity'), 
               '[)'
    ) m_effective,
    m.audit_date
    movie.name,
  FROM movie$a s

-- Range view 2
CREATE view movie_ranges AS 
  SELECT
    tsrange(
      s.audit_date, 
      coalesce(lead(s.audit_date) 
                 over(
                   partition by s.i_id 
                   order by s.audit_date), 
               'infinity'), 
               '[)'
    ) m_effective,
    m.audit_date
    movie.name,
  FROM movie$a s

-- Range view 3
WITH s as (
  SELECT
    *
  FROM movie_vw s
  LEFT JOIN (
    license_vw 
  ) l ON l.name = m.licensee
),
all_joined as (
  SELECT
    -- anything not found in a left join gets turned into an infinite range
    coalesce(mis_effective, tsrange('-infinity', 'infinity', '[]')) mis_effective,
    coalesce(mir_effective, tsrange('-infinity', 'infinity', '[]')) mir_effective,
    greatest(s._audit_date_, r._audit_date_) _audit_date_,
    s.tmf_level
  FROM s
  LEFT JOIN r ON s.id = r.id
)
select *
from all_joined
where ...


-- Blame with timestamp
SELECT movie_user, license_user, test
FROM movies_vw
WHERE id = ...
AND date_range <@ '12345'




