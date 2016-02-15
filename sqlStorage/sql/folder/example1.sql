SELECT
  *
FROM
  myTable
{% if id is defined %}
where
  id = %(id)s
{% endif %}
