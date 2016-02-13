SELECT
  *
FROM
  accounts
{% if id is defined %}
where
  id = %(id)s
{% endif %}
