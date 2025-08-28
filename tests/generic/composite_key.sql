/*
# Custom Generic Test: Composite Key Uniqueness Validator

Custom generic test validating that specified columns form a unique composite key.


Use in schema.yml on model-/-source-level:
    tests:
      - composite_key_candidate:
          columns: ['movie_id', 'location', 'month']
*/

{% test composite_key_candidate(model, columns) %}

    select
        {% for column in columns %} {{ column }}{{ ',' if not loop.last }} {% endfor %},
        count(*)
    from {{ model }}
    group by
        {% for column in columns %} {{ column }}{{ "," if not loop.last }} {% endfor %}
    having count(*) > 1

{% endtest %}
