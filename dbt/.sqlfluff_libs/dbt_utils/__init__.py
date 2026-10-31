"""sqlfluff jinja-templater stub for the dbt_utils package (lint-time only).

The jinja templater renders SQL offline, without dbt or a database, so
package macros must be stubbed with SQL of the same shape.
"""


def generate_surrogate_key(field_list):
    return "md5(" + " || '-' || ".join(f"cast({f} as varchar)" for f in field_list) + ")"
