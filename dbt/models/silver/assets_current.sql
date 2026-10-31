-- Mirror of app.assets with soft deletes (see macros/cdc_current.sql).
{{ config(unique_key='asset') }}
{{ cdc_current(
    'stg_assets_latest',
    key='asset',
    columns=['name', 'precision', 'updated_at']
) }}
