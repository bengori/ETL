import yaml
import os

with open('shema.yaml', encoding='utf-8') as f:
    YAML_DATA = yaml.safe_load(f)


hubs = {
    hub_name: {
        table: 'dds.h_{hub_name}'.format(hub_name=hub_name)
        for table, cols in YAML_DATA['sources']['tables'].items()
        for col in cols['columns']
        for bk_column, inf in col.items()
        if inf.get('bk_for') == hub_name
    }
    for hub_name in YAML_DATA['groups']['hubs'].keys()
}

hub_satellites = {
    hub_name: {
        table_name: 'dds.s_{hub_name}'.format(hub_name=hub_name)
        for table_name, cols in YAML_DATA['sources']['tables'].items()
        for col in cols['columns']
        for bk_column, inf in col.items()
        if (inf.get('bk_for') == hub_name) and (inf.get('owner') is True)
    }
    for hub_name, info in YAML_DATA['groups']['hubs'].items()
    for satellite_columns in info['satellite']['columns'].items()
}

print('hub -> satellites:')
for hub, info in hub_satellites.items():
    for source_table, task in info.items():
        print(hubs[hub][source_table] + '--' + task)
